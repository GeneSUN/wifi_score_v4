"""
================================================================================
  WiFi Score AI Agent (Tool-Use Architecture)
  ---------------------------------------------------------------------------
  Evolves the linear 7-stage pipeline into a Claude tool-use agent.

  Instead of:  classify → build prompt → one LLM call → text answer
  Now:         LLM decides which tools to call → executes them → reasons over results

  The agent loop:
    1. User asks a question
    2. Claude sees the question + available tools
    3. Claude decides which tool(s) to call (and with what params)
    4. We execute the tool, return the result to Claude
    5. Claude either calls another tool or produces a final answer
    6. Repeat until Claude says "I'm done"
================================================================================
"""

import json
import base64
import textwrap
from datetime import date, timedelta
from typing import Any, Optional

import anthropic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

HDFS_ROOT   = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000"
PARQUET_DIR = "/user/ZheS/wifi_score_v4/wifiScore_location"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

KPI_DOCUMENT_TEXT = """
WiFi Score v4 – KPI Reference
------------------------------
SCORE DIMENSIONS
  * reliabilityScore  - composite of reboot stability, IP-change frequency,
                        and band-steering success.
  * speedScore        - derived from phyrate (physical link rate) and RSSI.
  * coverageScore     - reflects RSSI levels and airtime utilisation.
  * wifiScore         - weighted average of reliability, speed, and coverage.

CATEGORY THRESHOLDS (numeric encoding)
  4 = Excellent | 3 = Good | 2 = Fair | 1 = Poor
"""


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL DEFINITIONS (Claude API schema)
#  These tell Claude what tools exist, their parameters, and when to use them.
# ══════════════════════════════════════════════════════════════════════════════

TOOLS = [
    {
        "name": "fetch_device_metrics",
        "description": (
            "Fetch WiFi metrics for a device on a specific date. "
            "Returns scores (wifiScore, reliabilityScore, speedScore, coverageScore) "
            "and detailed KPIs (RSSI, PHY rate, reboots, airtime, etc). "
            "Use this whenever you need to look up a device's current or historical metrics."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sn":   {"type": "string", "description": "Device serial number"},
                "date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
            },
            "required": ["sn", "date"],
        },
    },
    {
        "name": "fetch_timeseries",
        "description": (
            "Fetch multiple days of WiFi metrics for a device. "
            "Returns a list of daily metric snapshots sorted by date. "
            "Use this when the user asks about trends, recent performance, "
            "or any question that requires data over a time range."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sn":        {"type": "string", "description": "Device serial number"},
                "end_date":  {"type": "string", "description": "End date (YYYY-MM-DD), inclusive"},
                "num_days":  {"type": "integer", "description": "Number of days to look back (including end_date)", "default": 7},
            },
            "required": ["sn", "end_date"],
        },
    },
    {
        "name": "plot_timeseries",
        "description": (
            "Generate a line chart from time series data. "
            "Use this after fetch_timeseries to visualize trends for the user. "
            "Supports plotting one or more metrics over time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sn":      {"type": "string",                "description": "Device serial number (used in chart title)"},
                "dates":   {"type": "array", "items": {"type": "string"}, "description": "List of date strings for x-axis"},
                "metrics": {
                    "type": "object",
                    "description": "Dict of {metric_name: [values]} to plot as separate lines",
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "number"},
                    },
                },
                "title":   {"type": "string", "description": "Chart title", "default": "WiFi Score Trend"},
            },
            "required": ["sn", "dates", "metrics"],
        },
    },
    {
        "name": "detect_anomaly_forecast",
        "description": (
            "Run a time series forecasting model to detect if the most recent data point "
            "is a statistically significant anomaly compared to the forecasted trend. "
            "Use this when the user asks if a recent drop is significant, unexpected, "
            "or abnormal. Returns the forecast, confidence interval, and anomaly flag."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sn":         {"type": "string",                              "description": "Device serial number"},
                "dates":      {"type": "array", "items": {"type": "string"}, "description": "Ordered date strings"},
                "values":     {"type": "array", "items": {"type": "number"}, "description": "Metric values corresponding to dates"},
                "metric_name": {"type": "string",                             "description": "Name of the metric being analyzed"},
            },
            "required": ["sn", "dates", "values", "metric_name"],
        },
    },
    {
        "name": "detect_anomaly_autoencoder",
        "description": (
            "Run a pre-trained autoencoder model to detect if this device's recent "
            "performance is significantly different from the general population. "
            "Use this when the user asks if their experience is unusual compared to "
            "other users, or wants a population-level comparison."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sn":         {"type": "string",                              "description": "Device serial number"},
                "dates":      {"type": "array", "items": {"type": "string"}, "description": "Ordered date strings"},
                "values":     {"type": "array", "items": {"type": "number"}, "description": "Metric values corresponding to dates"},
                "metric_name": {"type": "string",                             "description": "Name of the metric being analyzed"},
            },
            "required": ["sn", "dates", "values", "metric_name"],
        },
    },
]


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL IMPLEMENTATIONS
#  Each function matches a tool name above. Returns a dict that gets
#  JSON-serialized and sent back to Claude as the tool result.
# ══════════════════════════════════════════════════════════════════════════════

def _tool_fetch_device_metrics(spark: SparkSession, sn: str, date: str) -> dict:
    """Read a single day of parquet data for one device."""
    parquet_path = f"{HDFS_ROOT}{PARQUET_DIR}/{date}"
    df = spark.read.parquet(parquet_path).filter(F.col("sn") == sn.upper()).limit(1)
    rows = df.collect()
    if not rows:
        return {"error": f"No data found for sn={sn} on {date}"}
    raw = rows[0].asDict()
    return {k: (v if v is not None else "N/A") for k, v in raw.items()}


def _tool_fetch_timeseries(spark: SparkSession, sn: str, end_date: str, num_days: int = 7) -> list[dict]:
    """Read multiple days of parquet data for one device."""
    from datetime import datetime
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    results = []

    for i in range(num_days):
        day = end - timedelta(days=num_days - 1 - i)
        day_str = str(day)
        try:
            row_data = _tool_fetch_device_metrics(spark, sn, day_str)
            if "error" not in row_data:
                row_data["_date"] = day_str
                results.append(row_data)
        except Exception:
            continue  # skip days with missing partitions

    if not results:
        return [{"error": f"No data found for sn={sn} in the last {num_days} days ending {end_date}"}]
    return results


def _tool_plot_timeseries(sn: str, dates: list[str], metrics: dict[str, list], title: str = "WiFi Score Trend") -> dict:
    """
    Generate a line chart. Returns the chart as a base64-encoded PNG.

    YOUR IMPLEMENTATION: Use matplotlib, plotly, or any charting library.
    The base64 image can be returned to the frontend for rendering.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import io

    fig, ax = plt.subplots(figsize=(10, 5))
    for metric_name, values in metrics.items():
        ax.plot(dates, values, marker="o", label=metric_name)
    ax.set_title(f"{title} — {sn}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Score")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode("utf-8")

    return {
        "chart_base64": img_b64,
        "description": f"Line chart of {', '.join(metrics.keys())} for {sn} from {dates[0]} to {dates[-1]}",
    }


def _tool_detect_anomaly_forecast(
    sn: str, dates: list[str], values: list[float], metric_name: str,
) -> dict:
    """
    Run a time-series forecasting model and flag if the last point is anomalous.

    YOUR IMPLEMENTATION: Replace this stub with your Chronos model or any
    forecasting model (Prophet, ARIMA, etc). The interface stays the same.
    """
    # ── STUB: replace with your Chronos / forecasting model ──
    # Example: from your_models import chronos_forecast
    # forecast_result = chronos_forecast(values)

    import numpy as np
    history = values[:-1]
    actual  = values[-1]
    mean    = float(np.mean(history))
    std     = float(np.std(history)) if len(history) > 1 else 0.0

    forecast  = mean  # naive forecast (replace with Chronos)
    lower     = mean - 2 * std
    upper     = mean + 2 * std
    is_anomaly = actual < lower or actual > upper

    return {
        "metric_name":       metric_name,
        "actual_value":      actual,
        "forecast_value":    round(forecast, 2),
        "confidence_lower":  round(lower, 2),
        "confidence_upper":  round(upper, 2),
        "is_anomaly":        is_anomaly,
        "anomaly_direction": "drop" if actual < lower else ("spike" if actual > upper else "normal"),
        "history_mean":      round(mean, 2),
        "history_std":       round(std, 2),
        "method":            "stub_mean_2sigma",  # change to "chronos" when ready
    }


def _tool_detect_anomaly_autoencoder(
    sn: str, dates: list[str], values: list[float], metric_name: str,
) -> dict:
    """
    Run a pre-trained autoencoder to compare this device against the population.

    YOUR IMPLEMENTATION: Replace this stub with your autoencoder model.
    Load model → encode the device's time series → compute reconstruction error →
    compare against population threshold.
    """
    # ── STUB: replace with your autoencoder model ──
    # Example: from your_models import autoencoder_detect
    # result = autoencoder_detect(sn, values)

    import numpy as np
    reconstruction_error = float(np.random.uniform(0.01, 0.15))  # placeholder
    threshold = 0.10
    is_outlier = reconstruction_error > threshold

    return {
        "metric_name":          metric_name,
        "reconstruction_error": round(reconstruction_error, 4),
        "population_threshold": threshold,
        "is_outlier":           is_outlier,
        "percentile":           round(float(np.random.uniform(0.5, 99.5)), 1),  # placeholder
        "interpretation":       (
            f"This device's {metric_name} pattern is {'significantly different from' if is_outlier else 'within normal range of'} "
            f"the general population."
        ),
        "method": "stub_autoencoder",  # change to "autoencoder_v1" when ready
    }


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL DISPATCHER
#  Maps tool names to their implementations.
# ══════════════════════════════════════════════════════════════════════════════

def dispatch_tool(tool_name: str, tool_input: dict, spark: SparkSession) -> str:
    """Execute a tool by name and return the JSON-serialized result."""

    if tool_name == "fetch_device_metrics":
        result = _tool_fetch_device_metrics(spark, **tool_input)

    elif tool_name == "fetch_timeseries":
        result = _tool_fetch_timeseries(spark, **tool_input)

    elif tool_name == "plot_timeseries":
        result = _tool_plot_timeseries(**tool_input)

    elif tool_name == "detect_anomaly_forecast":
        result = _tool_detect_anomaly_forecast(**tool_input)

    elif tool_name == "detect_anomaly_autoencoder":
        result = _tool_detect_anomaly_autoencoder(**tool_input)

    else:
        result = {"error": f"Unknown tool: {tool_name}"}

    return json.dumps(result, default=str)


# ══════════════════════════════════════════════════════════════════════════════
#  SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = textwrap.dedent(f"""
    You are a WiFi diagnostics expert agent for a telecommunications company.
    You help customers and support agents understand device WiFi performance
    by analyzing real telemetry data.

    You have access to tools that can:
    - Fetch device metrics for a single day or a time range
    - Plot time series charts
    - Run forecasting models to detect significant drops
    - Run autoencoder models to compare against the general population

    === KPI DOCUMENTATION ===
    {KPI_DOCUMENT_TEXT.strip()}
    =========================

    WORKFLOW:
    1. First, understand what the user is asking.
    2. Decide which tool(s) you need. Call them.
    3. Analyze the results. If you need more data, call more tools.
    4. Provide a clear, grounded answer referencing actual numbers.

    RULES:
    - Always fetch real data before answering — never guess metric values.
    - When showing trends, always call plot_timeseries so the user gets a visual.
    - For "is this drop significant?" questions, use detect_anomaly_forecast.
    - For "am I different from others?" questions, use detect_anomaly_autoencoder.
    - You may chain multiple tools in one turn if needed.
    - Today's date is {{today}}.
""").strip()


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT LOOP
#  The core loop: send message → check for tool_use → execute → repeat
# ══════════════════════════════════════════════════════════════════════════════

MAX_TURNS = 10  # safety limit to prevent infinite loops


def run_agent(
    sn: str,
    question: str,
    target_date: Optional[str] = None,
    *,
    spark: SparkSession,
    anthropic_client: anthropic.Anthropic,
) -> dict[str, Any]:
    """
    Main entry point. Replaces run_pipeline().

    The agent autonomously decides which tools to call, executes them,
    and produces a final answer grounded in real data.

    Returns
    -------
    {
        "request":    {"sn", "question", "target_date"},
        "answer":     str (the agent's final text response),
        "tool_trace": [{"tool", "input", "output"}, ...],  # full audit trail
        "artifacts":  [{"type": "chart", "base64": "..."}],  # any generated charts
    }
    """
    target_date = target_date or str(date.today())

    # Build the initial user message with context
    user_message = (
        f"Device Serial Number: {sn}\n"
        f"Date: {target_date}\n"
        f"Question: {question}"
    )

    system = SYSTEM_PROMPT.replace("{today}", target_date)
    messages = [{"role": "user", "content": user_message}]
    tool_trace = []
    artifacts = []

    for turn in range(MAX_TURNS):
        print(f"[Agent turn {turn + 1}/{MAX_TURNS}]")

        # ── Call Claude ──
        response = anthropic_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            system=system,
            tools=TOOLS,
            messages=messages,
        )

        # ── If the model is done (no more tool calls), extract final text ──
        if response.stop_reason == "end_turn":
            final_text = ""
            for block in response.content:
                if block.type == "text":
                    final_text += block.text
            print("[Agent] Final answer ready.")
            return {
                "request": {"sn": sn, "question": question, "target_date": target_date},
                "answer": final_text,
                "tool_trace": tool_trace,
                "artifacts": artifacts,
            }

        # ── Process tool calls ──
        # Claude may request multiple tools in one turn — execute all of them
        assistant_content = response.content
        tool_results = []

        for block in assistant_content:
            if block.type == "tool_use":
                tool_name  = block.name
                tool_input = block.input
                tool_id    = block.id

                print(f"  -> Calling tool: {tool_name}({json.dumps(tool_input, default=str)[:120]}...)")

                # Execute the tool
                tool_output = dispatch_tool(tool_name, tool_input, spark)

                # Track for audit trail
                tool_trace.append({
                    "tool": tool_name,
                    "input": tool_input,
                    "output": json.loads(tool_output),
                })

                # Collect chart artifacts
                parsed = json.loads(tool_output)
                if "chart_base64" in parsed:
                    artifacts.append({
                        "type": "chart",
                        "base64": parsed["chart_base64"],
                        "description": parsed.get("description", ""),
                    })

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": tool_output,
                })

        # ── Append assistant turn + tool results, then loop ──
        messages.append({"role": "assistant", "content": assistant_content})
        messages.append({"role": "user", "content": tool_results})

    # Safety: if we hit MAX_TURNS, return what we have
    return {
        "request": {"sn": sn, "question": question, "target_date": target_date},
        "answer": "Agent reached maximum turns without a final answer.",
        "tool_trace": tool_trace,
        "artifacts": artifacts,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  EXAMPLE USAGE
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wifi_agent").getOrCreate()
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

    # Example 1: Trend question → agent will call fetch_timeseries + plot_timeseries
    result = run_agent(
        sn="AA114400877",
        question="What is my recent 7-day performance of overall WiFi score?",
        target_date="2026-03-24",
        spark=spark,
        anthropic_client=client,
    )
    print("\n=== ANSWER ===")
    print(result["answer"])
    print(f"\n=== TOOLS CALLED ({len(result['tool_trace'])}) ===")
    for t in result["tool_trace"]:
        print(f"  {t['tool']}({list(t['input'].keys())})")

    # Example 2: Anomaly detection → agent will call fetch_timeseries + detect_anomaly_forecast
    # result = run_agent(
    #     sn="AA114400877",
    #     question="Did my WiFi score significantly drop recently?",
    #     ...
    # )

    # Example 3: Population comparison → agent will call fetch_timeseries + detect_anomaly_autoencoder
    # result = run_agent(
    #     sn="AA114400877",
    #     question="Is my WiFi performance significantly different from other users?",
    #     ...
    # )
