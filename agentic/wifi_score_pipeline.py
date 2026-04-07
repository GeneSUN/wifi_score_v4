
"""
================================================================================
  WiFi Score AI Diagnostic Pipeline
  ---------------------------------------------------------------------------
  Purpose : Accept a user query about a specific device/customer, classify
            the question type, fetch that device's metrics from the parquet
            store, call Claude to generate a structured answer, and return a
            JSON response ready for downstream software to render.

  Design philosophy
  -----------------
  * Each stage is a plain function – easy to unit-test independently.
  * No global state.  Every function receives what it needs as arguments.
  * The final output is a typed dict so a frontend / API layer can deserialize
    it without guessing field names.

  Stages
  ------
  1.  build_request()           → validate & normalise the raw user input
  2.  load_device_metrics()     → read parquet row(s) for this device/date
  3.  retrieve_kpi_context()    → RAG: retrieve relevant KPI chunks from Redis
  4.  classify_question()       → ask Claude what kind of question this is
  5.  build_prompt()            → assemble a stage-specific system+user prompt
  6.  call_claude()             → single Anthropic API call, returns raw text
  7.  build_response()          → wrap everything into the final output schema
  8.  run_pipeline()            → orchestrates 1-7 end-to-end

================================================================================
"""

# ── stdlib ────────────────────────────────────────────────────────────────────
import json
import textwrap
from datetime import date
from typing import Any
from typing import Optional

# ── third-party ──────────────────────────────────────────────────────────────
import anthropic          # pip install anthropic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── RAG retriever ─────────────────────────────────────────────────────────────
from kpi_rag.rag_pipeline import build_retriever, OPENAI_API_KEY as RAG_OPENAI_API_KEY


# ══════════════════════════════════════════════════════════════════════════════
#  0.  CONSTANTS & CONFIG
# ══════════════════════════════════════════════════════════════════════════════
ANTHROPIC_API_KEY = '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'

HDFS_ROOT   = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000"
PARQUET_DIR = "/user/ZheS/wifi_score_v4/wifiScore_location"

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ── RAG retriever singleton ───────────────────────────────────────────────────
# Initialized once on first use; reused for every subsequent pipeline call.
_rag_retriever = None

def _get_rag_retriever():
    global _rag_retriever
    if _rag_retriever is None:
        print("Initialising RAG retriever (connecting to Redis) ...")
        _rag_retriever = build_retriever(openai_api_key=RAG_OPENAI_API_KEY)
        print("RAG retriever ready.")
    return _rag_retriever

# Six supported question types used as literals throughout the pipeline.
QUESTION_TYPES = [
    "Diagnosis",
    "Metric explanation",
    "Score calculation",
    "Comparison",
    "Recommendation",
    "Trend / anomaly",
]



# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 1 - build_request
#  Validate & normalise the raw caller-supplied input.
# ══════════════════════════════════════════════════════════════════════════════

def build_request(
    sn: str,
    question: str,
    target_date: Optional[str] = None,
) -> dict[str, Any]:
    """
    Parameters
    ----------
    sn          : Device serial number - the sole user/device identifier.
    question    : Free-text question from the customer / support agent.
    target_date : Date string 'YYYY-MM-DD'.  Defaults to today if omitted.

    Returns
    -------
    Normalised request dict that flows unchanged through every downstream stage.

    Schema
    ------
    {
        "sn":          str,   # upper-cased serial number
        "question":    str,   # stripped question text
        "target_date": str    # ISO date string
    }
    """
    if not sn:
        raise ValueError("sn (serial number) is required.")
    if not question:
        raise ValueError("question is required.")

    return {
        "sn":          sn.strip().upper(),
        "question":    question.strip(),
        "target_date": target_date or str(date.today()),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 2 - load_device_metrics
#  Read one device row from the parquet store via PySpark, keyed on sn.
# ══════════════════════════════════════════════════════════════════════════════

def load_device_metrics(
    spark: SparkSession,
    request: dict[str, Any],
) -> dict[str, Any]:
    """
    Reads the parquet partition for target_date and filters to the single
    row whose sn column matches request["sn"].

    Returns a plain Python dict (column -> value) so no downstream stage
    carries a PySpark dependency.  None values are replaced with "N/A" for
    clean prompt rendering.
    """
    parquet_path = f"{HDFS_ROOT}{PARQUET_DIR}/{request['target_date']}"

    df = (
        spark.read.parquet(parquet_path)
             .filter(F.col("sn") == request["sn"])
             .limit(1)           # one row per device per day
    )

    rows = df.collect()

    if not rows:
        raise LookupError(
            f"No metrics found for sn='{request['sn']}' "
            f"on {request['target_date']}."
        )

    raw = rows[0].asDict()
    return {k: (v if v is not None else "N/A") for k, v in raw.items()}


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 3 - retrieve_kpi_context
#  Query the RAG vector store for KPI chunks relevant to this question.
# ══════════════════════════════════════════════════════════════════════════════

def retrieve_kpi_context(retriever: Any, question: str) -> str:
    """
    Runs a similarity search against the Redis vector store and returns the
    top-k retrieved KPI document chunks formatted as a single string.

    Parameters
    ----------
    retriever : LangChain retriever returned by kpi_rag.rag_pipeline.build_retriever()
    question  : The customer's question (used as the retrieval query).

    Returns
    -------
    Concatenated text of the retrieved chunks, ready to inject into a prompt.
    """
    docs = retriever.invoke(question)
    if not docs:
        return "(No relevant KPI documentation retrieved.)"
    return "\n\n---\n\n".join(doc.page_content for doc in docs)


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 4 - classify_question
#  Ask Claude to categorise the question into one of the six types.
# ══════════════════════════════════════════════════════════════════════════════

def classify_question(
    client: anthropic.Anthropic,
    question: str,
) -> str:
    """
    Sends a short, strict classification prompt to Claude.

    Returns one of the QUESTION_TYPES strings.
    Falls back to "Diagnosis" if the model's reply is unrecognisable.
    """
    type_list = "\n".join(f"  - {t}" for t in QUESTION_TYPES)

    prompt = textwrap.dedent(f"""
        You are a question classifier for a WiFi diagnostics system.

        Classify the following customer question into EXACTLY ONE of these types:
        {type_list}

        Rules:
        - Reply with ONLY the type label, nothing else.
        - Choose the single best match.

        Question: "{question}"
    """).strip()

    response = client.messages.create(
        model      = CLAUDE_MODEL,
        max_tokens = 20,
        messages   = [{"role": "user", "content": prompt}],
    )

    raw_type = response.content[0].text.strip()

    # Match against known types; fall back gracefully if the model drifts.
    for t in QUESTION_TYPES:
        if t.lower() in raw_type.lower():
            return t

    return "Diagnosis"



# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 5 - build_prompt
#  Assemble (system_prompt, user_prompt) tailored to the detected question type.
# ══════════════════════════════════════════════════════════════════════════════

# Each question type steers Claude's tone, structure, and analytical focus.
QUESTION_TYPE_INSTRUCTIONS: dict[str, str] = {

    "Diagnosis": textwrap.dedent("""
        The customer wants to understand WHY their WiFi score or a KPI is in
        its current state.  Your job:
        1. Identify which metric(s) are below the "Good" threshold.
        2. Explain in plain language what each low metric means.
        3. State the most probable root cause.
        Use a structured, empathetic tone.
    """),

    "Metric explanation": textwrap.dedent("""
        The customer wants a clear explanation of what a specific metric means.
        Your job:
        1. Define the metric in plain language (avoid jargon).
        2. Explain how it is measured and what range is considered healthy.
        3. Describe how it affects the overall WiFi score.
        Keep the response concise (2-3 paragraphs).
    """),

    "Score calculation": textwrap.dedent("""
        The customer wants to know HOW the WiFi score is computed.
        Your job:
        1. Summarise the three sub-scores (reliability, speed, coverage).
        2. Describe the weighting / aggregation logic from the KPI document.
        3. Show which of the device's current values feed into each sub-score.
        Be precise and cite the KPI document where possible.
    """),

    "Comparison": textwrap.dedent("""
        The customer wants to compare two metrics or sub-scores.
        Your job:
        1. State the numeric/category value of each dimension being compared.
        2. Explain WHY one is better or worse using the underlying KPIs.
        3. Highlight the specific driver(s) behind the gap.
        Use a side-by-side structure in your answer.
    """),

    "Recommendation": textwrap.dedent("""
        The customer wants actionable advice to improve their WiFi.
        Your job:
        1. Focus on the 1-3 lowest-scoring metrics.
        2. Give concrete, customer-actionable steps for each.
        3. Estimate the expected improvement where possible.
        Prioritise by impact (highest first) and use a numbered list.
    """),

    "Trend / anomaly": textwrap.dedent("""
        The customer is asking about a change or unexpected behaviour over time.
        Your job:
        1. Identify which metrics show anomalous or trending values.
        2. Hypothesise what event could cause this pattern.
        3. Suggest next steps to confirm or resolve the anomaly.
        Note: you only have ONE day of data; acknowledge that limitation.
    """),
}

# Columns forwarded to the prompt - avoids sending the full ~50-column row.
PROMPT_COLUMNS = [
    "sn", "model_name", "firmware", "day",
    "wifiScore", "reliabilityScore", "speedScore", "coverageScore",
    "numerical_reliabilityScore", "numerical_speedScore", "numerical_coverageScore",
    "rssi_numeric", "RSSI_category",
    "phyrate_numeric", "phyrate_category",
    "no_poor_airtime", "Airtime_Utilization_Category",
    "no_sudden_drop", "sudden_drop_category",
    "ip_change_category", "band_success_count",
    "steer_start_count", "steer_start_perHome_count",
    "total_reboots_count", "reboot_category",
    "mdm_resets_count", "user_reboot_count",
    "state", "county",
]


def build_prompt(
    request: dict[str, Any],
    metrics: dict[str, Any],
    kpi_context: str,
    question_type: str,
) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) ready to pass directly to the
    Claude API.

    The system prompt holds the KPI documentation and expert persona.
    The user prompt holds the device metrics, the question, the detected
    type, and a strict JSON output schema.
    """
    instructions = QUESTION_TYPE_INSTRUCTIONS.get(
        question_type,
        QUESTION_TYPE_INSTRUCTIONS["Diagnosis"],   # safe fallback
    )

    metrics_block = "\n".join(
        f"  {col:<38} = {metrics.get(col, 'N/A')}"
        for col in PROMPT_COLUMNS
    )

    system_prompt = textwrap.dedent(f"""
        You are a WiFi diagnostics expert assistant for a telecommunications company.
        You have access to:
          1. A customer's real-time device metrics (provided in the user message).
          2. The official KPI documentation that explains how scores are computed.

        Persona: knowledgeable, calm, and customer-friendly.
        Always ground your answer in the actual metric values provided.
        Never invent values or reference metrics not present in the data.

        === KPI DOCUMENTATION (RAG-retrieved) ===
        {kpi_context}
        ==========================================
    """).strip()

    user_prompt = textwrap.dedent(f"""
        Device SN   : {request['sn']}
        Date        : {request['target_date']}
        Question    : "{request['question']}"

        --- Device Metrics ---
        {metrics_block}
        ----------------------

        Question Type Detected: {question_type}

        Instructions for this question type:
        {instructions.strip()}

        IMPORTANT - Output format:
        Reply with valid JSON only.  No markdown fences, no preamble.
        Use exactly these keys:
        {{
            "summary":         "<one-sentence answer in plain English>",
            "detail":          "<full explanation following the instructions above>",
            "metrics_cited":   ["<metric_name>", ...],
            "recommendations": ["<step>", ...]
        }}
        "recommendations" should be an empty list [] when not applicable.
    """).strip()

    return system_prompt, user_prompt





# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 6 - call_claude
#  Single API call; parses and returns the model's JSON dict.
# ══════════════════════════════════════════════════════════════════════════════

def call_claude(
    client: anthropic.Anthropic,
    system_prompt: str,
    user_prompt: str,
) -> dict[str, Any]:
    """
    Calls Claude with the assembled prompts and returns the model's structured
    output as a plain Python dict.

    Markdown fences (```json ... ```) are stripped before parsing.
    A JSONDecodeError falls back to a safe envelope so the pipeline never
    crashes at this stage.
    """
    response = client.messages.create(
        model      = CLAUDE_MODEL,
        max_tokens = 1024,
        system     = system_prompt,
        messages   = [{"role": "user", "content": user_prompt}],
    )

    raw_text = response.content[0].text.strip()

    # Strip optional markdown fences the model sometimes adds
    if raw_text.startswith("```"):
        raw_text = raw_text.split("```")[1]
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
        raw_text = raw_text.strip()

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return {
            "summary":         "Unable to parse structured response.",
            "detail":          raw_text,
            "metrics_cited":   [],
            "recommendations": [],
        }


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 7 - build_response
#  Package everything into the final output schema.
# ══════════════════════════════════════════════════════════════════════════════

def build_response(
    request: dict[str, Any],
    metrics: dict[str, Any],
    question_type: str,
    llm_output: dict[str, Any],
) -> dict[str, Any]:
    """
    Final output schema (for software engineers consuming this pipeline)
    -------------------------------------------------------------------
    {
        "request": {
            "sn":          str,   // device serial number (sole identifier)
            "question":    str,   // original question text
            "target_date": str    // date of the metrics snapshot
        },
        "question_type": str,     // one of the six QUESTION_TYPES
        "device_snapshot": {      // key KPIs for UI rendering / logging
            "wifiScore":          str,
            "reliabilityScore":   str,
            "speedScore":         str,
            "coverageScore":      str,
            "rssi_category":      str,
            "phyrate_category":   str,
            "reboot_category":    str,
            "airtime_category":   str
        },
        "answer": {
            "summary":         str,    // one-sentence headline answer
            "detail":          str,    // full explanation
            "metrics_cited":   [str],  // metric column names referenced
            "recommendations": [str]   // actionable steps; [] if N/A
        }
    }
    """
    return {
        "request": {
            "sn":          request["sn"],
            "question":    request["question"],
            "target_date": request["target_date"],
        },
        "question_type": question_type,
        "device_snapshot": {
            "wifiScore":        str(metrics.get("wifiScore",                    "N/A")),
            "reliabilityScore": str(metrics.get("reliabilityScore",             "N/A")),
            "speedScore":       str(metrics.get("speedScore",                   "N/A")),
            "coverageScore":    str(metrics.get("coverageScore",                "N/A")),
            "rssi_category":    str(metrics.get("RSSI_category",                "N/A")),
            "phyrate_category": str(metrics.get("phyrate_category",             "N/A")),
            "reboot_category":  str(metrics.get("reboot_category",              "N/A")),
            "airtime_category": str(metrics.get("Airtime_Utilization_Category", "N/A")),
        },
        "answer": llm_output,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 8 - run_pipeline
#  Orchestrates all stages end-to-end.
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    sn: str,
    question: str,
    target_date: Optional[str] = None,
    *,
    spark: SparkSession,
    anthropic_client: Optional[anthropic.Anthropic] = None,
) -> dict[str, Any]:
    """
    Main entry point.

    Parameters
    ----------
    sn              : Device serial number (the sole identifier).
    question        : Free-text customer question.
    target_date     : 'YYYY-MM-DD' metrics date.  Defaults to today.
    spark           : Active SparkSession (required).
    anthropic_client: Pre-built Anthropic client.  Created automatically from
                      ANTHROPIC_API_KEY env var if not supplied.

    Returns
    -------
    Final response dict (see build_response docstring for full schema).
    """

    # 1.  Validate & normalise input
    print("[1/7] Building request ...")
    request = build_request(sn, question, target_date)

    # 2.  Fetch device metrics from HDFS parquet
    print("[2/7] Loading device metrics from HDFS parquet ...")
    metrics = load_device_metrics(spark, request)

    # 3.  Retrieve relevant KPI chunks from RAG
    print("[3/7] Retrieving KPI context from RAG ...")
    kpi_context = retrieve_kpi_context(_get_rag_retriever(), request["question"])

    # 4.  Classify the question
    print("[4/7] Classifying question type ...")
    client = anthropic_client or anthropic.Anthropic()
    question_type = classify_question(client, request["question"])
    print(f"      -> Detected: {question_type}")

    # 5.  Build stage-specific prompt
    print("[5/7] Assembling prompt ...")
    system_prompt, user_prompt = build_prompt(
        request, metrics, kpi_context, question_type
    )

    # 6.  Call Claude
    print("[6/7] Calling Claude ...")
    llm_output = call_claude(client, system_prompt, user_prompt)

    # 7.  Package final response
    print("[7/7] Building final response ...")
    result = build_response(request, metrics, question_type, llm_output)

    print("Pipeline complete.\n")
    return result

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_wifi_score')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    result_data = run_pipeline( sn = "AA114400877",
                            question    = "Why is my speed score lower than my reliability score?",
                            target_date = "2026-03-20",
                            spark = spark,
                            anthropic_client = anthropic.Anthropic(api_key = ANTHROPIC_API_KEY),
                            )
    # 2. Extract variables for easy reading
    sn = result_data['request']['sn']
    question = result_data['request']['question']
    summary = result_data['answer']['summary']
    detail = result_data['answer']['detail']

    # 3. Build a clean HTML template
    # (Using <pre> tag for the detail section to perfectly preserve the line breaks and bullet points)
    html_string = f"""
    <div style="font-family: Arial, sans-serif; max-width: 800px; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px; background-color: #fcfcfc;">
        <h3 style="color: #202124; margin-top: 0;">Device Analysis: {sn}</h3>
        <p style="font-size: 14px; color: #5f6368;"><strong>Customer Query:</strong> <em>"{question}"</em></p>
        <hr style="border: 0; border-top: 1px solid #e0e0e0; margin: 15px 0;">
        
        <h4 style="color: #1a73e8; margin-bottom: 5px;">Summary</h4>
        <p style="font-size: 15px; color: #202124; line-height: 1.5; margin-top: 0;">{summary}</p>
        
        <h4 style="color: #1a73e8; margin-bottom: 5px; margin-top: 20px;">Detailed Breakdown</h4>
        <pre style="font-family: Arial, sans-serif; font-size: 14px; color: #3c4043; background-color: #ffffff; padding: 15px; border: 1px solid #e0e0e0; border-radius: 6px; white-space: pre-wrap;">{detail}</pre>
    </div>
    """

    # 4. Tell Zeppelin to render the HTML string
    print(f"%html\n{html_string}")
    
    import sys
    