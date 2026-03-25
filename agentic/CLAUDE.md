# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A GenAI-powered WiFi diagnostic API for Verizon that uses Claude AI + PySpark to answer plain-English questions about device WiFi metrics. The system serves customer support staff querying telemetry data for 11M+ households.

## Running the API

**Production (on server):**
```bash
ssh vmasadm@njbbvmaspd11.nss.vzwnet.com
cd /usr/apps/vmas/scripts/ZS/agentic
bash api_server.sh
# Wait for: "=== All resources ready. Accepting requests. ==="
```

**Health check:**
```bash
curl http://localhost:8000/health
# Expected: {"status": "ok", "spark_ready": true, "anthropic_ready": true}
```

**Test a query:**
```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"sn": "AA114400877", "question": "Why is my WiFi score low?", "target_date": "2026-03-20"}'
```

**Dependencies** (no requirements.txt — install manually):
```bash
pip install fastapi uvicorn pydantic anthropic
# PySpark is available in the cluster environment
```

## Architecture

### 7-Stage Pipeline (`wifi_score_pipeline.py`)

Each request flows through these stages in order:

1. **`build_request()`** — Validates/normalizes input (SN, question, date)
2. **`load_device_metrics()`** — Reads device data from HDFS Parquet (`/user/ZheS/wifi_score_v4/wifiScore_location/{date}`)
3. **`load_kpi_context()`** — Loads KPI documentation (currently embedded text; designed as extension point for PDF reading)
4. **`classify_question()`** — Claude API call #1: categorizes question into one of 6 types (Diagnosis, Metric explanation, Score calculation, Comparison, Recommendation, Trend/anomaly)
5. **`build_prompt()`** — Assembles stage-specific system + user prompts tailored to the classified question type
6. **`call_claude()`** — Claude API call #2: generates the structured JSON answer
7. **`build_response()`** — Packages final output

Every request makes **exactly 2 Claude API calls** (classify → answer) and takes ~20-30 seconds.

### API Server (`api_server.py`)

FastAPI server wrapping the pipeline:
- `uvicorn` runs with `workers=1` — intentional, SparkSession is not fork-safe
- Spark + Anthropic client initialized once at startup via `lifespan` context manager
- Blocking pipeline runs in `ThreadPoolExecutor` to avoid blocking the async event loop
- `POST /ask` — main endpoint; `GET /health` — liveness check; `/docs` — Swagger UI

### Data Flow

```
dashboard.html → POST /ask → api_server.py → run_pipeline() → HDFS Parquet + Anthropic API
```

### Response Schema

```json
{
  "request": {"sn", "question", "target_date"},
  "question_type": "<one of 6 types>",
  "device_snapshot": {"wifiScore", "reliabilityScore", "speedScore", "coverageScore",
                      "rssi_category", "phyrate_category", "reboot_category", "airtime_category"},
  "answer": {"summary", "detail", "metrics_cited": [], "recommendations": []}
}
```

## Key Configuration

| Setting | Location | Value |
|---------|----------|-------|
| Claude model | `wifi_score_pipeline.py:87` | `claude-sonnet-4-20250514` |
| HDFS root | `wifi_score_pipeline.py:50` | `hdfs://njbbvmaspd11.nss.vzwnet.com:9000` |
| Parquet path | `wifi_score_pipeline.py:51` | `/user/ZheS/wifi_score_v4/wifiScore_location` |
| API base URL | `dashboard.html:321` | `http://10.134.181.45:8000` |
| Spark master | `api_server.sh` | `spark://njbbepapa1.nss.vzwnet.com:7077` |

## Frontend (`dashboard.html`)

Single-page app (vanilla HTML/CSS/JS, no build tools). Features: SN + date + question inputs, 6 quick-pick question chips, score tiles, AI answer display, 10-query history, dark mode.

## Error Handling

- `LookupError` → 404: device/date not found in Parquet
- `ValueError` → 422: bad input (empty SN or question)
- Other exceptions → 500: pipeline failures

Parquet data is partitioned by date — 404s are expected when querying dates with no ingested data.
