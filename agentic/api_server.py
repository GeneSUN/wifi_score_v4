"""
================================================================================
  WiFi Score Diagnostic API  -  api_server.py
  ---------------------------------------------------------------------------
  Wraps wifi_score_pipeline.py in a FastAPI HTTP server.

  HOW TO RUN (via spark-submit, using api_server.sh):
  ----------------------------------------------------
  bash /usr/apps/vmas/scripts/ZS/agentic/api_server.sh

  Endpoints
  ---------
  POST /ask      - run the full pipeline, return structured JSON answer
  GET  /health   - liveness check (confirms Spark + Anthropic are ready)

  Notes
  -----
  * Must be launched via spark-submit (not plain python) so PySpark can
    connect to the remote Spark cluster.
  * SparkSession is created ONCE via getOrCreate() - it picks up all the
    --master, --executor, --conf flags from spark-submit automatically.
  * workers=1 is intentional: SparkSession is not fork-safe.
================================================================================
"""

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Optional

import anthropic
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

# ── your pipeline module (must be in the same directory) ─────────────────────
from wifi_score_pipeline import ANTHROPIC_API_KEY, run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  Shared resources  (created once at startup, reused for every request)
# ══════════════════════════════════════════════════════════════════════════════

_spark: Optional[SparkSession] = None
_anthropic_client: Optional[anthropic.Anthropic] = None
_executor = ThreadPoolExecutor(max_workers=4)


def get_spark() -> SparkSession:
    """
    Returns the active SparkSession.
    Because this script is launched via spark-submit, SparkSession.getOrCreate()
    automatically picks up the --master, --executor-memory, --conf flags etc.
    We do NOT set those here - spark-submit already set them.
    """
    global _spark
    if _spark is None:
        log.info("Initialising SparkSession (picking up spark-submit config) ...")
        _spark = SparkSession.builder \
            .appName("wifi_score_api") \
            .getOrCreate()
        # Suppress noisy Spark INFO logs in the API output
        _spark.sparkContext.setLogLevel("WARN")
        log.info("SparkSession ready.")
    return _spark


def get_anthropic() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        log.info("Anthropic client ready.")
    return _anthropic_client


# ══════════════════════════════════════════════════════════════════════════════
#  App lifecycle  (runs at startup and shutdown)
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=== API startup: warming up Spark and Anthropic ===")
    get_spark()
    get_anthropic()
    log.info("=== All resources ready. Accepting requests. ===")
    yield
    log.info("=== API shutdown ===")
    if _spark:
        _spark.stop()


# ══════════════════════════════════════════════════════════════════════════════
#  FastAPI app
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="WiFi Score Diagnostic API",
    description="Ask plain-English questions about a device's WiFi metrics.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS - allows the dashboard (different port or opened as local file) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
#  Request / Response schemas
# ══════════════════════════════════════════════════════════════════════════════

class AskRequest(BaseModel):
    sn: str = Field(
        ...,
        description="Device serial number - the sole user identifier.",
        examples=["AA114400877"],
    )
    question: str = Field(
        ...,
        description="Free-text question about the device's WiFi performance.",
        examples=["Why is my speed score lower than my reliability score?"],
    )
    target_date: Optional[str] = Field(
        default=None,
        description="Metrics date in YYYY-MM-DD format. Defaults to today.",
        examples=["2026-03-20"],
    )


class DeviceSnapshot(BaseModel):
    wifiScore: str
    reliabilityScore: str
    speedScore: str
    coverageScore: str
    rssi_category: str
    phyrate_category: str
    reboot_category: str
    airtime_category: str


class Answer(BaseModel):
    summary: str
    detail: str
    metrics_cited: list[str]
    recommendations: list[str]


class AskResponse(BaseModel):
    request: dict
    question_type: str
    device_snapshot: DeviceSnapshot
    answer: Answer


# ══════════════════════════════════════════════════════════════════════════════
#  Endpoints
# ══════════════════════════════════════════════════════════════════════════════

async def run_pipeline_async(sn: str, question: str, target_date: Optional[str]):
    """
    Runs the blocking PySpark + Anthropic pipeline in a thread-pool so it
    does not block FastAPI's async event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: run_pipeline(
            sn=sn,
            question=question,
            target_date=target_date,
            spark=get_spark(),
            anthropic_client=get_anthropic(),
        ),
    )


@app.post(
    "/ask",
    response_model=AskResponse,
    summary="Ask a diagnostic question about a device",
)
async def ask(body: AskRequest):
    """
    Runs the full 7-stage WiFi diagnostic pipeline and returns a structured answer.

    - **sn**: device serial number
    - **question**: plain-English question about the device
    - **target_date**: which day's metrics to use (defaults to today)
    """
    log.info(f"POST /ask  sn={body.sn}  date={body.target_date}  q={body.question[:80]}")
    try:
        result = await run_pipeline_async(body.sn, body.question, body.target_date)
        return result
    except LookupError as e:
        # Device / date not found in parquet
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        # Bad input (empty sn, empty question, etc.)
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        log.exception("Unexpected pipeline error")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")


@app.get("/health", summary="Health / liveness check")
async def health():
    """Returns 200 with Spark and Anthropic status."""
    return {
        "status":           "ok",
        "spark_ready":      _spark is not None,
        "anthropic_ready":  _anthropic_client is not None,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
#  When spark-submit runs this file, __name__ == "__main__", so uvicorn starts.
#  The SparkSession created above reuses the spark-submit context automatically.
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        app,                  # pass the app object directly (not a string)
                              # because spark-submit can't do module reloading
        host="0.0.0.0",       # listen on all interfaces so browser can reach it
        port=8000,
        workers=1,            # MUST be 1 - SparkSession is not fork-safe
        reload=False,
        log_level="info",
    )
