"""
================================================================================
  WiFi KPI RAG API  -  rag_server.py
  ---------------------------------------------------------------------------
  Wraps rag_pipeline.py in a FastAPI HTTP server.

  HOW TO RUN:
  ----------- 
      pip install fastapi uvicorn
      python rag_server.py

  Endpoints
  ---------
  POST /ask      - answer a question about the KPI document
  GET  /health   - liveness check (confirms RAG chain is ready)
  GET  /docs     - Swagger UI (auto-generated)

  Notes
  -----
  * The RAG chain (document load → embed → index → chain) runs ONCE at startup.
  * Subsequent /ask requests hit Redis for retrieval and OpenAI for the LLM.
  * Runs on port 8001 (wifi_score API uses 8000).
================================================================================
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from rag_pipeline import OPENAI_API_KEY, init_rag, run_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  Shared resources  (created once at startup)
# ══════════════════════════════════════════════════════════════════════════════

_chain: Optional[Any] = None
_executor = ThreadPoolExecutor(max_workers=4)


def get_chain() -> Any:
    if _chain is None:
        raise RuntimeError("RAG chain not initialised yet.")
    return _chain


# ══════════════════════════════════════════════════════════════════════════════
#  App lifecycle
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _chain
    log.info("=== RAG API startup: building pipeline (this may take ~60s) ===")
    loop = asyncio.get_event_loop()
    _chain = await loop.run_in_executor(_executor, init_rag)
    log.info("=== RAG chain ready. Accepting requests. ===")
    yield
    log.info("=== RAG API shutdown ===")


# ══════════════════════════════════════════════════════════════════════════════
#  FastAPI app
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="WiFi KPI RAG API",
    description="Ask plain-English questions about the WiFi KPI document.",
    version="1.0.0",
    lifespan=lifespan,
)

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
    question: str = Field(
        ...,
        description="Plain-English question about the WiFi KPI document.",
        examples=["How does a reboot affect the WiFi score?"],
    )


class SourceDoc(BaseModel):
    content:  str
    metadata: dict


class AskResponse(BaseModel):
    question: str
    answer:   str
    sources:  list[SourceDoc]


# ══════════════════════════════════════════════════════════════════════════════
#  Endpoints
# ══════════════════════════════════════════════════════════════════════════════

async def _run_query_async(question: str) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: run_query(get_chain(), question),
    )


@app.post(
    "/ask",
    response_model=AskResponse,
    summary="Ask a question about the WiFi KPI document",
)
async def ask(body: AskRequest):
    """
    Retrieves relevant KPI document chunks and generates a grounded answer.

    - **question**: any plain-English question about WiFi scores and KPIs
    """
    log.info(f"POST /ask  q={body.question[:100]}")
    try:
        result = await _run_query_async(body.question)
        return result
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        log.exception("RAG pipeline error")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")


@app.get("/health", summary="Health / liveness check")
async def health():
    """Returns 200 when the RAG chain is initialised and ready."""
    return {
        "status":    "ok",
        "rag_ready": _chain is not None,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        workers=1,       # keep to 1 — chain init is not fork-safe
        reload=False,
        log_level="info",
    )
