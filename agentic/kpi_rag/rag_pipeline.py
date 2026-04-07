"""
================================================================================
  WiFi KPI RAG Pipeline
  ---------------------------------------------------------------------------
  Purpose : Answer plain-English questions about the WiFi KPI document.
            Assumes the Redis vector store is already built and populated.

  Stages
  ------
  1.  build_retriever()  → connect to existing Redis index, return retriever
  2.  build_chain()      → assemble the LangChain RAG chain
  3.  run_query()        → answer one question using the chain
  4.  init_rag()         → orchestrate stages 1-2 (called once at startup)

================================================================================
"""

import os
import textwrap
from typing import Any, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_redis import RedisVectorStore
from langchain_redis.config import RedisConfig
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

OPENAI_API_KEY = os.getenv(
    "OPENAI_API_KEY",
)

REDIS_HOST     = os.getenv("REDIS_HOST",     "redis-10340.c251.east-us-mz.azure.cloud.redislabs.com")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "10340"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "szBRg5Pe2IspC5m9m72kaye3w4HyBpwA")
REDIS_URL      = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
REDIS_INDEX    = "wifi_kpi_rag"

EMBED_MODEL = "text-embedding-3-large"
LLM_MODEL   = "gpt-4o"

RAG_SYSTEM_PROMPT = textwrap.dedent("""
    You answer questions about a WiFi KPI document for a telecommunications company.

    Rules:
    1. Use ONLY the retrieved context below.  Do not invent values or metrics.
    2. Preserve the score hierarchy exactly as written in the document:
       KPI → bucket score → final WiFi score.
    3. If X affects Y only through an intermediate layer, state the full path.
    4. Never say a KPI directly determines the WiFi score unless the document
       explicitly states this.
    5. If the context is insufficient, say: "I don't have enough information."

    Answer format:
    - Short direct answer (1-2 sentences)
    - Supporting logic with evidence from the context
    - If relevant, list any intermediate aggregation steps

    Context:
    {context}
""").strip()


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 1 - build_retriever
#  Connect to the existing Redis vector index and return a retriever.
# ══════════════════════════════════════════════════════════════════════════════

def build_retriever(*, openai_api_key: str, top_k: int = 5) -> Any:
    """
    Connects to the pre-built Redis vector store and returns a similarity
    search retriever.  No embedding or ingestion is performed here.

    Parameters
    ----------
    openai_api_key : Used to embed the incoming query at retrieval time.
    top_k          : Number of chunks to retrieve per query.

    Returns
    -------
    A LangChain retriever ready for use in a chain.
    """
    print("[1/2] Connecting to Redis vector store ...")

    embeddings = OpenAIEmbeddings(model=EMBED_MODEL, api_key=openai_api_key)

    config = RedisConfig(
        index_name=REDIS_INDEX,
        redis_url=REDIS_URL,
        metadata_schema=[
            {"name": "doc_id",   "type": "tag"},
            {"name": "chunk_id", "type": "numeric"},
            {"name": "h1",       "type": "tag"},
            {"name": "h2",       "type": "tag"},
        ],
    )

    vectorstore = RedisVectorStore(embeddings=embeddings, config=config)

    print(f"      -> Connected to index '{REDIS_INDEX}'")
    return vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": top_k},
    )


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 2 - build_chain
#  Assemble the LangChain RAG chain (retriever + prompt + LLM).
# ══════════════════════════════════════════════════════════════════════════════

def build_chain(retriever: Any, *, llm: ChatOpenAI) -> Any:
    """
    Creates a retrieval chain: question → retrieve context → LLM answer.

    Returns a runnable chain.  Call it with:
        chain.invoke({"input": "<question>"})
    """
    print("[2/2] Assembling RAG chain ...")

    prompt = ChatPromptTemplate.from_messages([
        ("system", RAG_SYSTEM_PROMPT),
        ("human", "{input}"),
    ])

    document_chain = create_stuff_documents_chain(llm, prompt)
    chain = create_retrieval_chain(retriever, document_chain)

    print("RAG pipeline ready.\n")
    return chain


# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 3 - run_query
#  Answer a single question using the pre-built chain.
# ══════════════════════════════════════════════════════════════════════════════

def run_query(chain: Any, question: str) -> dict[str, Any]:
    """
    Parameters
    ----------
    chain    : The RAG chain returned by build_chain().
    question : Free-text question about the WiFi KPI document.

    Returns
    -------
    {
        "question": str,
        "answer":   str,
        "sources":  [{"content": str, "metadata": dict}, ...]
    }
    """
    if not question or not question.strip():
        raise ValueError("question must not be empty.")

    result = chain.invoke({"input": question.strip()})

    sources = [
        {"content": doc.page_content, "metadata": doc.metadata}
        for doc in result.get("context", [])
    ]

    return {
        "question": question.strip(),
        "answer":   result["answer"],
        "sources":  sources,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  init_rag
#  Orchestrate stages 1-2.  Call this once at server startup.
# ══════════════════════════════════════════════════════════════════════════════

def init_rag(*, openai_api_key: Optional[str] = None, top_k: int = 5) -> Any:
    """
    Connects to the existing Redis index and returns a ready-to-query chain.

    Parameters
    ----------
    openai_api_key : OpenAI API key.  Falls back to OPENAI_API_KEY constant.
    top_k          : Number of chunks to retrieve per query.

    Returns
    -------
    Runnable RAG chain.  Pass to run_query() for each incoming question.
    """
    api_key = openai_api_key or OPENAI_API_KEY
    os.environ["OPENAI_API_KEY"] = api_key

    llm = ChatOpenAI(model=LLM_MODEL, temperature=0, api_key=api_key)

    retriever = build_retriever(openai_api_key=api_key, top_k=top_k)
    chain     = build_chain(retriever, llm=llm)

    return chain


# ══════════════════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    chain = init_rag()

    test_questions = [
        "How does a reboot affect the WiFi score?",
        "What is the difference between reliabilityScore and speedScore?",
        "How is coverageScore calculated?",
    ]

    for q in test_questions:
        print(f"\n{'='*60}")
        print(f"Q: {q}")
        print("="*60)
        result = run_query(chain, q)
        print(f"A: {result['answer']}")
        print(f"\nSources ({len(result['sources'])}):")
        for i, s in enumerate(result["sources"], 1):
            print(f"  [{i}] {s['content'][:120].strip()} ...")
