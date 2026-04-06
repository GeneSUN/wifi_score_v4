# WiFi KPI RAG API — Setup & Access Guide


## What This Is


A web chatbot that answers plain-English questions about the WiFi KPI document.
Ask anything about score calculations, metric definitions, thresholds, or how
KPIs relate to each other — and get a grounded answer with source evidence.

Unlike the WiFi Score Diagnostic API (which queries live device data from HDFS),
this system has no Spark dependency. It runs on any machine with Python 3.10+.


---


## Files


| File | Purpose |
|---|---|
| `rag_pipeline.py` | Core pipeline: loads KPI doc, chunks, embeds, indexes, answers |
| `rag_server.py` | FastAPI HTTP server wrapping the pipeline (port 8001) |
| `rag_dashboard.html` | Browser UI — open this on your laptop |


---


## One-Time Setup


### 1. Install Python dependencies

```bash
pip install fastapi uvicorn \
            langchain langchain-openai langchain-redis \
            langchain-community langchain-text-splitters \
            requests
```


### 2. Verify credentials

The credentials below are already set as defaults in `rag_pipeline.py`.
Override them with environment variables if needed:

| Variable | Default set in | What it controls |
|---|---|---|
| `OPENAI_API_KEY` | `rag_pipeline.py:39` | OpenAI embeddings + GPT-4o |
| `REDIS_HOST` | `rag_pipeline.py:42` | Redis Cloud hostname |
| `REDIS_PORT` | `rag_pipeline.py:43` | Redis Cloud port |
| `REDIS_PASSWORD` | `rag_pipeline.py:44` | Redis Cloud password |

To override without editing the file:
```bash
export OPENAI_API_KEY="sk-proj-..."
export REDIS_PASSWORD="your-password"
```


### 3. Verify Redis is reachable (optional)

```bash
python3 -c "
import redis
conn = redis.Redis(
    host='redis-10340.c251.east-us-mz.azure.cloud.redislabs.com',
    port=10340,
    password='szBRg5Pe2IspC5m9m72kaye3w4HyBpwA',
    socket_connect_timeout=5
)
print('Connected:', conn.ping())
"
```


---


## Running the Server


### Step 1 — Start the RAG API server

```bash
cd /usr/apps/vmas/scripts/ZS/agentic
python rag_server.py
```

Startup takes **~60 seconds** the first time (fetches the doc, embeds all chunks,
ingests into Redis). Subsequent restarts are faster because Redis retains the index.

Wait until you see:
```
=== RAG chain ready. Accepting requests. ===
```
Leave this terminal open.


### Step 2 — Forward port 8001 via VS Code

1. Open VS Code → connect to the server via Remote-SSH
2. Click the **PORTS** tab at the bottom
3. Click **Forward a Port** → type `8001` → press Enter


### Step 3 — Verify the API is working

```
http://localhost:8001/health
```
Expected response:
```json
{"status": "ok", "rag_ready": true}
```


### Step 4 — Open the dashboard

Open `rag_dashboard.html` directly in your browser (double-click the file).

Make sure the `API_BASE` variable inside the file points to:
```javascript
const API_BASE = 'http://localhost:8001';
```


---


## Using the Dashboard


1. Click one of the **quick-pick chips** or type your own question
2. Press **Enter** or click **Ask**
3. The answer appears with:
   - A direct answer grounded in the KPI document
   - **Show sources** — the exact chunks retrieved from the vector store,
     with heading metadata (section, subsection)
   - **Recent queries** history — click any past query to reload it


Example questions:
- "How does reboot affect the WiFi score?"
- "What is the difference between reliabilityScore and speedScore?"
- "What thresholds define Excellent vs Poor phyrate?"
- "How is coverageScore calculated?"
- "How does band steering relate to reliability?"


---


## Test the API Directly


```bash
# Health check
curl http://localhost:8001/health

# Ask a question
curl -X POST http://localhost:8001/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "How does reboot affect the WiFi score?"}'
```

Swagger UI (interactive docs):
```
http://localhost:8001/docs
```


---


## Debugging


### "Failed to fetch" in the browser
- Check VS Code PORTS tab — is port 8001 still forwarded?
- Check that `python rag_server.py` is still running
- Try `http://localhost:8001/health` in the browser


### Server failed to start
Common causes:

| Error | Fix |
|---|---|
| `ModuleNotFoundError` | Run the `pip install` command above |
| `ConnectionError` (Redis) | Check Redis credentials and network access |
| `HTTPError` (GitHub URL) | Check network; or pass a local file path to `init_rag()` |


### Use a local copy of the KPI document instead of the URL

Edit the `init_rag()` call at the bottom of `rag_server.py`:
```python
_chain = await loop.run_in_executor(
    _executor,
    lambda: init_rag("/path/to/wifiScore_KPI_Document_cleaned_for_rag.md")
)
```


### Re-embed from scratch (force rebuild)

If you update the KPI document and need to re-index:
```python
init_rag(force_rebuild=True)
```


---


## Architecture


```
Your Laptop Browser
      |
      | http://localhost:8001  (VS Code SSH tunnel)
      |
VS Code Remote SSH ──────────────────────────────── Server: njbbvmaspd11
                                                         |
                                                   rag_server.py
                                                   (FastAPI on port 8001)
                                                         |
                                              ┌──────────┴──────────┐
                                              |                     |
                                       rag_pipeline.py         OpenAI API
                                              |            (text-embedding-3-large
                                      Redis Vector Store       + GPT-4o)
                                     (wifi_kpi_rag index)
                                              |
                                    Hybrid Retriever
                               (BM25 sparse + vector dense)
                                      + LLM Compressor
```


---


## Key Facts


| Item | Value |
|---|---|
| API port | `8001` |
| Health check | `http://localhost:8001/health` |
| Swagger UI | `http://localhost:8001/docs` |
| LLM model | `gpt-4o` |
| Embedding model | `text-embedding-3-large` |
| Redis index name | `wifi_kpi_rag` |
| KPI document source | GitHub raw URL (auto-fetched at startup) |
| Chunk size | 1200 chars, 150 overlap |
| Retrieval strategy | BM25 (40%) + vector (60%) with LLM compression |
