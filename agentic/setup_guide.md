# WiFi Score Diagnostic API — Setup & Access Guide


## What This Is


A web dashboard that lets you ask plain-English questions about any device's
WiFi metrics. Type a serial number (SN), pick a date, ask a question — get a
structured AI-generated diagnostic answer.


---


## Files


| File | Purpose |
|---|---|
| `wifi_score_pipeline.py` | Core pipeline: loads metrics from HDFS, calls Claude, returns answer |
| `api_server.py` | FastAPI HTTP server wrapping the pipeline |
| `api_server.sh` | Shell script to launch the server via spark-submit |
| `dashboard.html` | Browser UI — open this on your laptop |


All files live on the server at:
```
/usr/apps/vmas/scripts/ZS/agentic/
```


---


## One-Time Setup


### 1. Install Python dependencies on the server
```bash
ssh vmasadm@njbbvmaspd11.nss.vzwnet.com
pip install fastapi uvicorn pydantic anthropic
```


### 2. Install VS Code on your laptop
Download from: https://code.visualstudio.com


### 3. Install the Remote - SSH extension in VS Code
`Ctrl+Shift+X` → search `Remote SSH` → Install


---


## Every Time You Want to Use the Dashboard


### Step 1 — Start the API server on the server


SSH into the server (via SecureCRT or any terminal):
```bash
ssh vmasadm@njbbvmaspd11.nss.vzwnet.com
# password: Summer2021@

sudo su
cd /usr/apps/vmas/scripts/ZS/agentic
bash api_server.sh
```


Wait until you see this line in the output:
```
=== All resources ready. Accepting requests. ===
```
Leave this terminal open — the server must keep running.


### Step 2 — Connect VS Code to the server and forward port 8000


1. Open VS Code on your laptop
2. Press `Ctrl+Shift+P` → type `Remote-SSH: Connect to Host` → enter:
   ```
   vmasadm@njbbvmaspd11.nss.vzwnet.com
   ```
3. Enter password: `Summer2021@`
4. Click the **PORTS** tab at the bottom of VS Code
5. Click **Forward a Port** → type `8000` → press Enter


VS Code now tunnels `localhost:8000` on your laptop to port 8000 on the server.
Keep VS Code open while using the dashboard.


### Step 3 — Verify the API is working


Open your laptop browser and go to:
```
http://localhost:8000/health
```
You should see:
```json
{"status": "ok", "spark_ready": true, "anthropic_ready": true}
```


### Step 4 — Open the dashboard


In your laptop browser go to:
```
http://localhost:8000/docs
```
Or open `dashboard.html` directly (double-click the file on your laptop).
Make sure `API_BASE` inside `dashboard.html` is set to:
```javascript
const API_BASE = "http://localhost:8000";
```


---


## Using the Dashboard


1. Enter the device **Serial Number (SN)** — e.g. `AA114400877`
2. Pick a **Date** — the day's metrics you want to query
3. Type a **Question** — or click one of the quick-pick chips:
   - "Why is my WiFi score low?"
   - "How is the WiFi score calculated?"
   - "Why is speed worse than coverage?"
   - "How can I improve my WiFi?"
   - "What does phyrate mean?"
   - "Are there any anomalies in my metrics?"
4. Click **Ask** — wait ~30 seconds for Spark + Claude to respond
5. The result shows:
   - **Question type** detected (Diagnosis, Comparison, Recommendation, etc.)
   - **Score tiles** — wifiScore, reliability, speed, coverage
   - **KPI summary** — RSSI, PHY rate, reboots, airtime
   - **Answer** — summary + full detail
   - **Recommendations** — actionable steps if applicable
   - **Metrics cited** — which columns the answer is based on


---


## Debugging


### "Failed to fetch" in the browser
- Check VS Code PORTS tab — is port 8000 still forwarded?
- Check the server — is `api_server.sh` still running?
- Try: `http://localhost:8000/health` in the browser


### API server crashed
```bash
# Check the log on the server
cat /usr/apps/vmas/scripts/ZS/agentic/api_server.log
```


### Test the API directly from the server
```bash
curl http://localhost:8000/health


curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"sn": "AA114400877", "question": "Why is my WiFi score low?", "target_date": "2026-03-20"}'
```


### Check if the server is running
```bash
ss -tlnp | grep 8000
```
If nothing shows — the server is not running. Restart with `bash api_server.sh`.


---


## Architecture


```
Your Laptop Browser
      |
      | http://localhost:8000  (VS Code SSH tunnel)
      |
VS Code Remote SSH ──────────────────────────────── Server: njbbvmaspd11
                                                         |
                                                   api_server.py
                                                   (FastAPI on port 8000)
                                                         |
                                              ┌──────────┴──────────┐
                                              |                     |
                                     wifi_score_pipeline.py    Anthropic API
                                              |                (Claude)
                                         HDFS Parquet
                                       (WiFi metrics data)
```


---


## Key Facts


| Item | Value |
|---|---|
| Server | `njbbvmaspd11.nss.vzwnet.com` |
| Server IP | `10.134.181.45` |
| API port | `8000` |
| API docs (Swagger) | `http://localhost:8000/docs` |
| Health check | `http://localhost:8000/health` |
| Parquet data path | `hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/wifi_score_v4/wifiScore_location/` |