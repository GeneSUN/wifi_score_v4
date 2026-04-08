# Wi-Fi score data-augmented RAG pipeline

<img width="795" height="647" alt="Untitled" src="https://github.com/user-attachments/assets/1084ee12-b62a-4300-bca7-068bb8067dcf" />

---

# Generative-AI

## ⭐ Situation
**1. At Verizon, I worked on a project to build a hierarchical Wi-Fi Metrics system to represent customer network performance.**

The system had multiple layers of metrics:

- Top level: Overall Wi-Fi Score (customer experience)
- Mid level: Speed/Coverage/Reliability
- Low level metrics: Latency/Upload/download throughput


**2. While the scoring system successfully quantified network performance, stakeholders struggled to interpret the results.**

For example: A customer might report unstable network experience, Analysts would see dozens of metrics changing

It was unclear:
- Which metrics were actually degraded?
- Which ones were symptoms vs root causes?
- Which metric engineers should investigate first?

**3. Another major challenge was domain knowledge complexity.**
Telecom metrics are highly specialized:
- Low SNR → may indicate interference
- High steering → may indicate poor coverage
- Reboot spikes → device instability

Non-experts — including product managers and analysts — could not translate metrics into actionable insights.

As a result:
- The Wi-Fi Score system was technically correct
- But difficult to operate.
Interpretability became a critical bottleneck.


## ⭐ Task

**Overall Goal:**
Transform the WiFi Score system from a passive metric-reporting tool into an intelligent, self-explaining diagnostic engine — so that non-expert stakeholders could understand *why* a customer’s network is degraded and *what to do about it*, without needing to manually interpret dozens of KPIs.

**Granular Objectives:**

1. **Ground LLM responses in real customer data** — convert per-device KPI snapshots (RSSI, SNR, packet loss, retransmission, etc.) from Parquet into structured natural-language narratives the LLM could reason over accurately, avoiding hallucination on customer-specific facts.

2. **Inject telecom domain knowledge via RAG** — build and retrieve from a domain-specific knowledge base covering WiFi score construction, KPI definitions, and causal relationships (e.g., low SNR → interference), because a general-purpose LLM lacks this specialized context.

3. **Ensure responses are intent-aware** — the system's output should be determined by the type of request: a diagnosis question should yield root-cause analysis, an explanation question should yield concept clarification, a recommendation question should yield actionable steps — not a one-size-fits-all answer.

4. **Productionize at scale** — wrap the pipeline in a FastAPI service backed by PySpark on a cluster, capable of querying telemetry for 11M+ households on demand, with sub-30-second response times.


## Action:

<details>
<summary>System Inputs and Pipeline Flow</summary>

I built an end-to-end pipeline where two inputs enter the system and a diagnostic answer comes out.

For input of the system, The user provides two things:
- **Customer ID + date** — identifies which device and which day’s data to investigate
- **A natural-language question** — what the user wants to know (e.g., "Why is this customer’s Wi-Fi score low?")

Now, let's talk about the pipeline.

**1) Fetch customer data**
The customer ID and date are used to query the database and retrieve that device’s KPI snapshot (Wi-Fi score, coverage/speed/reliability sub-scores, RSSI, SNR, packet loss, retransmission, etc.), which is then converted into a natural-language summary for the LLM to reason over.

**2) Retrieve relevant domain context (RAG)**
The question is used to retrieve the most relevant knowledge from a three-source knowledge base I built:
- **Public telecom documentation** — chunked reference material on standard wireless concepts (RSSI, SNR, interference, steering, etc.)
- **Custom Wi-Fi Score documentation I authored** — a structured markdown doc explaining how each metric in our scoring system is defined, how the hierarchy is constructed, and how low-level KPIs drive mid-level scores
- **Internal codebase** — scripts, classes, and functions describing the system’s own logic

**3) Route to an intent-specific prompt**
The question is also passed through an LLM-based classifier that determines the question type — Diagnosis, Explanation, Recommendation, Comparison, Score Calculation, or Trend/Anomaly. Each type maps to a dedicated prompt template designed for that mode of response.

**4) Assemble and generate**
The three components — customer KPI summary, retrieved RAG context, and intent-specific prompt — are combined into a final prompt and fed to the LLM to produce the response.

</details>

---

<details>
<summary>RAG System Design — Components and Selection</summary>

Building the RAG system involved choosing the right component at each stage of the pipeline.

**Knowledge sources**

Three document collections form the retrieval corpus:
- **Public telecom documentation** — domain knowledge covering standard wireless concepts, signal metrics, and network behavior
- **Wi-Fi Score documentation I authored** — a ~10-page markdown document with clearly defined section titles, explaining how the scoring hierarchy is constructed, how each metric is defined, and how low-level KPIs roll up into sub-scores
- **Internal scripts** — codebase containing multiple classes, methods, and functions that describe the system's own logic

**Document preprocessing**

Before ingestion, each source required preprocessing — analogous to data preprocessing in machine learning. Raw documents are rarely retrieval-ready. 
- For example, the Wi-Fi Score documentation originally existed as a PDF with screenshots of tables and figures. I converted it into a clean markdown file: all tables were reconstructed as markdown tables, figures were replaced with descriptive text, and each section was given a structured heading — making it parseable and semantically retrievable.

**Document Chunk**: chunking strategy was chosen per document type. 
- For the Wi-Fi Score documentation, I evaluated three common strategies — fixed-size, semantic, and recursive — and selected recursive chunking: since the document is well-structured markdown with each section spanning 500–1000 words, the section boundaries naturally serve as clean split points. 
- For internal scripts, I applied a code-aware AST-based chunking strategy that respects class and function boundaries rather than splitting on character count,  LlamaIndex CodeSplitter.

**Embedding model**: selection depends on content type — 
- pure text favors lightweight models like `text-embedding-3-small`; 
- pure code favors `voyage-code-2`; 
- mathematical content has specialized models like `MathBERT`/`TeleBERT` which better preserves formula semantics.
Since our corpus is mixed — natural language documentation, structured markdown, and Python scripts — I used a general-purpose embedding model(`text-embedding-3-large`) that handles all three adequately rather than optimizing for one modality.

**Vector store**: I evaluated three candidates. 
- MongoDB Atlas Vector Search — Verizon has an active partnership with MongoDB — so it was the natural starting point. However, MongoDB is primarily a document database; when you are already storing application data (e.g., customer records) in MongoDB and want vector retrieval in the same system. As a standalone vector store it underperforms dedicated options, with less tunable indexing and higher latency at scale. 
- GCP Vector Search has a similar pattern — it integrates well within the GCP ecosystem but adds operational overhead and cold-start latency when used outside of it. 
- I ultimately selected **Redis**: it is a purpose-built, battle-tested in-memory store with native vector similarity search, sub-millisecond retrieval latency, and strong HNSW index support — making it the best fit for a pipeline where response time is a constraint.

**Retriever**: I implemented hybrid retrieval — combining dense semantic search with sparse keyword search (BM25) — and compared it against pure semantic search. Hybrid consistently outperformed. This is expected in technical domains: semantic search captures meaning well but can miss exact-match terms like `RSSI`, `SNR`, or `wifiScore` that don't have meaningful synonyms.
- **linked retrieval**
- The retriever also derives a **dedicated retrieval** query from the user question rather than using the raw input directly, which improves precision when the question contains conversational phrasing irrelevant to retrieval.
- Reranker: a cross-encoder reranker was considered to re-score the top-k candidates and improve precision before passing context to the LLM.

**LLM selection**: two categories were evaluated — local open-source models and commercial model APIs.

*Option 1: Local model — Qwen-3.5-7B (telecom fine-tuned)*

Pros:
- Fine-tuned specifically on telecom knowledge, giving it stronger baseline understanding of domain terminology without RAG
- Runs fully on-premise — no data leaves the internal network, which satisfies strict enterprise security requirements
- No per-token API cost

Cons:
- Requires dedicated GPU infrastructure and ongoing deployment maintenance — a significant operational burden for a data science team
- One-time release with no future updates; as the underlying technology evolves, this model falls behind with no upgrade path

*Option 2: Commercial API — Claude (selected)*

Pros:
- Updated approximately every two months — the model continuously improves without any action on our end
- Accessible via SDK with minimal integration overhead; compatible with orchestration frameworks like LangChain. Extremely capable out of the box; strong instruction-following and structured output reliability, which matters for a pipeline that expects JSON responses
- Extremely capable out of the box; strong instruction-following and structured output reliability, which matters for a pipeline that expects JSON responses
- Despite the fine-tuning advantage, when a general model is paired with a well-designed RAG layer, the local model's domain edge largely disappears

Cons:
- Data is sent externally, raising security concerns — however, commercial providers do not use API inputs for model training, and the model can be version-locked to prevent unexpected behavior changes
- Token consumption cost — but as an internal tool with limited concurrent users, the volume is low and cost is acceptable

The combination of RAG-supplied domain context and Claude's reasoning quality outperformed the fine-tuned local model on response accuracy, making the commercial API the stronger choice for this use case.


</details>

---

<details>
<summary>RAG Evaluation</summary>

Evaluating RAG quality required a multi-layer strategy, since there are no ground-truth labels for root-cause explanations:

- **Component-level evaluation with RAGAS**: each stage of the pipeline — retrieval relevance, context faithfulness, answer groundedness — was measured independently using the RAGAS framework, isolating where quality degraded.
- **Offline test dataset**: I constructed a curated set of representative queries with expected retrieval targets and answer characteristics, enabling repeatable regression testing as the knowledge base or model changed.
- **Online user feedback**: the UI included a thumbs-up / thumbs-down rating on each response to capture satisfaction signal in production. Thumbs-down cases were triaged to identify failure patterns (e.g., wrong intent classification, missing context, hallucinated metrics) and the most informative cases were added to the offline test dataset, closing the feedback loop.

</details>




## Result

Firstly, it is delivered with high engagement rate

### Measurable Explanation Accuracy

Since ground-truth labels for root causes do not exist, we evaluated the system by measuring:

**KPI-grounding accuracy**

- 80% of explanations correctly referenced degraded KPIs


### Faster Root-Cause Analysis

Before the system:
- Analysts manually reviewed 20–50 KPIs per customer
- Root-cause analysis typically took 15–30 minutes per case

After deployment:
- Analysts could identify likely causes in 1–3 minutes using the generated explanations.



