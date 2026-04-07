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

While the scoring system successfully quantified network performance, stakeholders struggled to interpret the results.


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

My task was to make the Wi-Fi Score system explainable and actionable so that:
- Analysts could quickly diagnose problems
- Engineers could identify root causes
- Product teams could understand customer experience drivers

Traditional dashboards and metric tables were insufficient, So I proposed building an **LLM-based explanation system using Retrieval-Augmented Generation (RAG).**
The goal was to enable users to ask questions like:
- "Why is this customer’s Wi-Fi score low?"
- "Is poor coverage causing reliability issues?"
- "Which metric should we fix first?"

RAG was ideal because it allows LLMs to **leverage domain-specific knowledge and private datasets**, improving accuracy and interpretability.


## Action:

**1) Turned raw KPI tables into LLM-friendly “customer narratives”**
- I pulled a single customer’s Wi-Fi score + sub-scores + KPI snapshot via SQL (e.g., Wi-Fi score, coverage/speed/reliability, RSSI, SNR, packet loss, latency, retransmission).
- Then I converted the structured row into a natural-language performance summary, because LLMs reason more reliably over concise textual context than raw tables.

Why this matters: this step made explanations customer-specific instead of generic metric definitions.


**2) Built a retrieval layer that fetches the right domain knowledge per question**

**1. First, I created a domain-specific knowledge base to support retrieval.**
This included two main sources:

- Telecom engineering documentation, which describes technical concepts such as RSSI, SNR, retransmissions, latency, and steering behavior.
- Custom Wi-Fi Score documentation that I designed myself, which explains:
  - How the Wi-Fi Score hierarchy is constructed
  - How each metric is defined
  - What each metric means operationally
  - How metrics relate to each other
  - How low-level KPIs influence mid-level scores like speed, coverage, and reliability
This documentation became the primary retrieval corpus for the RAG system.

**2). I then implemented a retrieval function that:**
- Converts the user question into a retrieval-focused query (rag_query)
- Searches the knowledge base
- Returns the top-k most relevant snippets
- Formats them into a structured context block:

**3) Designed Intent-Based Prompts**

**1. Different questions require different types of answers.**

For example:
- Diagnosis questions → identify root causes
- Explanation questions → explain the metrics
- Recommendation questions → suggest actions

Using one generic prompt often produced confusing or inconsistent answers, such as diagnosing problems when the user only wanted an explanation.


**2. So I designed a simple intent-based prompt framework.**

First, I classified questions into a few types:
- Diagnosis
- Explanation
- Recommendation

Then each type used a different prompt template so the model would respond in the right way.


## Result

### Measurable Explanation Accuracy

Since ground-truth labels for root causes do not exist, we evaluated the system by measuring:

**KPI-grounding accuracy**

For sampled cases:

- 90% of explanations correctly referenced degraded KPIs
- <10% contained irrelevant metrics

This ensured explanations were consistent with actual network data.

### Faster Root-Cause Analysis

Before the system:
- Analysts manually reviewed 20–50 KPIs per customer
- Root-cause analysis typically took 15–30 minutes per case

After deployment:
- Analysts could identify likely causes in 1–3 minutes using the generated explanations.

This reduced investigation time by approximately:

**~70–85% per case**

This allowed teams to analyze many more problem cases in batch investigations.




---

## Table of Contents

- [Wi-Fi score data-augmented RAG pipeline](#wi-fi-score-data-augmented-rag-pipeline)
- [Step 1 - SQL data Input](#step-1---sql-data-input)
  - [1.1 Query data by customer ID](#11-query-data-by-customer-id)
  - [1.2 Convert structured data → natural language](#12-convert-structured-data--natural-language)
- [Step 2 — Retrieve relevant KPI explanations from RAG](#step-2--retrieve-relevant-kpi-explanations-from-rag)
  - [Step 2.1 classify the user question](#step-21-classify-the-user-question)
  - [Step 2.2 generate a retrieval-focused query](#step-22-generate-a-retrieval-focused-query)
- [Step 3 — LLM prompt + data + RAG context](#step-3--llm-prompt--data--rag-context)
  - [3.1 Define a small set of prompt “modes”](#31-define-a-small-set-of-prompt-modes)
  - [3.2 Route user questions to a mode](#32-route-user-questions-to-a-mode)
  - [3.3 Parameterize your prompt](#33-parameterize-your-prompt)
- [Step 4: Putting it all together (clean pipeline)](#step-4-putting-it-all-together-clean-pipeline)

# Step 1 - SQL data Input

## 1.1. Query data by customer ID

```
    {
        "customer_id": "CUST_002",
        "wifi_score": 55,
        "coverage_score": 60,
        "speed_score": 50,
        "reliability_score": 58,
        "avg_rssi": -71,
        "avg_snr": 18,
        "packet_loss_pct": 6.8,
        "latency_ms": 95,
        "retransmission_pct": 14.2
    }
```

## 1.2 Convert structured data → natural language

LLMs reason better over textual summaries than raw tables.

```python

              Customer Wi-Fi Performance Summary:

              Overall Wi-Fi Score: 55
              Coverage Score: 60
              Speed Score: 50
              Reliability Score: 58

              Network KPIs:
              - Average RSSI: -71 dBm
              - Average SNR: 18 dB
              - Packet Loss: 6.8 %
              - Latency: 95 ms
              - Retransmission Rate: 14.2 %


```


## Step 2 — Retrieve relevant KPI explanations from RAG




```python

def retrieve_kpi_context(query: str, retriever, top_k: int = 5) -> str:
    results = retriever.retrieve(query=query, top_k=top_k)

    context = "\n\n".join(
        f"[Source {i+1}]\n{r.text}"
        for i, r in enumerate(results)
    )
    return context

```

**rag_query != user_query, derive a retrieval query from the user question**

### Step 2.1 classify the user question

Before retrieval, determine what kind of question the user is asking.

| Question type      | Example                             |
| ------------------ | ----------------------------------- |
| Diagnosis          | “Why is my Wi-Fi score low?”        |
| Metric explanation | “What does SNR mean?”               |
| Score calculation  | “How is Wi-Fi score computed?”      |
| Comparison         | “Why is speed worse than coverage?” |
| Recommendation     | “How can I improve my Wi-Fi?”       |
| Trend / anomaly    | “Why did the score drop last week?” |




### Step 2.2 generate a retrieval-focused query



## Step 3 — LLM prompt + data + RAG context


### 3.1 Define a small set of prompt “modes”

Think in intent-driven templates, not free-form prompts.

```
PROMPT_TEMPLATES = {
    "diagnosis": "...",
    "explanation": "...",
    "recommendation": "...",
    "comparison": "...",
}
```


### 3.2 Route user questions to a mode

```python
def classify_question(user_query: str) -> str:
    q = user_query.lower()
    if "why" in q or "issue" in q or "problem" in q:
        return "diagnosis"
    if "what is" in q or "explain" in q:
        return "explanation"
    if "how to improve" in q or "recommend" in q:
        return "recommendation"
    if "compare" in q or "difference" in q:
        return "comparison"
    return "diagnosis"
```

### 3.3 Parameterize your prompt

```python
def build_prompt(
    intent: str,
    user_query: str,
    customer_metrics: str,
    rag_context: str
) -> str:
    intent_config = {
        "diagnosis": {
            "role": "You are a network performance engineer.",
            "task": [
                "Diagnose the root causes",
                "Tie each issue to specific KPIs",
                "Avoid speculation",
            ],
        },
        "explanation": {
            "role": "You are a Wi-Fi expert.",
            "task": [
                "Explain concepts clearly",
                "Do NOT diagnose the customer",
                "Use simple language",
            ],
        },
        "recommendation": {
            "role": "You are a Wi-Fi optimization specialist.",
            "task": [
                "Provide actionable recommendations",
                "Map each recommendation to KPIs",
            ],
        },
    }

    if intent not in intent_config:
        raise ValueError(f"Unsupported intent: {intent}")

    cfg = intent_config[intent]
    task_block = "\n".join(f"- {t}" for t in cfg["task"])

    return f"""
                {cfg["role"]}
                
                User question:
                {user_query}
                
                Customer Metrics:
                {customer_metrics}
                
                Reference Knowledge:
                {rag_context}
                
                Task:
                {task_block}
                """.strip()


```

## Step 4: Putting it all together (clean pipeline)
```python
def answer_user_question(customer_id: str, user_query: str, retriever, llm):
    # 1) Get structured data
    data = get_customer_row(customer_id)
    customer_text = format_customer_metrics(data)

    # 2) Classify intent
    intent = classify_question(user_query)

    # 3) Retrieve reference knowledge
    rag_context = retrieve_kpi_context(rag_query, retriever)

    # 4) Build prompt dynamically
    prompt = build_prompt(
        intent=intent,
        user_query=user_query,
        customer_metrics=customer_text,
        rag_context=rag_context,
    )

    # 5) LLM inference
    return llm.generate(prompt)
```






