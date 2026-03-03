# Wi-Fi Score KPI Documentation

This document explains the methodology and key performance indicators (KPIs) used in calculating the **Wi-Fi Score**, which measures user experience based on **Coverage**, **Speed**, and **Reliability/Stability**.

<img width="2093" height="609" alt="Screenshot 2025-11-02 at 2 44 29 PM" src="https://github.com/user-attachments/assets/3cf3dbc3-0932-4465-a7ea-70e4d1f5be71" />

- Wifi Score v1: https://github.com/GeneSUN/wifiScore
- Wifi Score v2:
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/wifiScore_v4.py
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/agg_kpi.py
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/hdfs_to_s3.py
- Wifi Score Hourly
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/Hourly_Score/StationConnection.py
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/Hourly_Score/wifi_score_hourly.py
- StationScore:
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/stationScore.py
- StationScore Hourly:
  - https://github.com/GeneSUN/wifi_score_v4/blob/main/Hourly_Score/station_score_hourly.py

---

## Introduction to the WiFi Score Project
### 📄 Table of Contents

- [1. Situation & Background](#1-situation--background)    
- [2. Task Breakdown](#2-task-breakdown)  
- [3. Actions Taken](#3-actions-taken)  
- [4. Results](#4-results)  
- [5. Key Challenges](#5-key-challenges)  
- [6. Overview of Wi-Fi Score Components](#6-overview-of-wi-fi-score-components)
---

**Translate business challenges into data-driven problems and design appropriate data science solutions, including metrics and analytics,**


## 1. Situation & Background

1. Verizon operates two home internet products: Traditional fixed Wi-Fi (cable-based)/5G Home (FWA – fixed wireless access over cellular)
2. 5G Home has rapidly expanded in recent years, has faced performance challenges, including unstable network behavior, unexpected drops in speed, reliability issues.
3. To address these issues, the business aimed to continuously monitor network performance, proactively identify customers experiencing service degradation, and enable early intervention.



## 2. Task Breakdown

**The overall task is to evaluate customer network performance. is this cutomer having good experience, or is this customer  having bad experience.** <br>
> _this business question can be translated into data-driven problems, how to define good or bad, how to quantify?_

The WiFi Score framework involves three major dimensions:

**define a single “network health” score that represents the customer’s overall experience.**
- Individual KPIs are noisy and inconsistent and conflict with each other
- Different devices behave differently
- Usage patterns vary by household
- Network conditions vary by location

**Design a hierarchical scoring system**
- Metric dimension (100s of raw counters)
- Device dimension (multiple stations per home)
- Time dimension (daily → hourly → trend)

## 3. Actions Taken
This project went through four major stages of evolution.

### Stage 1 – Initial Prototype (Good Idea, Limited Scope)
- I started with a simple Wi-Fi score built on four features: Two signal-strength metrics/One data consumption metric/One stationarity metric
- When I presented it, leadership liked the concept, but they weren’t convinced. The concern was that four features were not enough to represent overall network performance.

### Stage 2 – Expanded & Production-Ready Framework

- I worked cross-functionally with other teams and expanded the score to 20+ metrics.
- We structured them into three pillars: Speed/Coverage/Reliability
- This version was tested, validated, and moved into production.

We also began collecting real customer ticket feedback to validate the signal quality. Next stage, I am trying work on these corner cases.

### Stage 3 – Granularity & Product Expansion

Next, we improved both granularity and scope:
- Expanded from household-level score → device-level score
- Expanded from daily scoring → hourly monitoring
- Extended from Wi-Fi only → 5G Home score

This made the system more precise and applicable across products.

### Stage 4 – From Monitoring Tool to Feature Store

Finally, the system evolved beyond monitoring. It became a structured feature store that supported:
- Churn prediction
- Extender recommendation
- Time-series anomaly detection
- GenAI-based explanation systems

At this stage, it was no longer just a score — it became foundational infrastructure for multiple downstream models.


### Action 1: KPI Taxonomy Design & Hierarchical Mapping 

**Built a KPI hierarchy graph**
- Raw signals → engineered indicators → pillar-level scores

To avoid double-counting correlated metrics, in addition to rely just on domain knowledge, but also statistical method:

- Performed correlation analysis
- Applied PCA on correlation, such as speed-related features

I cannot talk too much about this
- (Fragmented/Scattered)
- domain specified
- Business secrete

### Action 2: Validation Strategy Under No Ground Truth

There was no label for “bad experience.” So I designed proxy validation:
- Field engineer synthesis issues
- Correlation with support tickets,- Ticket pre/post comparison
- Precision@Top-K analysis

Example framing, This provided operational credibility.

> Among top 20 worst-scored households per day, ~70–75% were confirmed actionable cases.

### Action 3: Stability & Alert Engineering

During this phase, two major challenges:
1. Result need to be interpretable, Technician easy to understand
2. Not overload Technician capacity

Alert is defined in two dimension:
1. Historically, accumulative alert goes beyond certain threshold
2. Segment analysis, if number/percentage alert goes beyond certain threshold
> typically the threshold is defined in terms of Technician capacity and Severity level(domain)


### Action 4: Feature Engineering at Multiple Granularities

Built feature layers:

**Customer-level**
- Weighted aggregation based on device traffic share
- Heavy-hitter weighted averaging
- Outlier-resistant aggregation (trimmed mean)

**Device-level**
- Percentile PHY rate
- Reboot frequency normalized by uptime
- IP change rate per active day

**Time-level**
- 7-day rolling median


### Action 5: Daily → Hourly Architecture Upgrade
Original version was daily batch. I redesigned:
- Hourly scoring pipeline
- Device-level hourly monitoring

This reduced latency from 24h → 1h and enabled:
- Near real-time degradation detection
- Early intervention window

### Action 6: Extended Framework to 5G Home



### Action 7: Converted Rule-Based Score into Reusable Feature Store
- Churn Prediction
- Extender Recommendation
- Feature-level time series anomaly detection
- GenAI



 <img width="2093" height="609" alt="Screenshot 2025-11-02 at 2 44 29 PM" src="https://github.com/user-attachments/assets/3cf3dbc3-0932-4465-a7ea-70e4d1f5be71" />

## 4. Results

In essence, this project functions as a **telecom-specific feature store**, and **rule-based anomaly detection system** built on top.

1. Detect customers whose network performance is declining,  11 million customer, 10+ models, 100+ price plans
   - Provide targeted fixes, support, or upgraded service packages.
   - Customer Segment monitoring, customers can be segment by Demographic or Behavioral;
   - Generates interpretable scores for both engineering and customer-support teams, business teams.
2. Feature store for downstream analytics and machine learning
   - churn prediction model; extenter recommendation model; real-time anomaly detection model.
   - shared in cloud with other teams.

Difficult to quantify the impact of this project, because this is a feature store and anomaly detection system. we telling stakeholders these metrics are important, and these customer are in trouble; i provide resource so that they can take advantage of it.



## 5. Key Challenges

### 🏢 5.1 Cross-Department Alignment & Communication

This project involved multiple departments (Network Engineering, Customer Support, Data Science, and Product), and even people from different corporate entities.  
This brought both collaboration and competition, creating challenges:

| Challenge | Explanation |
|-----------|-------------|
| Partial information sharing | Teams collaborated, but carefully controlled how much detail or credit to share. |
| PM group was demanding but delayed | PM frequently delayed documentation and clarification, but still imposed tight deadlines. |
| Ownership and visibility concerns | Our team originally proposed the idea, but another group later redefined the scoring logic and gained visibility. |

**Learning:** Success wasn't just about technology — it required communication, expectation management, and visibility handling across teams.


---

### 📈 5.2 Project Scope Expanded Rapidly
The Wi-Fi Score initiative started from a simple idea and gradually grew as more use cases and requirements were added:

<img width="2093" height="609" alt="Screenshot 2025-11-02 at 2 44 29 PM" src="https://github.com/user-attachments/assets/3cf3dbc3-0932-4465-a7ea-70e4d1f5be71" />


| Expansion | Description |
|-----------|-------------|
| Product Scope | Started with Wi-Fi only → then extended to both Wi-Fi **and** 5G Home (FWA). |
| Granularity | Began with **daily** customer-level scoring → later refined to **hourly** and **device-level** monitoring. |
| Technical Depth | Started with simple rule-based scoring → then added richer feature engineering → built a feature store → finally integrated ML-based scoring and anomaly detection.|


- https://github.com/GeneSUN/wifi_score_v4/blob/main/granularization-corner%20case/The%20Power%20of%20One%20Granularity.md

- [https://github.com/GeneSUN/wifi_score_v4/tree/main/Hourly_Score](https://github.com/GeneSUN/wifi_score_v4/blob/main/Hourly_Score/Real-Time%20Wi-Fi%20Score%20Challenge.md)

**Learning:** The challenge wasn’t a one-time build, but continuously growing the system while keeping it maintainable and scalable.



---

### 🧠 5.3 Domain + Machine Learning Integration

Developing the score required blending domain expertise with data science:

- Telecom knowledge (RSRP, CQI, CPE connectivity, Wi-Fi stability, packet loss)
- Feature engineering across **network**, **device**, and **customer** levels
- Transition from thresholds → probabilistic detection → ML anomaly detection and churn models
- Designing a score that both engineers and customer-support teams could interpret

**Learning:** ML is powerful, but without domain context and interpretability, it has limited business value.


---

## 6. Overview of Wi-Fi Score Components

| Component      | Sub-Metrics                                   | Description |
|----------------|-----------------------------------------------|--------------|
| **Coverage**   | RSSI                                          | Measures signal strength and connection stability. |
| **Speed**      | Phy Rate, Airtime Utilization                 | Evaluates data throughput and network congestion. |
| **Reliability**| Reboot, Sudden Drop, SON/Client Steering, IP Changes | Captures stability and connection consistency. |

<img width="677" height="437" alt="Screenshot 2025-11-02 at 2 25 41 PM" src="https://github.com/user-attachments/assets/44f67ec3-1ead-425f-9e7c-19c0f8853026" />

