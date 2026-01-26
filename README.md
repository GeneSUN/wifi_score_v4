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

In recent years, Verizon launched a new product—5G Home WiFi—as a fixed wireless access (FWA) solution.
However, the early customer experience consistent issue: network performance was unstable, with unexpected drops in speed, reliability, and overall quality.

To address this, the company initiated a project to continuously monitor network performance and Proactively identify customers who are having service issues..

The same scoring mechanism is also used for traditional wired WiFi products, making the system broadly applicable across access technologies.



## 2. Task Breakdown

The overall task, business challenges is to evaluate customer network performance. is this cutomer good, or is this customer bad.

this business question can be translated into data-driven problems, how to define good or bad, how to quantify?

The WiFi Score framework involves three major dimensions:

1. Multi-Aspect Evaluation (KPI Level)

The score evaluates several performance dimensions—such as speed, reliability, and coverage.
Each dimension is tracked through a specific set of KPIs or features.

2. Multi-Device Aggregation (Customer Level)
A customer’s overall score aggregates signals from multiple devices in the home environment.
Each device contributes its own set of metrics, which are then combined into a customer-level performance assessment.

3. Daily Monitoring (Time Level)

**Illustration: A Medical Analogy**

You can think of WiFi Score like evaluating a patient’s health:
- A patient (customer) has multiple systems: nervous, circulatory, respiratory.
- Each system contains multiple organs, each with its own critical indicators:
  - Heart → heartbeat
  - Cardiovascular system → blood pressure
  - Respiratory system → oxygen level

These indicators combine to form a summary health score.

## 3. Actions Taken

1. Carefully selected critical KPIs based on empirical telecom knowledge and network behavior.
2. Transformed raw telecom signals into interpretable features (e.g., combining multiple raw fields into meaningful metrics—similar to BMI combining height and weight).
   - SNR
   - Speed, include multiple different corelated, PCA
   - One lose, all lose.
3. Defined thresholds and rules to categorize performance as Good, Fair, or Poor.
4. Validation, majorly by field test

> expansion/evolve of the project

## 4. Results

In essence, this project functions as a **telecom-specific feature store**, and **rule-based anomaly detection system** built on top.

1. Detect customers whose network performance is declining
   - Provide targeted fixes, support, or upgraded service packages.
2. Customer Segment monitoring, 11 million customer, 10+ models, 100+ price plans
   - customers can be segment by Demographic or Behavioral;
   - Generates interpretable scores for both engineering and customer-support teams, business teams.
3. feature store for downstream analytics and machine learning
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

---

## ⚙️ Reliability / Stability

### 🔁 Reboot

**Raw Data Sources:**
- `OWLHistory.Diag_Result_dev_restart`
- `OWLHistory.owl_data_modem_event`

**Calculation Methodology:**
1. **Identify Restarts:** Count non-null records per home/serial number (indicating restart events).
2. **Defined Period:** Analyze a **30-day window** to capture all modem resets.
3. **Categorization:** Classify reboots by type:
   - User-initiated  
   - Automatic  
   - System-triggered  

---

### 🌐 IP Changes Categorization

**Methodology:**
1. **Defined Period:** Track IP changes over a **10-day window**.
2. **Count Frequency:** Monitor IP changes per home using `Owl_Data_fwa_cpe_data.ipv4_ip`.
3. **Performance Categories:**
   - *Poor*
   - *Fair*
   - *Good*
   - *Excellent*
4. **Reboot Consideration:** Apply different thresholds depending on whether reboots occurred.

| Category | # of IP Changes (No Reboot) | # of IP Changes (With Reboot) |
|-----------|-----------------------------|--------------------------------|
| **Poor** | >6 | >16 |
| **Fair** | 4–6 | 7–15 |
| **Good** | 1–3 | 1–6 |
| **Excellent** | 0 | 0 |

---

### ⚙️ SON / Per Client Steering

**Definition:**  
SON (*Self-Optimizing Network*) automatically manages Wi-Fi performance by dynamically steering clients between bands or access points.

- **Band Steering:** Move STA from 2.4 GHz → 5 GHz to balance range and speed.
- **AP Steering:** Move STA between router ↔ extender for optimal coverage.

**Raw Data:**  
- `Diag_Result_band_steer`  
- `Diag_Result_ap_steer`

**Methodology:**
1. Count steering records (`Diag_Result_band_steer.intend`).
2. Time window: **1 day**.
3. Filter by:
   - `STA_type == "2"`
   - Action categories and counts:

| Action | Count | Description |
|--------|-------|-------------|
| 0 | 15,041,539 | Failed |
| 1 | 40,875,296 | Succeed |
| 2 | 156,165,390 | Start |
| 3 | 47,574,699 | Timeout |
| 4 | 52,615,049 | Cancel |

---

### ⚡ Sudden Drop of Connection

Triggered when clients unexpectedly disappear from one sample to the next.

| Client Type | Poor | Fair | Good | Excellent |
|--------------|------|------|------|------------|
| **Stationary Client** | >2 | 1 | 0 | 0 |
| **Non-Stationary Client (≥2 or 3)** | >2 | 1 | 0 | 0 |

**Raw Data:**  
`Station_Data_connect_data.station_mac`

---

## 🚀 Speed

### 📶 RSSI (Signal Strength)

**Data Structure:**  
Each station has multiple records for 2.4 GHz, 5 GHz, and 6 GHz connections.

**RSSI Thresholds:**

| Band | Poor | Fair | Good | Excellent |
|------|------|------|------|------------|
| 2.4 GHz | < −78 | −71 to −77 | −56 to −70 | < −55 |
| 5 GHz | < −75 | −71 to −75 | −56 to −70 | < −55 |
| 6 GHz | < −70 | −65 to −70 | −56 to −65 | < −55 |

**Categorization Levels:**
1. **STA Record Level:** Evaluate per record.
2. **STA Connect Type Level:**  
   If >12 records below threshold in a band → STA classified under that category.
3. **STA Level:**  
   Aggregation of all connect types for final classification.
4. **Serial Number Level:**  
   Combine results per device.

---

### 📡 Phy Rate (Data Rate)

Represents the speed at which data is transmitted to the client device.

**Key Concepts:**
- **Data Filtration:** Remove control-channel phy rates before computation.
- **Thresholds:**

| Band | Poor | Fair | Good | Excellent |
|------|------|------|------|------------|
| 2.4 GHz | < 80 | 80–100 | 101–120 | 120 + |
| 5 GHz | < 200 | 201–350 | 351–500 | 500 + |
| 6 GHz | TBD | TBD | TBD | TBD |

---

### 📊 Airtime Utilization / Congestion

**Key Points:**
1. **Congestion Indicator:** Higher airtime utilization → greater channel congestion.
2. **Focus:** Currently evaluates **2.4 GHz** only.
3. **Aggregation:** Weekly aggregation of utilization values.
4. **Threshold:** Airtime ≥ 70% indicates potential congestion.

**Raw Data:**  
`Group_Diag_History_radio_wifi_info._2_4g.airtime_util`

---

## 📡 Coverage

Evaluates the **RSSI** (signal strength) and **Airtime Utilization** to assess how well a network covers an area.

---

## 🧮 Example SQL Queries

### SON On/Off
```sql
SELECT DISTINCT tplg_data_sn, tplg_data_son
FROM bhrdatabase.bhrx_devices_version_001
WHERE date = '<date>'
  AND tplg_data_son IS NOT NULL
  AND tplg_data_model_name = '<model>';
