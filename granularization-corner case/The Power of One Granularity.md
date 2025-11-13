# The Power of One Granularity: How Edge Cases Transform WiFi Score Strategy

**Review of WiFi Score:**

- https://github.com/GeneSUN/wifi_score_v4/blob/main/README.md#introduction-to-the-wifi-score-project

<img width="677" height="437" alt="Screenshot 2025-11-02 at 2 25 41 PM" src="https://github.com/user-attachments/assets/44f67ec3-1ead-425f-9e7c-19c0f8853026" />


Once the scoring system was built, the next step was to **validate it against real-world customer experience**.  
We compared the score with multiple external signals — _customer support tickets_, _field technician feedback_, _device logs_, and _reports from other network teams_.  
Very quickly, we began uncovering a number of **mismatches and corner cases** where the score did not accurately reflect the customer’s lived experience.

This step is similar to building a smart health wristband:  
even if the device works perfectly in the lab and follows established medical knowledge, you still need to validate it on **real patients**, under real conditions.  

---

## “When the WiFi Score Lies — Examples of Corner Cases”?

### Corner Case #1 — The Danger of Daily Aggregation: 
Our initial WiFi Score operated at the **daily level.**
For each customer, we aggregated their entire day of performance into a single daily score, capturing long-term patterns.

#### A Perfect Day With a 5-Minute Disaster

During validation, we found customers whose daily score looked excellent: steady throughput, stable signal, and no signs of degradation.

But buried inside that “perfect” day was a single 5-minute network crash —
a complete outage that the customer absolutely noticed, even if the daily average smoothed it out.

For some customers (work-from-home, livestreaming, gaming), a 5-minute outage is catastrophic, even if the other 23 hours and 55 minutes are perfect.


#### Solution: Move From Daily Scores to Hourly Scores

To fix this, we redesigned the system to compute hourly WiFi Scores rather than daily aggregates.

- Instead of one score per day → 24 scores per day
- Instead of averaging over 24 hours → granular monitoring every hour

This shift allowed us to:
- Capture short, sudden failures that daily averages washed out
- Detect patterns (e.g., peak-hour congestion) hidden inside daily data


Hourly scores provided the right level of granularity to detect real-world failures while still being computationally manageable.

---

### Corner Case #2 — One Threshold Cannot Fit All (Segmentation vs. Global Population)

After computing the numerical features — such as LTE capacity, 5G capacity, or airtime utilization — we needed to define thresholds to classify performance as Good, Fair, or Poor.

Our first approach: **use one universal threshold across the entire customer population.**

this seemed reasonable. But during validation, we discovered a major issue.

#### The Corner Case: A Threshold That Penalizes Entire Groups

When we applied the global threshold, certain groups of customers were consistently flagged as poor, even though their performance was normal for their segment.

For example:
- Certain **device models** naturally have lower PHY rates due to hardware design.
- Some **price plans** cap peak throughput by policy.
- Rural **region** may never reach the performance of urban area.

Under a single threshold, these groups collectively failed — not because their network was unhealthy, but because the scoring system didn’t account for real-world segmentation.


#### Solution: Segment the Population

To correct this, we enhanced the algorithm by introducing segmented thresholds:

- Per device model
- Per price plan
- (Optionally) Per technology type (LTE-only vs 5G SA/NSA)

Segmented thresholds allowed the system to: Evaluate customers relative to their true **capability** envelope

Without segmentation, the score was technically correct — but operationally useless.

---

### Corner Case #3 — The Limits of Rule-Based Logic

The original WiFi Score relied heavily on rule-based thresholds, grounded in telecom knowledge.
For example, in LTE networks:
- SNR < 5 dB is widely recognized as a signal quality failure.

Rules like this are simple, interpretable, and effective. They work well for detecting known bad conditions.

However, during validation, we uncovered a major limitation.

#### The Corner Case: “Bad” vs. “Unusual”

A rule-based system can detect when a metric crosses a threshold, but it cannot detect when a metric behaves abnormally compared to its own past patterns.

Consider this example:
- A customer normally operates at SNR ≈ 100 dB.
- Suddenly, it drops to 10 dB.
- According to telecom rules, 10 dB is not a failure.
- But for this customer, the drop is catastrophic and highly unusual.

A rule-based engine would say:
- ✔️ “Still above 5 dB → not a failure.”
But in reality:
- ❌ “The customer just experienced a massive degradation.”

This is not a rare corner case — it’s extremely common and affects many customers daily.

More details of rule-based anomaly detection is dicussed in this article:
- https://medium.com/@injure21/the-data-type-driven-guide-to-anomaly-detection-in-practice-c7c7313a2118

#### Solution: Move Beyond Rules → Real-Time Novelty Detection

To address this, we expanded the scoring strategy beyond static thresholds.

We introduced real-time unsupervised novelty detection, comparing:

- The customer’s current performance vs. The customer’s historical baseline

This approach detects, Personalized anomalies that rule-based systems miss:
- Unusual deviations
- Abnormal patterns, such as Sudden drops


Unlike rule-based detection, which answers:
- “Is the metric objectively bad?”

Novelty detection answers:
- “Is the metric unusually bad for this customer?”

This shift allowed us to capture anomalies that were invisible to threshold-based logic, leading to more accurate detection and better alignment with true customer experience.

- https://github.com/GeneSUN/tech_career/tree/main/Project
- https://github.com/GeneSUN/Anomaly_Detection_toolkit

---

### Corner Case #4, Rare & Unresolvable Cases

During validation, we found customers whose experience was terrible despite having excellent network KPIs.
In many cases, the root cause was not the network, but legacy or incompatible devices, such as:

- Smartphones or laptops 10–15 years old
- Devices using outdated WiFi standards (802.11b/g)
- Hardware incapable of handling modern router features

On the flip side, some devices reported terrible KPIs even though the network was healthy — again due to old hardware, broken firmware, or measurement inaccuracies.

These cases caused **“false anomalies”** in both directions:
- Good network → bad experience
- Bad KPIs → false alarms

#### Why We Chose Not to Solve This Algorithmically

After careful analysis, we realized these cases were not practical to solve within the scoring system. There were several reasons:

1. Rare and Highly Fragmented Device Types

There are thousands of obscure or outdated device models in the field.
Many appear only a handful of times in the dataset, making it impossible to establish:
- Statistical baselines
- Segment-specific thresholds
- Reliable behavioral patterns

2. No Reliable Data to Support Modeling

- Accurate Device information
- Firmware consistency

Without trustworthy reference data, even a sophisticated model would be guessing.

3. Low Business Value

These devices represent a tiny percentage of all customers.
Even if we solved the problem:
- The operational impact would be minimal
- The engineering cost would be large
- Managing exceptions would increase long-term complexity

From a cost–benefit perspective, the return simply wasn’t worth the effort.


## Summary — Three Outcomes When You Truly Dive Deep

Through the WiFi Score project, one key insight became clear: **Diving deep into anecdotes and corner cases does not always lead to the same type of action.**
In practice, there are three distinct outcomes.

**1. Patch the Existing Algorithm**
Some corner cases reveal gaps in the current approach — but the underlying framework is still valid.
In these situations, you can apply incremental improvements, extending the logic without rewriting the entire system.

This type of corner case is like the historical development of Newton’s Laws of Motion.
Newton didn’t create one perfect law all at once. Instead, he built the framework incrementally, each law addressing a new piece of physical behavior:
Even after these three laws were established, scientists explored candidates for additional laws

Examples in WiFi Score:
- Segmented thresholds by device model or price plan
- Adding extra KPIs or adjusting boundaries

These are patchable problems.

**2. Redefine the Algorithm Entirely**

Other corner cases challenge the core assumption of the system itself.
In these cases, no amount of patching will fix the problem — the algorithm needs a new paradigm.

- This mirrors Einstein’s relativity, which didn’t patch Newton’s laws — it replaced them for a broader domain.

In WiFi Score:
- Rule-based systems fail to detect “unusual but not objectively bad” conditions
- Moving from daily → hourly scoring


This is when diving deep leads to innovation, not iteration.

**3. Accept the Limitation and Move On**

Finally, some corner cases are: Too rare/Too noisy/Too costly to solve Or have no business value

In these cases, continuing to dive deep becomes **low ROI.**

Not every irregularity is worth addressing — recognizing this is part of good judgment.

Examples in WiFi Score:
- Extremely old, incompatible devices
- Rare KPI 
- Issues that affect <0.1% of customers

Here, the right answer is:
**➡️ Document it, understand it, and do not over-engineer a solution.**


**Final Thought**

Diving deep is not just about finding problems —
- it’s about knowing which ones deserve a **patch**, which require **reinvention**, and which are simply **not worth the cost**.
- distinguishing between **iteration**, **innovation**, and **intentional omission**.
