# The Power of One Granularity: How Edge Cases Transform WiFi Score Strategy

**Review of WiFi Score:**

- https://github.com/GeneSUN/wifi_score_v4/blob/main/README.md#introduction-to-the-wifi-score-project

<img width="677" height="437" alt="Screenshot 2025-11-02 at 2 25 41 PM" src="https://github.com/user-attachments/assets/44f67ec3-1ead-425f-9e7c-19c0f8853026" />


Once the scoring system was built, the next step was to **validate it against real-world customer experience**.  
We compared the score with multiple external signals — _customer support tickets_, _field technician feedback_, _device logs_, and _reports from other network teams_.  
Very quickly, we began uncovering a number of **mismatches and corner cases** where the score did not accurately reflect the customer’s lived experience.

This step is similar to building a smart health wristband:  
even if the device works perfectly in the lab and follows established medical knowledge, you still need to validate it on **real patients**, under real conditions.  

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


### One Threshold Cannot Fit All (Segmentation vs. Global Population)

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
