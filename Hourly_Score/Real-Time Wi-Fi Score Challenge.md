# Organized Discussion: Real-Time Wi-Fi Score Challenge
## 1. Background

Originally, Wi-Fi Score was calculated daily.
Now the goal is to deliver hourly / near real-time Wi-Fi score so problems can be detected sooner and users can be helped faster.

<img width="1371" height="468" alt="Untitled" src="https://github.com/user-attachments/assets/1d50a058-47b0-4c74-85a0-1f24169a1622" />



**Current Hourly Score: `/sha_data/StationHistory/date=20251110/hour=16(utc, est 11)**

run at one and half hour after the initial time

| Permission | Owner | Group | Size | Last Modified | Replication | Block Size | File Name |
|------------|-------|--------|------|----------------|-------------|-------------|-----------|
| -rw-r--r-- | root | supergroup | 169.46 MB | 11/10/25 11:12 | 2 | 128 MB | 10.103.17.27-StationHistory-s2_ArcFlumeData_1762790404062.parquet |
| -rw-r--r-- | root | supergroup | 173.28 MB | 11/10/25 11:12 | 2 | 128 MB | 10.103.16.147-StationHistory-s2_ArcFlumeData_1762790404390.parquet |
| … | … | … | … | … | … | … | … |
| -rw-r--r-- | root | supergroup | 176.32 MB | 11/10/25 12:05 | 2 | 128 MB | 10.103.48.90-StationHistory-s1_ArcFlumeData_1762793463337.parquet |
| -rw-r--r-- | root | supergroup | 175.08 MB | 11/10/25 12:05 | 2 | 128 MB | 10.103.60.245-StationHistory-s2_ArcFlumeData_1762793454008.parquet |

**output:**
start run at 12:30
//sha_data//hourlyScore_include_pac/station_score_hourly/date=20251110/hour=16
//sha_data//hourlyScore_include_pac/wifi_score_hourly/date=20251110/hour=16

| Permission   | Owner | Group       | Size | Last Modified    | Replication | Block Size | Name     |
|--------------|-------|-------------|------|------------------|-------------|------------|----------|
| drwxr-xr-x   | root  | supergroup  | 0 B  | Nov 10 12:43     | 0           | 0 B        | hour=16  |


## 2. Real-Time Goal

Instead of waiting until the hour is finished and all files are flushed to HDFS,
we want to compute score while the hour is still ongoing, e.g.:

- run job every 30 minutes
- read current avaliable data

### ✅ Methodology
To enable near–real-time hourly scoring, we read the **most recent timestamp** in the StationHistory data and then look back exactly **one hour** from that point.

#### 📌 Example
If the script runs at **12:30 ET** (15:30 UTC), the latest StationHistory (/sha_data/StationHistory/) may contain events up to:

```
--- Timestamp Range (UTC) ---
Earliest ts : 1762798275000 → 2025-11-10 14:25:15
Latest ts   : 1762801875000 → 2025-11-10 15:25:15
```

## 3. Issue Discovered: The data sources do not update simultaneously.

**/sha_data/StationHistory/date=20251110/hour=16**
from xx:12 to (xx+1):05

| Permission | Owner | Group | Size | Last Modified | Replication | Block Size | File Name |
|------------|-------|--------|------|----------------|-------------|-------------|-----------|
| -rw-r--r-- | root | supergroup | 169.46 MB | 11/10/25 11:12 | 2 | 128 MB | 10.103.17.27-StationHistory-s2_ArcFlumeData_1762790404062.parquet |
| -rw-r--r-- | root | supergroup | 173.28 MB | 11/10/25 11:12 | 2 | 128 MB | 10.103.16.147-StationHistory-s2_ArcFlumeData_1762790404390.parquet |
| … | … | … | … | … | … | … | … |
| -rw-r--r-- | root | supergroup | 176.32 MB | 11/10/25 12:05 | 2 | 128 MB | 10.103.48.90-StationHistory-s1_ArcFlumeData_1762793463337.parquet |
| -rw-r--r-- | root | supergroup | 175.08 MB | 11/10/25 12:05 | 2 | 128 MB | 10.103.60.245-StationHistory-s2_ArcFlumeData_1762793454008.parquet |


**/sha_data/purple_prod/bhrx_stationhistory/date=20251110/hour=16**
from xx:05 to (xx+1):05

| Permission | Owner | Group | Size | Last Modified | Replication | Block Size | File Name |
|------------|-------|--------|------|----------------|-------------|-------------|-----------|
| -rw-r--r-- | root | supergroup | 45.99 KB | 11/10/25 11:05 | 2 | 128 MB | part-00003-f058af80-51e1-4a38-ad03-98643a163be1-c000.snappy.parquet |
| -rw-r--r-- | root | supergroup | 45.29 KB | 11/10/25 11:05 | 2 | 128 MB | part-00005-c24c148b-71f1-401c-81ab-c9111b7bbfbc-c000.snappy.parquet |
| -rw-r--r-- | root | supergroup | 44.90 KB | 11/10/25 12:02 | 2 | 128 MB | part-00015-8f9b8007-3e96-4eba-828a-0ca09b1ce337-c000.snappy.parquet |
| -rw-r--r-- | root | supergroup | 46.97 KB | 11/10/25 12:02 | 2 | 128 MB | part-00023-8f9b8007-3e96-4eba-828a-0ca09b1ce337-c000.snappy.parquet |

**/sha_data/DeviceGroups/date=20251110/hour=16**

update twice a hour
- one at xx:33 -> xx:37 
- one at (xx+1):02 -> (xx+1):06

| Permission | Owner | Group | Size | Last Modified | Replication | Block Size | File Name |
|------------|-------|--------|------|----------------|-------------|-------------|-----------|
| -rw-r--r-- | root | supergroup | 108.44 MB | 11/10/25 11:32 | 2 | 128 MB | 10.103.16.147-DeviceGroups-s1_ArcFlumeData_1762791623061.parquet |
| -rw-r--r-- | root | supergroup | 109.08 MB | 11/10/25 11:32 | 2 | 128 MB | 10.103.16.147-DeviceGroups-s2_ArcFlumeData_1762790706500.parquet |
| -rw-r--r-- | root | supergroup | 95.14 MB | 11/10/25 12:06 | 2 | 128 MB | 10.103.62.81-DeviceGroups-s2_ArcFlumeData_1762793747818.parquet |
| -rw-r--r-- | root | supergroup | 94.33 MB | 11/10/25 12:06 | 2 | 128 MB | 10.103.48.196-DeviceGroups-s1_ArcFlumeData_1762793750935.parquet |

| 14:00 | 14:01 | 14:02 | 14:03 | 14:04 | 14:05 | 14:06 | 14:07 | 14:08 | 14:09 | 14:10 | 14:11 | 14:12 | 14:13 | 14:14 | 14:15 | 14:16 | 14:17 | 14:18 | 14:19 | 14:20 | 14:21 | 14:22 | 14:23 | 14:24 | 14:25 | 14:26 | 14:27 | 14:28 | 14:29 | 14:30 |
|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|
| 7,628,427 | 9,174,753 | 9,125,586 | 9,102,833 | 9,173,856 | 7,762,801 | 9,162,935 | 9,125,564 | 9,092,885 | 9,171,361 | 7,799,834 | 9,156,656 | 8,894,235 | 9,068,355 | 9,160,625 | 7,817,023 | 9,138,236 | 9,118,195 | 9,100,438 | 9,154,194 | 7,840,142 | 9,140,982 | 9,115,062 | 9,108,717 | 9,141,575 | 7,828,181 | 9,136,754 | 9,099,483 | 9,073,506 | 8,995,178 | 2,565,845 |


this cause an issue: if i run the script at 11:45.
- stationhistory contains data from 10:40 -> 11:40.
- purple_prod/stationhistory contains data from 10:43 -> 11:43.
- DeviceGroups contains data from 10:43 -> 11:43.

## Alternative

The entire pipeline is vulnarable, difficult to maintain, since it focus on the time inside the dataframe , but dataframe update is not strict; and it would be difficult to backfill.

originally, we want 
- Read the **latest data** whenever files arrive, No need to wait for hour completion
- Update twice a hour;

However, the reality of the data ingestion pipeline makes this extremely difficult. All three inputs (StationHistory, Purple StationHistory, and DeviceGroups) write parquet files asynchronously and at different times

1. Use fixed synchronization checkpoints (ex: :10 and :40)

At :10 and :40, assume all three datasets have produced at least one batch of new data and run the scoring job.

❌ not truly real-time
❌ If any source is delayed, the whole pipeline blocks
❌ Backfilling becomes extremely complicated

2. Base the scoring window on the latest timestamps inside the dataframes

- Read StationHistory, Purple StationHistory, DeviceGroups
- Find the latest timestamp across all three
- Recompute the score using the last 1-hour window of each dataset

❌ Still not real-time — we can only run after all sources catch up
❌ Very high computation cost (full scan to detect the latest timestamp)
❌ Dataframes may still be incomplete if one source lags by minutes
❌ Backfill and debugging become even harder:
