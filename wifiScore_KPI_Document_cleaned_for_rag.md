# Wi-Fi Score Architecture

## Architecture (transcribed from diagram)

- **Wi-Fi Score** = **worst of**
  - **Coverage**
    - **RSSI**
  - **Speed**
    - **phy rate** with weight **1/2**
    - **Airtime_Utilization** with weight **1/2**
  - **Reliability**
    - **Reboot**
      - **customer_reboot** with weight **1/7**
      - **total reboot** with weight **1/7**
      - **modem_reset** with weight **1/7**
    - **Sudden Drop of Connection** with weight **1/7**
    - **SON / Per Client Steer**
      - **Steer_Start** with weight **1/7**
      - **Steer_Start_perHomeSTA** with weight **1/7**
    - **No. of IP changes** with weight **1/7**

# Reliability/Stability

## **Reboot**:

### Reboot of Device (transcribed from mind map)

- **Reboot of Device**
  - **Router reboot**
    - **user-initiated**
      - GUI
      - App
      - BTN
    - **Others**
  - **Modem reset**
    - `owl_data_modem_event`

### Reboot cause contribution

| Reboot Cause | % of contribution |
|---|---:|
| GUI | 0.29% |
| Kernel | 0.78% |
| App | 0.52% |
| BTN | 0.81% |
| Watchdog | 0.19% |
| FW | 2.37% |
| ACS | 2.40% |
| PWR | 90.99% |
| Unknown | 1.64% |

### Raw data

**OWLHistory.Diag\_Result\_dev\_restart**

```text
+--------------------------------------+-------------+------------------------------------------------------+
| rowkey                               | ts          | Diag_Result_dev_restart                              |
+--------------------------------------+-------------+------------------------------------------------------+
| 7145-ABV30117145_78670EE00AAC        | 1725498684508 | {1725498658000, Reset, BTN, ABV30117145, 3.3.0.11} |
| 5047-GRR21605047_C899B27C9280        | 1725498788215 | {1725498771000, Reset, BTN, GRR21605047, 3.1.1.17} |
| 6898-ABG24216898_FC12632E797D        | 1725498853916 | {1725498851000, Reset, BTN, ABG24216898, 3.3.0.6}  |
| 7795-AB234917795_DCF51B66C196        | 1725471583076 | {1725471582000, Reboot, GUI, E3021231118000986, 3.2.0.12} |
| 4790-ABG25124790_FC12638BB689        | 1725474697869 | {1725474709000, Reboot, APP, ABG25124790, 3.3.0.6} |
| 8530-G401119081348530_04A222ED83E8   | 1725474748272 | {1725474747000, Reset, BTN, G401119081348530, 3.2.0.15} |
+--------------------------------------+-------------+------------------------------------------------------+
```

**OWLHistory.owl\_data\_modem\_event**

```text
Modem event

+----------------------+----------------------------------------------------------+-------------+------+
| reason               | info                                                     | timestamp   | tsd  |
+----------------------+----------------------------------------------------------+-------------+------+
| EXCEPTION|[ASSERT]  | file:mcu/protocol/lte_sec/errc/assert/errc_el1_assert_mob.c line:97 | 1725367750000 | null |
| EXCEPTION|          |                                                          | 1725367755000 | null |
| EXCEPTION|          |                                                          | 1725367845000 | null |
| EXCEPTION|          |                                                          | 1725367917000 | null |
| EXCEPTION|[ASSERT]  | file:mcu/l1/el1/el1d/md95/src/tx/ltxpucch.c line:4901    | 1725367792000 | null |
+----------------------+----------------------------------------------------------+-------------+------+
```

### Reboot Calculation Methodology:

* **Identify Number of Restarts:** Count the number of non-null records for each home or serial number, as these indicate a restart event.  
* **Defined Period:** Consider a 30-day window to capture all **modem resets** for the calculation.  
* **Categorization**: Classify the reboots into **three categories** based on the type of restart (e.g., user-initiated, automatic, or system-triggered).

|  | Reboot/Reset (rolling window 30 days) | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | Number of reboots per home | 5+ | 4 | 2-3 | >=1 |
| 2 | Number of modem resets per home | 5+ | 4 | 2-3 | >=1 |
| 3 | Number of reboots initiated via customer | 3+ | 2 | 1 | 0 |

## IP Changes Categorization

### Methodology:

1. **Defined Period:** Track the number of IP changes over a specific period **(e.g., 10 days),** which is crucial for consistent performance evaluation.  
2. **Number of IP Changes:** Monitor the frequency of IP changes for each home within the defined period. (`Owl_Data_fwa_cpe_data.ipv4_ip`)

Example without IP change:

```text
+------------+---------------------+----------+--------------+--------------+---------------+--------+-------------------------+
| sn         | datetime            | model_name | ipv4_ip      | prev_ip4     | ip_changes_flag | reason | Diag_Result_dev_restart |
+------------+---------------------+----------+--------------+--------------+---------------+--------+-------------------------+
| ABU23300021| 2024-09-04 00:00:30 | XCI55AX  | 75.196.219.189 | null       | 0             | null   | null                    |
| ABU23300021| 2024-09-04 00:05:30 | XCI55AX  | 75.196.219.189 | 75.196.219.189 | 0         | null   | null                    |
| ABU23300021| 2024-09-04 00:10:31 | XCI55AX  | 75.196.219.189 | 75.196.219.189 | 0         | null   | null                    |
| ABU23300021| 2024-09-04 00:15:31 | XCI55AX  | 75.196.219.189 | 75.196.219.189 | 0         | null   | null                    |
| ABU23300021| 2024-09-04 00:20:31 | XCI55AX  | 75.196.219.189 | 75.196.219.189 | 0         | null   | null                    |
| ABU23300021| 2024-09-04 00:25:32 | XCI55AX  | 75.196.219.189 | 75.196.219.189 | 0         | null   | null                    |
+------------+---------------------+----------+--------------+--------------+---------------+--------+-------------------------+
```

Example with IP change:

```text
+------------+---------------------+-----------+---------------+---------------+----------------+--------+-------------------------+
| sn         | datetime            | model_name | ipv4_ip       | prev_ip4      | ip_changes_flag | reason | Diag_Result_dev_restart |
+------------+---------------------+-----------+---------------+---------------+----------------+--------+-------------------------+
| IACR41800396 | 2024-09-04 14:33:48 | ASK-NCM1100E | 197.176.229.239 | 197.149.125.192 | 1 | null | null |
| IACL35003153 | 2024-09-04 13:15:56 | ASK-NCM1100E | 97.129.19.196  | 97.129.222.219  | 1 | null | null |
| IACL40202099 | 2024-09-04 12:43:14 | ASK-NCM1100E | 72.107.64.52   | 72.111.191.45   | 1 | null | null |
| IACL40202099 | 2024-09-04 13:06:19 | ASK-NCM1100E | 72.111.185.150 | 72.107.64.52    | 1 | null | null |
| IACN40600315 | 2024-09-04 19:57:15 | ASK-NCM1100E | 97.213.86.6    | 75.241.62.151   | 1 | null | null |
| IACN40600315 | 2024-09-04 20:12:15 | ASK-NCM1100E | 75.244.132.10  | 97.213.86.6     | 1 | null | null |
| IACN40801799 | 2024-09-04 21:35:32 | ASK-NCM1100E | 72.111.177.110 | 75.236.242.119  | 1 | null | null |
+------------+---------------------+-----------+---------------+---------------+----------------+--------+-------------------------+
```

3. **Performance Categories:** Classify the performance as "Poor," "Fair," "Good," or "Excellent" based on the number of IP changes.  
4. **Reboot Consideration:** Use different thresholds for homes with and without reboots, as frequent IP changes without reboots can signal network issues.

|  | \# of IP changes | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | \# of IP changes without reboot | >6 | 4-6 | 1-3 | 0 |
| 2 | \# of IP change caused with reboot | >16 | 7-15 | 1-6 | 0 |

## SON/Per Client Steering:

SON (Self-Optimizing Network) is a system that automatically optimizes a network's performance through two key actions:

* **Band Steering:** The client device (STA) is moved from one frequency band to another (e.g., **2.4 GHz to 5 GHz**) based on predefined conditions. This ensures the device gets the optimal balance between range and speed, depending on its location in the house.  
* **AP Steering:** The client device is directed to switch between **network devices**, such as from a **router** to an **extender**, or vice versa, to maintain optimal connectivity.

**Diag\_Result\_band\_steer**

```text
+------------+-------------------------+-------------+----------+-----------+--------+-------------+-----------+------------+
| sn         | rowkey                  | ts          | sta_type | orig_name | action | intend_band | orig_band | target_band |
+------------+-------------------------+-------------+----------+-----------+--------+-------------+-----------+------------+
| AA113600026| 0026-AA113600026_...    | 1725374811922 | 2      | NCQ1338   | 2      | 5GI         | 2.4GI     | 5GI        |
| AA113600026| 0026-AA113600026_...    | 1725374812199 | 2      | NCQ1338   | 1      | 5GI         | 2.4GI     | 5GI        |
| AA113600026| 0026-AA113600026_...    | 1725381027813 | 2      | NCQ1338   | 2      | 2.4GI       | 5GI       | 5GI        |
| AA113600026| 0026-AA113600026_...    | 1725381157990 | 2      | NCQ1338   | 3      | 2.4GI       | 5GI       | 5GI        |
| AA113600026| 0026-AA113600026_...    | 1725397051033 | 2      | NCQ1338   | 2      | 2.4GI       | 5GI       | 5GI        |
| AA113600026| 0026-AA113600026_...    | 1725397192282 | 2      | NCQ1338   | 3      | 2.4GI       | 5GI       | 5GI        |
| AA114400059| 0059-AA114400059_...    | 1725387737696 | 2      | NCQ1338E  | 2      | 5GI         | 2.4GI     | 5GI        |
| AA114400059| 0059-AA114400059_...    | 1725387992697 | 2      | NCQ1338E  | 4      | 5GI         | 2.4GI     | 5GI        |
| AA114400059| 0059-AA114400059_...    | 1725388291610 | 2      | NCQ1338E  | 2      | 5GI         | 2.4GI     | 5GI        |
| AA114400059| 0059-AA114400059_...    | 1725388480072 | 2      | NCQ1338E  | 1      | 5GI         | 2.4GI     | 5GI        |
| AA114400059| 0059-AA114400059_...    | 1725388998492 | 2      | NCQ1338E  | 2      | 5GI         | 2.4GI     | 5GI        |
+------------+-------------------------+-------------+----------+-----------+--------+-------------+-----------+------------+
```

**Diag\_Result\_ap\_steer**

```text
+-------------+----------------------+-------------+----------+------------------+--------+-------------+-----------+------------+
| sn          | rowkey               | ts          | sta_type | orig_name        | action | intend_band | orig_band | target_band |
+-------------+----------------------+-------------+----------+------------------+--------+-------------+-----------+------------+
| G40212010076| 2576-G40212010076... | 1725336000311 | 2      | G3100            | 2      | 2.4GI       | 2.4GI     | 5GI        |
| G40111912070| 6970-G40111912070... | 1725336000488 | 2      | G3100            | 0      | 2.4GI       | 2.4GI     | 2.4GI      |
| G40212108205| 0383-G40212108205... | 1725336000650 | 2      | E3200-04A222DFD76DI | 4   | 2.4GI       | 2.4GI     | 5GI        |
| AAY14111248 | 1248-AAY14111248...  | 1725336000723 | 2      | E3200-3CBDC5E7F518I | 4   | 2.4GI       | 2.4GI     | 5GI        |
| ABP23035159 | 5159-ABP23035159...  | 1725336000770 | 2      | E3200-3CBDC5F32780I | 3   | 2.4GI       | 2.4GI     | 5GI        |
+-------------+----------------------+-------------+----------+------------------+--------+-------------+-----------+------------+
```

### Methodology:

1. **Number of Steering:** Count records of `Diag_Result_band_steer.intend`  
2. Time window, 1 day  
3. Filter  
   1. `STA_type == "2"`  
   2. Action

| action | count | meaning |
|---:|---:|---|
| 3 | 47,574,699 | timeout |
| 0 | 15,041,539 | Failed |
| 1 | 40,875,296 | Succeed |
| 4 | 52,615,049 | cancel |
| 2 | 156,165,390 | Start |

### Event field dictionary

| Items | Type | Description |
|---|---|---|
| station_mac | String | MAC address of station which was involved in this event |
| sta_type | String | Type of station: `"1"` = "LEGACY", `"2"` = "BTMi" |
| type | String | Type of device events: `"1"` = Steering black list added; `"2"` = AP steering; `"3"` = Band steering; `"4"` = Client device self AP steering; `"5"` = Client device self band steering |
| reason | String | Reason of AP/band steering events: `"1"` = RSSI_LOW (for AP Steering); `"2"` = RSSI_HIGH_TO_5G (for Band Steering); `"3"` = RSSI_LOW_TO_2G (for Band Steering); `"4"` = CH_OVERLOAD_TO_2G (for Band Steering); `"5"` = CH_OVERLOAD_TO_5G (for Band Steering) |
| action | String | Action of AP/band steering events: `"0"` = fail; `"1"` = succeed; `"2"` = start; `"3"` = timeout; `"4"` = cancel |

4.  **Categories:**

|  | SON End Client | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | Per client steer start | >60 | 31-60 | 11-29 | 1-10 |
| 2 | Per client steer failure |  |  |  |  |
| 3 | Total Steer request per Home/Total STA | >3.0 | 2.0 to 3.0 | 1.1 to 2.0 | 1 or <1 |

### Per Client Steering (transcribed from mind map)

- **Per Client Steering**
  - **Band steering**
    - filter: `STA_type == "2"`
      - `Action == "2" (Start)` -> **Per client steer start**
      - `Action == "0" (Failed)` -> **Per client steer failure**
      - `What Action` -> **Total Steer request per Home / Total STA**
  - **AP steering**

## Sudden drop of connection

In event of total number of clients disappearing from previous sample

|  | Sudden drop of connection | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | Stationary client | >2 | 1 | 0 | 0 |
| ~~2~~ | ~~Non-stationary client (minimum 2 or 3)~~ | ~~>2~~ | ~~1~~ | ~~0~~ | ~~0~~ |

`Station_Data_connect_data.station_mac`

# Coverage

## Rssi

**Data Structure:**  
Each station has multiple records over time, each with different connection types (2.4 GHz, 5 GHz, and 6 GHz), and signal strengths.

Example structure:

```text
+------------+------------------------+-------------+--------------+-----------------+-------------------+-------------------+-------------------+
| sn         | rowkey                 | ts          | connect_type | signal_strength | signal_strength_2_4GHz | signal_strength_5GHz | signal_strength_6GHz |
+------------+------------------------+-------------+--------------+-----------------+-------------------+-------------------+-------------------+
| AB240807943| 7943-AB240807943_...   | 1726113594000 | 5GI       | -62             | null              | -62               | null              |
| ABH34100231| 0231-ABH34100231_...   | 1726113599000 | 2_4GI     | -82             | -82               | null              | null              |
| GRR23102625| 2625-GRR23102625_...   | 1726113597000 | 5GI       | -81             | null              | -81               | null              |
| ABU41904452| 4452-ABU41904452_...   | 1726113596000 | 2_4GI     | -50             | -50               | null              | null              |
+------------+------------------------+-------------+--------------+-----------------+-------------------+-------------------+-------------------+
```

**Methodology:**

|  | RSSI Threshold per radio | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | 2.4 GHz | <-78 | -71 to -77 | -56 to -70 | < -55 |
| 2 | 5 GHz | <-75 | -71 to -75 | -56 to -70 | < -55 |
| 3 | 6 Ghz | <-70 | <-65 to -70 | -56 to -65 | < -55 |

1. RSSI Categorization at **STA-Record level:**

Example:

| rowkey | ts | connect_type | signal_strength | signal_strength_2_4GHz | signal_strength_5GHz | signal_strength_6GHz | category_2_4GHz | category_5GHz | category_6GHz |
|---|---:|---|---:|---:|---:|---:|---|---|---|
| 0664-ABP22120664_12C45D02F1F6 | 1.72613E+12 | 5G | -43 | null | -43 | null | No Data | Excellent | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72615E+12 | 5G | -63 | null | -63 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -62 | null | -62 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -55 | null | -55 | null | No Data | Excellent | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72620E+12 | 5G | -71 | null | -71 | null | No Data | Fair | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72613E+12 | 5G | -43 | null | -43 | null | No Data | Excellent | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72615E+12 | 5G | -61 | null | -61 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -58 | null | -58 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72618E+12 | 5G | -51 | null | -51 | null | No Data | Excellent | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72616E+12 | 5G | -67 | null | -67 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -48 | null | -48 | null | No Data | Excellent | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -81 | null | -81 | null | No Data | Poor | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 5G | -59 | null | -59 | null | No Data | Good | No Data |
| 0664-ABP22120664_12C45D02F1F6 | 1.72617E+12 | 2_4G | -62 | -62 | null | null | Good | No Data | No Data |

2. RSSI Categorization at **STA-Connect type level:**  
   If more than 12 records in a specific connection type fall below the threshold for a given category, that STA is classified under that category for that connection type.

| rowkey | poor_count_2_4GHz | poor_count_5GHz | poor_count_6GHz | fair_count_2_4GHz | fair_count_5GHz | fair_count_6GHz |
|---|---:|---:|---:|---:|---:|---:|
| 0664-ABP22120664_12C45D02F1F6 | 0 | 7 | 0 | 1 | 11 | 0 |

| good_count_2_4GHz | good_count_5GHz | good_count_6GHz |
|---:|---:|---:|
| 1 | 13 | 0 |

3. RSSI Categorization at **STA level:**

| sn | rowkey | final_category |
|---|---|---|
| ABP22120664 | 0664-ABP22120664_12C45D02F1F6 | Good |

### RSSI aggregation logic (transcribed from diagram)

- **Home Rssi**
  - weight = `log(STA 1 volume / Total volume)`
  - **Station 1 Rssi**
    - **2.4 GHz**
      - poor -> 10 records `< -71`
      - fair -> 12 records `[-78, -71]`
      - good -> 13 records `[-71, -56]`
      - excellent -> 20 records `> -56`
    - **5 GHz** -> poor
    - **6 GHz** -> good
  - **Station 2 Rssi** -> good
  - **Station 3 Rssi** -> excellent
  - **Station i Rssi**

4. RSSI Categorization at **Serial Number level:**

$$
\text{stationScore}_i
=
\text{weight}_{rssi}\times(1-\% \text{poor}_{rssi})
+
\text{weight}_{phyrate}\times(1-\% \text{poor}_{phyrate})
$$

$$
\text{wifiScore}
=
\sum_{i=1}^{n}
\text{station\_weight}_i \times \text{stationScore}_i
$$

**Note:** Weights based on Data Consumption & Mobility.

# Speed

## Airtime Utilization/Congestion

Key Points:

1. **Congestion Indicator:**  
   Higher airtime utilization values signal potential congestion in the network. The more the channel is utilized, the greater the likelihood of congestion, which can negatively impact performance.  
2. **Focus on 2.4GHz Band:**  
   Although airtime utilization is measured for different radios (frequency bands) in a router, the current version of the Wi-Fi score focuses only on the 2.4GHz band for analysis.  

   `Group_Diag_History_radio_wifi_info._2_4g.airtime_util`

```text
+------------+-------------+--------------+--------+
| sn         | ts          | airtime_util | enable |
+------------+-------------+--------------+--------+
| ABU41507485| 1726390794000 | 32.000000 | 1 |
| AAY21516729| 1726390797000 | 43.000000 | 1 |
| G401119110718891 | 1726390799000 | 4.000000  | 1 |
| G401119110801302 | 1726390801000 | 14.000000 | 1 |
| G401119110801302 | 1726390801000 | 7.000000  | 1 |
| GRR23283006 | 1726390794000 | 29.000000 | 1 |
| AA122100213 | 1726390812000 | 27.000000 | 1 |
+------------+-------------+--------------+--------+
```

3. **1-Week Aggregation:**  
   Over a 1-week period, the airtime utilization values are aggregated to assess overall network usage and identify patterns of congestion or ideal channel usage.  
4. **Threshold for Congestion:**  
   For the initial analysis, an airtime utilization value of **70%** or higher is considered not ideal, indicating potential congestion.

## Phy Rate (Data Rate) Overview:

The Phy rate or data rate is a crucial measure of the speed at which data is being delivered to each client in a WiFi network. It reflects the performance and quality of the connection a device (e.g., an iPhone or laptop) experiences when connected to either the 2.4GHz or 5GHz radio bands.

Key Concepts:

* Data Filtration:  
  Before calculating the score, certain data, such as **control channel** phy rates, needs to be filtered out as per current WiFi score logic.   
* Phy Rate Thresholds:

|  | Phy Rate | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | 2.4Ghz Radio | <80 | 100-80 | 101-120 | 120+ |
| 2 | 5Ghz Radio | <200 | 201-350 | 351-500 | 500+ |
| 3 | 6Ghz Radio |  |  |  |  |

# Aggregation:

## Architecture (transcribed from diagram)

- **Wi-Fi Score** = **worst of**
  - **Coverage**
    - **RSSI**
  - **Speed**
    - **phy rate** with weight **1/2**
    - **Airtime_Utilization** with weight **1/2**
  - **Reliability**
    - **Reboot**
      - **customer_reboot** with weight **1/7**
      - **total reboot** with weight **1/7**
      - **modem_reset** with weight **1/7**
    - **Sudden Drop of Connection** with weight **1/7**
    - **SON / Per Client Steer**
      - **Steer_Start** with weight **1/7**
      - **Steer_Start_perHomeSTA** with weight **1/7**
    - **No. of IP changes** with weight **1/7**

### **How aggregate multiple KPI into bucket score( such as speed, Coverage, and reliability), and then wifi score? Especially these KPI are defined using categorical value instead of numerical value**

**Use (reboot) \-\> reliability \-\> wifi score, as an example**

### Reboot -> Reliability -> Wi-Fi Score (transcribed from diagram)

- **Wi-Fi Score** = worst of:
  - Speed
  - Reliability
    - Reboot
      - customer_reboot (`1/7`)
      - total reboot (`1/7`)
      - modem_reset (`1/7`)
    - Sudden Drop of Connection (`1/7`)

1. Number of reboot is calculated, this is a numerical value, eg. 1, 2, 3…  
2. Numerical value of reboot is converted to categorical value based on domain-related threshold, as shown in table below. For example, if number of reboot is 3, then it is categorized as “Good”.

|  | Reboot/Reset (rolling window 30 days) | Poor | Fair | Good | Excellent |
|---|---|---|---|---|---|
| 1 | Number of reboots per home | 5+ | 4 | 2-3 | >=1 |
| 2 | Number of modem resets per home | 5+ | 4 | 2-3 | >=1 |
| 3 | Number of reboots initiated via customer | 3+ | 2 | 1 | 0 |

3. Following the same methodology as above, we got category values of other KPI, such as “good” `customer_reboot`, “fair” `modem_reset`, “poor” sudden drop of connection, etc.  
4. After getting all category values of different KPI, we need to aggregate to Reliability. **How can we aggregate categorical value together?**  
   1. Firstly convert the categorical value to numerical value based on table below.

| Original Value (`col_name`) | Mapped Numeric Value (`col_name_numeric`) |
|---|---:|
| Poor | 1 |
| Fair | 2 |
| Good | 3 |
| Excellent | 4 |
| Any other value (or `NULL`) | `NULL` |

   2. Aggregate the numerical value (1, 2, 3, 4) of each KPI into `Reliability_numerical`.  
   3. Convert `Reliability_numerical` into `Reliability_category`, based on table below.

| Numeric Value (`col_name`) | Mapped Categorical Value |
|---|---|
| < 1.5 | Poor |
| >= 1.5 and < 2.5 | Fair |
| >= 2.5 and < 3.5 | Good |
| >= 3.5 | Excellent |
| Any other value (or `NULL`) | `NULL` |

5. After we have categorical values of coverage, speed and reliability, we select the worst of these three as Wi-Fi score.  
   For example, if a home has `coverage = good`, `speed = fair`, and `reliability = poor`, the final Wi-Fi score is **poor**.

# Question:

## Son\_on\_off

```sql
SELECT distinct tplg_data_sn, tplg_data_son
FROM "bhrdatabase"."bhrx_devices_version_001"
where date = ' '
  and tplg_data_son is not null
  and tplg_data_model_name = ' '
```

## Ethernet not 1 Gig

```sql
SELECT
    sum(case when TROUBLE_TYPE_ALPHA_CD = 'INTR' then 1 else 0 end) as INTR,
    sum(case when TROUBLE_TYPE_ALPHA_CD = 'PCCR' then 1 else 0 end) as PCCR,
    sum(case when TROUBLE_TYPE_ALPHA_CD = 'SLOW' then 1 else 0 end) as SLOW,
    sum(case when TROUBLE_TYPE_ALPHA_CD = 'WIFI' then 1 else 0 end) as WIFI,
    sum(case when TROUBLE_TYPE_ALPHA_CD = 'CCON' then 1 else 0 end) as CCON,
    sum(case when DISPATCH_OUT_IND = '1.0' then 1 else 0 end) as DISPATCH_OUT_IND,
    sum(case when DROP_SHIP_IND = '1.0' then 1 else 0 end) as DROP_SHIP_IND
FROM "ticketdatabase"."bhr_ticketdata"
where receive_dt between '2024-06-01' and '2024-07-15'
```

Stationary:

This algorithm detects stationary station by comparing distribution percentiles (like the 90th and 50th percentiles) of `signal_strength`.

For each station:

1. Compute the difference between the 90th and 50th percentiles  
   (`diff = 90% - 50%`) of entire-day `signal_strength`.  
2. If this difference is small (`<= 5`), the station is considered stationary (low spread = little change).
