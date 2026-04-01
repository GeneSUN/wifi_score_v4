from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, date

# ============================================================
# Default Arguments
# ============================================================
default_args = {
    "owner": "ZheSun",
    "depends_on_past": False,
    "retries": 0,
}

# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="HourlyStation_WifiScore",
    default_args=default_args,
    description="Hourly Station, WiFi, and CPE Score pipeline with backfill check",
    schedule_interval="10 * * * *",  # once every hour, at HH:30
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["HourlyScore", "wifiScore", "stationScore"],
) as dag:

    # --------------------------------------------------------
    # Step 1: Generate Station Connection
    # --------------------------------------------------------
    task_station_connection = BashOperator(
        task_id="station_connection",
        bash_command='bash -c "/usr/apps/vmas/script/ZS/HourlyScore/StationConnection.sh"',
        env={
            "VAR_RUN_ID": "{{ run_id }}",
            "VAR_TRY_NUMBER": "{{ task_instance.try_number }}",
        },
    )

    # --------------------------------------------------------
    # Step 2a: Run Station Score (after StationConnection)
    # --------------------------------------------------------
    task_station_score_hourly = BashOperator(
        task_id="station_score_hourly",
        bash_command='bash -c "/usr/apps/vmas/script/ZS/HourlyScore/station_score_hourly.sh"',
        env={
            "VAR_RUN_ID": "{{ run_id }}",
            "VAR_TRY_NUMBER": "{{ task_instance.try_number }}",
        },
    )

    # --------------------------------------------------------
    # Step 2b: Run WiFi Score (after StationConnection)
    # --------------------------------------------------------
    task_wifi_score_hourly = BashOperator(
        task_id="wifi_score_hourly",
        bash_command='bash -c "/usr/apps/vmas/script/ZS/HourlyScore/wifi_score_hourly.sh"',
        env={
            "VAR_RUN_ID": "{{ run_id }}",
            "VAR_TRY_NUMBER": "{{ task_instance.try_number }}",
        },
    )

    # --------------------------------------------------------
    # Step 3: Backfill (after all above tasks finish)
    # --------------------------------------------------------
    task_backfill_hour_score = BashOperator(
        task_id="automate_backfill_hourScore",
        bash_command='bash -c "/usr/apps/vmas/script/ZS/HourlyScore/automate_backfill_hourScore.sh"',
        env={
            "VAR_RUN_ID": "{{ run_id }}",
            "VAR_TRY_NUMBER": "{{ task_instance.try_number }}",
        },
    )

    # --------------------------------------------------------
    # DAG Dependencies
    # --------------------------------------------------------
    # StationConnection → (StationScore + WiFiScore) → Backfill
    task_station_connection >> [task_station_score_hourly, task_wifi_score_hourly]
    [task_station_score_hourly, task_wifi_score_hourly] >> task_backfill_hour_score
