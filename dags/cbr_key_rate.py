"""Daily CBR key rate: raw -> stg -> DQ -> dds (MERGE). Marts are built by the cbr_dbt DAG."""
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from cbr_etl import client, dq, dwh, parsers
from cbr_etl.airflow_utils import dwh_conn, moscow_date, notify_failure
from cbr_etl.datasets import FCT_KEY_RATE

# Each run re-reads two weeks: covers weekends, holidays and late publication
LOOKBACK_DAYS = 14

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="cbr_key_rate",
    schedule="@daily",
    start_date=pendulum.parse(os.environ.get("CBR_START_DATE", "2026-01-01"), tz="Europe/Moscow"),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["cbr", "dwh"],
    doc_md=__doc__,
)
def cbr_key_rate():
    @task
    def extract(logical_date=None, run_id=None) -> int:
        date_to = moscow_date(logical_date)
        date_from = date_to - timedelta(days=LOOKBACK_DAYS)
        payload = client.fetch_key_rate(date_from, date_to)
        with dwh_conn() as conn:
            return dwh.insert_raw(conn, "cbr_key_rate",
                                  {"from": date_from.isoformat(), "to": date_to.isoformat()}, payload, run_id)

    @task
    def stage(load_id: int) -> int:
        with dwh_conn() as conn:
            rows = parsers.parse_key_rate(dwh.read_raw(conn, load_id))
            dwh.insert_stage(conn, "stg.key_rate", load_id, rows)
        return load_id

    @task
    def quality(load_id: int, run_id=None) -> int:
        with dwh_conn() as conn:
            dq.run_checks(conn, "key_rate", dq.KEY_RATE_CHECKS, {"load_id": load_id}, run_id)
        return load_id

    @task(outlets=[FCT_KEY_RATE])
    def load_dds(load_id: int) -> None:
        with dwh_conn() as conn:
            dwh.run_sql_file(conn, "key_rate_stg_to_dds.sql", {"load_id": load_id})

    load_dds(quality(stage(extract())))


cbr_key_rate()
