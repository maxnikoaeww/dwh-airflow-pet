"""Daily CBR exchange rates: raw -> stg -> DQ -> dds (MERGE). Marts are built by the cbr_dbt DAG."""
import os
from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task

from cbr_etl import client, dq, dwh, parsers
from cbr_etl.airflow_utils import dwh_conn, notify_failure
from cbr_etl.datasets import FCT_CURRENCY_RATE

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="cbr_currency_rates",
    schedule="@daily",
    start_date=pendulum.parse(os.environ.get("CBR_START_DATE", "2026-01-01"), tz="Europe/Moscow"),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["cbr", "dwh"],
    doc_md=__doc__,
)
def cbr_currency_rates():
    @task
    def extract(ds=None, run_id=None) -> int:
        payload = client.fetch_daily_rates(date.fromisoformat(ds))
        with dwh_conn() as conn:
            return dwh.insert_raw(conn, "cbr_daily", {"date_req": ds}, payload, run_id)

    @task
    def stage(load_id: int) -> int:
        with dwh_conn() as conn:
            rows = parsers.parse_daily_rates(dwh.read_raw(conn, load_id))
            dwh.insert_stage(conn, "stg.currency_rate", load_id, rows)
        return load_id

    @task
    def quality(load_id: int, ds=None, run_id=None) -> int:
        with dwh_conn() as conn:
            dq.run_checks(conn, "currency_rate", dq.CURRENCY_RATE_CHECKS, {"load_id": load_id, "ds": ds}, run_id)
        return load_id

    @task(outlets=[FCT_CURRENCY_RATE])
    def load_dds(load_id: int) -> None:
        with dwh_conn() as conn:
            dwh.run_sql_file(conn, "rates_stg_to_dds.sql", {"load_id": load_id})

    load_dds(quality(stage(extract())))


cbr_currency_rates()
