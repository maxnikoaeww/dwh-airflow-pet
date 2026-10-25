"""Daily CBR precious metal prices: raw -> stg -> DQ -> dds (MERGE). Marts are built by the cbr_dbt DAG."""
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from cbr_etl import client, dq, dwh, parsers
from cbr_etl.airflow_utils import dwh_conn, moscow_date, notify_failure
from cbr_etl.datasets import FCT_METAL_PRICE

# Each run re-reads a week: covers weekends and holidays when prices are not set
LOOKBACK_DAYS = 7

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="cbr_metal_prices",
    schedule="@daily",
    start_date=pendulum.parse(os.environ.get("CBR_START_DATE", "2026-01-01"), tz="Europe/Moscow"),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["cbr", "dwh"],
    doc_md=__doc__,
)
def cbr_metal_prices():
    @task
    def extract(logical_date=None, run_id=None) -> int:
        date_to = moscow_date(logical_date)
        date_from = date_to - timedelta(days=LOOKBACK_DAYS)
        payload = client.fetch_metal_prices(date_from, date_to)
        with dwh_conn() as conn:
            return dwh.insert_raw(conn, "cbr_metal",
                                  {"from": date_from.isoformat(), "to": date_to.isoformat()}, payload, run_id)

    @task
    def stage(load_id: int) -> int:
        with dwh_conn() as conn:
            rows = parsers.parse_metal_prices(dwh.read_raw(conn, load_id))
            dwh.insert_stage(conn, "stg.metal_price", load_id, rows)
        return load_id

    @task
    def quality(load_id: int, run_id=None) -> int:
        with dwh_conn() as conn:
            dq.run_checks(conn, "metal_price", dq.METAL_PRICE_CHECKS, {"load_id": load_id}, run_id)
        return load_id

    @task(outlets=[FCT_METAL_PRICE])
    def load_dds(load_id: int) -> None:
        with dwh_conn() as conn:
            dwh.run_sql_file(conn, "metal_stg_to_dds.sql", {"load_id": load_id})

    load_dds(quality(stage(extract())))


cbr_metal_prices()
