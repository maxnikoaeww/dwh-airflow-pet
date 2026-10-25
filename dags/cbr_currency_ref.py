"""Weekly CBR currency reference: raw -> stg -> DQ -> dds.dim_currency (SCD Type 2)."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from cbr_etl import client, dq, dwh, parsers
from cbr_etl.airflow_utils import dwh_conn, notify_failure

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="cbr_currency_ref",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cbr", "dwh"],
    doc_md=__doc__,
)
def cbr_currency_ref():
    @task
    def extract(run_id=None) -> int:
        payload = client.fetch_currency_ref()
        with dwh_conn() as conn:
            return dwh.insert_raw(conn, "cbr_valfull", {"d": 0}, payload, run_id)

    @task
    def stage(load_id: int) -> int:
        with dwh_conn() as conn:
            rows = parsers.parse_currency_ref(dwh.read_raw(conn, load_id))
            dwh.insert_stage(conn, "stg.currency_ref", load_id, rows)
        return load_id

    @task
    def quality(load_id: int, run_id=None) -> int:
        with dwh_conn() as conn:
            dq.run_checks(conn, "currency_ref", dq.CURRENCY_REF_CHECKS, {"load_id": load_id}, run_id)
        return load_id

    @task
    def load_scd2(load_id: int, data_interval_end=None) -> None:
        with dwh_conn() as conn:
            dwh.run_sql_file(conn, "currency_ref_scd2.sql",
                             {"load_id": load_id, "valid_from": data_interval_end.date()})

    load_scd2(quality(stage(extract())))


cbr_currency_ref()
