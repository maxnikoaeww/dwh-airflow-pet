"""dbt marts on top of dds, then incremental export to ClickHouse.

Runs when both facts (currency rates and key rate) have been updated: Airflow Datasets.
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from cbr_etl.airflow_utils import dwh_conn, notify_failure
from cbr_etl.datasets import FCT_CURRENCY_RATE, FCT_KEY_RATE

DBT = "cd /opt/airflow/dbt && dbt --no-use-colors"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="cbr_dbt",
    schedule=[FCT_CURRENCY_RATE, FCT_KEY_RATE],
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cbr", "dbt"],
    doc_md=__doc__,
)
def cbr_dbt():
    source_tests = BashOperator(task_id="dbt_source_tests", bash_command=f"{DBT} test --select source:*")
    # build = run models + run their tests in DAG order; a failed test stops downstream models
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build --exclude source:*")

    @task
    def export_clickhouse() -> int:
        from cbr_etl.clickhouse import export_currency_mart

        with dwh_conn() as conn:
            return export_currency_mart(conn)

    source_tests >> build >> export_clickhouse()


cbr_dbt()
