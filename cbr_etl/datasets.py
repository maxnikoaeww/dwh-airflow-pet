"""Airflow Datasets shared between the loading DAGs and the dbt DAG."""
from airflow.datasets import Dataset

FCT_CURRENCY_RATE = Dataset("postgres://dwh/dds/fct_currency_rate")
FCT_KEY_RATE = Dataset("postgres://dwh/dds/fct_key_rate")
