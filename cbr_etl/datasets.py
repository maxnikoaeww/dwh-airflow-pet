"""Airflow Datasets shared between the loading DAGs and the dbt DAG."""
from airflow.datasets import Dataset

FCT_CURRENCY_RATE = Dataset("postgres://postgres:5432/dwh/dds/fct_currency_rate")
FCT_KEY_RATE = Dataset("postgres://postgres:5432/dwh/dds/fct_key_rate")
FCT_METAL_PRICE = Dataset("postgres://postgres:5432/dwh/dds/fct_metal_price")
