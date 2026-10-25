FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt lives in its own virtualenv: its dependencies conflict with Airflow constraints
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0
ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"
