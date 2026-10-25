"""Airflow-specific glue: DWH connection and failure alerts."""
import logging
import os
from contextlib import contextmanager

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


@contextmanager
def dwh_conn():
    """psycopg2 connection to the DWH: commits on success, rolls back on error."""
    conn = PostgresHook(postgres_conn_id="dwh").get_conn()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def notify_failure(context) -> None:
    ti = context["task_instance"]
    text = (f"Airflow: задача упала\nDAG: {ti.dag_id}\nTask: {ti.task_id}\n"
            f"Run: {context['run_id']}\nОшибка: {context.get('exception')}\nЛог: {ti.log_url}")
    log.error(text)
    token, chat_id = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text}, timeout=10)
        except requests.RequestException:
            log.exception("Failed to send Telegram alert")
