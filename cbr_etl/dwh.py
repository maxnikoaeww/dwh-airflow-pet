"""Loading helpers for the PostgreSQL DWH. Functions take an open psycopg2 connection."""
import json
from pathlib import Path

from psycopg2.extras import execute_values

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "dml"


def insert_raw(conn, source: str, params: dict, payload: str, dag_run_id: str | None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw.cbr_response (source, request_params, payload, dag_run_id) "
            "VALUES (%s, %s, %s, %s) RETURNING load_id",
            (source, json.dumps(params), payload, dag_run_id),
        )
        return cur.fetchone()[0]


def read_raw(conn, load_id: int) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw.cbr_response WHERE load_id = %s", (load_id,))
        return cur.fetchone()[0]


def insert_stage(conn, table: str, load_id: int, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        # Idempotent on task retry: replace rows of this load
        cur.execute(f"DELETE FROM {table} WHERE load_id = %s", (load_id,))
        if not rows:
            return 0
        columns = list(rows[0])
        sql = f"INSERT INTO {table} (load_id, {', '.join(columns)}) VALUES %s"
        execute_values(cur, sql, [(load_id, *(r[c] for c in columns)) for r in rows])
    return len(rows)


def run_sql_file(conn, name: str, params: dict) -> int:
    with conn.cursor() as cur:
        cur.execute((SQL_DIR / name).read_text(encoding="utf-8"), params)
        return cur.rowcount
