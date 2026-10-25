"""Incremental export of the dbt mart from PostgreSQL to ClickHouse by an updated_at watermark."""
import os

import clickhouse_connect

TABLE = "currency_rate_daily"
COLUMNS = ["rate_date", "currency_id", "char_code", "name", "nominal", "value", "vunit_rate",
           "prev_vunit_rate", "change_pct", "ma_7", "ma_30", "key_rate", "updated_at"]


def _client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="cbr",
    )


def export_currency_mart(pg_conn) -> int:
    ch = _client()
    # Rows rebuilt by dbt get a fresh updated_at, so only they pass the watermark
    watermark = ch.command(f"SELECT toString(max(updated_at)) FROM {TABLE}")
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(COLUMNS[:-1])}, date_trunc('second', updated_at)::timestamp "
            "FROM mart.currency_rate_daily WHERE date_trunc('second', updated_at)::timestamp > %s::timestamp",
            (watermark,),
        )
        rows = cur.fetchall()
    if rows:
        ch.insert(TABLE, rows, column_names=COLUMNS)
    return len(rows)
