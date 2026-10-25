"""Export of the currency mart from PostgreSQL to ClickHouse."""
import os
from datetime import date

import clickhouse_connect

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


def export_currency_mart(pg_conn, date_from: date) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(COLUMNS[:-1])}, updated_at::timestamp(0) "
            "FROM mart.currency_rate_daily WHERE rate_date >= %s",
            (date_from,),
        )
        rows = cur.fetchall()
    if rows:
        _client().insert("currency_rate_daily", rows, column_names=COLUMNS)
    return len(rows)
