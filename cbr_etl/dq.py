"""Data quality checks on a staged load. Every check SQL returns the number of failed rows."""
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    severity: str = "error"  # error stops the pipeline, warn is only recorded


CURRENCY_RATE_CHECKS = [
    Check("rows_at_least_30",
          "SELECT greatest(0, 30 - count(*)) FROM stg.currency_rate WHERE load_id = %(load_id)s"),
    Check("positive_values",
          "SELECT count(*) FROM stg.currency_rate WHERE load_id = %(load_id)s "
          "AND (value IS NULL OR value <= 0 OR vunit_rate <= 0 OR nominal <= 0)"),
    Check("unique_date_currency",
          "SELECT count(*) - count(DISTINCT (rate_date, currency_id)) FROM stg.currency_rate "
          "WHERE load_id = %(load_id)s"),
    Check("char_code_iso_format",
          "SELECT count(*) FROM stg.currency_rate WHERE load_id = %(load_id)s "
          "AND char_code !~ '^[A-Z]{3}$'"),
    # CBR sets no rates on holidays: after New Year the latest rate can be 12 days old
    Check("rate_date_near_logical_date",
          "SELECT count(*) FROM stg.currency_rate WHERE load_id = %(load_id)s "
          "AND rate_date NOT BETWEEN %(ds)s::date - 14 AND %(ds)s::date + 1"),
    Check("day_change_below_20pct",
          """SELECT count(*)
             FROM stg.currency_rate s
             CROSS JOIN LATERAL (
                 SELECT f.vunit_rate FROM dds.fct_currency_rate f
                 WHERE f.currency_id = s.currency_id AND f.rate_date < s.rate_date
                 ORDER BY f.rate_date DESC LIMIT 1
             ) p
             WHERE s.load_id = %(load_id)s AND abs(s.vunit_rate / p.vunit_rate - 1) > 0.2""",
          severity="warn"),
]

KEY_RATE_CHECKS = [
    Check("rows_not_empty",
          "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM stg.key_rate WHERE load_id = %(load_id)s"),
    Check("rate_in_range",
          "SELECT count(*) FROM stg.key_rate WHERE load_id = %(load_id)s "
          "AND (rate IS NULL OR rate <= 0 OR rate >= 100)"),
    Check("unique_date",
          "SELECT count(*) - count(DISTINCT rate_date) FROM stg.key_rate WHERE load_id = %(load_id)s"),
]

CURRENCY_REF_CHECKS = [
    Check("rows_at_least_30",
          "SELECT greatest(0, 30 - count(*)) FROM stg.currency_ref WHERE load_id = %(load_id)s"),
    Check("unique_currency_id",
          "SELECT count(*) - count(DISTINCT currency_id) FROM stg.currency_ref WHERE load_id = %(load_id)s"),
    Check("positive_nominal",
          "SELECT count(*) FROM stg.currency_ref WHERE load_id = %(load_id)s AND (nominal IS NULL OR nominal <= 0)"),
]


METAL_PRICE_CHECKS = [
    Check("rows_not_empty",
          "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM stg.metal_price WHERE load_id = %(load_id)s"),
    Check("known_metal_code",
          "SELECT count(*) FROM stg.metal_price WHERE load_id = %(load_id)s AND metal_code NOT IN (1, 2, 3, 4)"),
    Check("positive_prices",
          "SELECT count(*) FROM stg.metal_price WHERE load_id = %(load_id)s "
          "AND (buy IS NULL OR sell IS NULL OR buy <= 0 OR sell <= 0)"),
    Check("unique_date_metal",
          "SELECT count(*) - count(DISTINCT (price_date, metal_code)) FROM stg.metal_price "
          "WHERE load_id = %(load_id)s"),
]


def run_checks(conn, dataset: str, checks: list[Check], params: dict, dag_run_id: str | None) -> None:
    failed_errors = []
    with conn.cursor() as cur:
        for check in checks:
            cur.execute(check.sql, params)
            failed_rows = cur.fetchone()[0]
            passed = failed_rows == 0
            cur.execute(
                "INSERT INTO meta.dq_result (dataset, check_name, severity, failed_rows, passed, load_id, dag_run_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (dataset, check.name, check.severity, failed_rows, passed, params.get("load_id"), dag_run_id),
            )
            if passed:
                log.info("DQ %s.%s passed", dataset, check.name)
            elif check.severity == "warn":
                log.warning("DQ %s.%s warning: %s rows", dataset, check.name, failed_rows)
            else:
                log.error("DQ %s.%s failed: %s rows", dataset, check.name, failed_rows)
                failed_errors.append(f"{check.name} ({failed_rows} rows)")
    if failed_errors:
        # Results must survive the failure, so commit before raising
        conn.commit()
        raise DataQualityError(f"{dataset}: " + ", ".join(failed_errors))
