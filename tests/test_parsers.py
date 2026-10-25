from datetime import date
from decimal import Decimal
from pathlib import Path

from cbr_etl import parsers

FIXTURES = Path(__file__).parent / "fixtures"


def _read(name: str, encoding: str) -> str:
    return (FIXTURES / name).read_bytes().decode(encoding)


def test_daily_rates():
    rows = parsers.parse_daily_rates(_read("daily_2026-10-24.xml", "cp1251"))
    assert len(rows) >= 30
    assert {r["rate_date"] for r in rows} == {date(2026, 10, 24)}
    aud = next(r for r in rows if r["char_code"] == "AUD")
    assert aud == {
        "rate_date": date(2026, 10, 24),
        "currency_id": "R01010",
        "num_code": "036",
        "char_code": "AUD",
        "nominal": 1,
        "name": "Австралийский доллар",
        "value": Decimal("56.4250"),
        "vunit_rate": Decimal("56.425"),
    }
    assert len({r["currency_id"] for r in rows}) == len(rows)


def test_daily_rates_nominal_and_missing_vunit_rate():
    xml = ('<?xml version="1.0" encoding="windows-1251"?><ValCurs Date="01.02.2021">'
           '<Valute ID="R01030"><NumCode>012</NumCode><CharCode>DZD</CharCode><Nominal>100</Nominal>'
           '<Name>Алжирский динар</Name><Value>58,1200</Value></Valute></ValCurs>')
    (row,) = parsers.parse_daily_rates(xml)
    assert row["nominal"] == 100
    assert row["vunit_rate"] == Decimal("0.5812")


def test_currency_ref():
    rows = parsers.parse_currency_ref(_read("valfull.xml", "cp1251"))
    aud = next(r for r in rows if r["currency_id"] == "R01010")
    assert aud["eng_name"] == "Australian Dollar"
    assert aud["parent_code"] == "R01010"  # trailing spaces from the source are stripped
    assert aud["iso_num_code"] == 36
    assert aud["iso_char_code"] == "AUD"


def test_key_rate():
    rows = parsers.parse_key_rate(_read("keyrate.xml", "utf-8"))
    assert rows
    assert all(isinstance(r["rate_date"], date) and isinstance(r["rate"], Decimal) for r in rows)
    assert {"rate_date": date(2026, 10, 23), "rate": Decimal("14.00")} in rows
