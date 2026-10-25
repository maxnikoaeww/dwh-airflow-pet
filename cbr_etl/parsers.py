"""Pure parsers of CBR XML responses into typed rows (no I/O, covered by tests)."""
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from decimal import Decimal

_XML_DECL = re.compile(r"^\s*<\?xml[^>]*\?>")


def _root(xml: str) -> ET.Element:
    # The declaration names windows-1251, but the text is already decoded
    return ET.fromstring(_XML_DECL.sub("", xml, count=1))


def _decimal(text: str | None) -> Decimal | None:
    if text is None or not text.strip():
        return None
    return Decimal(text.strip().replace(",", "."))


def _text(el: ET.Element, tag: str) -> str | None:
    value = el.findtext(tag)
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_daily_rates(xml: str) -> list[dict]:
    root = _root(xml)
    rate_date = datetime.strptime(root.attrib["Date"], "%d.%m.%Y").date()
    rows = []
    for v in root.iter("Valute"):
        nominal = int(_text(v, "Nominal"))
        value = _decimal(_text(v, "Value"))
        # VunitRate is absent in old responses: derive it from the nominal
        vunit_rate = _decimal(_text(v, "VunitRate")) or value / nominal
        rows.append({
            "rate_date": rate_date,
            "currency_id": v.attrib["ID"],
            "num_code": _text(v, "NumCode"),
            "char_code": _text(v, "CharCode"),
            "nominal": nominal,
            "name": _text(v, "Name"),
            "value": value,
            "vunit_rate": vunit_rate,
        })
    return rows


def parse_currency_ref(xml: str) -> list[dict]:
    rows = []
    for item in _root(xml).iter("Item"):
        iso_num = _text(item, "ISO_Num_Code")
        rows.append({
            "currency_id": item.attrib["ID"].strip(),
            "name": _text(item, "Name"),
            "eng_name": _text(item, "EngName"),
            "nominal": int(_text(item, "Nominal")),
            "parent_code": _text(item, "ParentCode"),
            "iso_num_code": int(iso_num) if iso_num else None,
            "iso_char_code": _text(item, "ISO_Char_Code"),
        })
    return rows


def parse_key_rate(xml: str) -> list[dict]:
    # <KR> elements live in the empty namespace inside the SOAP envelope
    return [
        {"rate_date": date.fromisoformat(_text(kr, "DT")[:10]), "rate": _decimal(_text(kr, "Rate"))}
        for kr in _root(xml).iter("KR")
    ]
