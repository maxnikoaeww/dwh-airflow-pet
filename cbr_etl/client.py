"""HTTP client for the Bank of Russia (CBR) public APIs."""
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DAILY_URL = "https://www.cbr.ru/scripts/XML_daily.asp"
VALFULL_URL = "https://www.cbr.ru/scripts/XML_valFull.asp"
SOAP_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"

KEY_RATE_ENVELOPE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <KeyRateXML xmlns="http://web.cbr.ru/">
      <fromDate>{date_from}</fromDate>
      <ToDate>{date_to}</ToDate>
    </KeyRateXML>
  </soap:Body>
</soap:Envelope>"""

TIMEOUT = 30


def _session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "POST"))
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "dwh-airflow-pet/1.0"
    return session


def fetch_daily_rates(on_date: date) -> str:
    """Official exchange rates set for on_date (XML, windows-1251)."""
    resp = _session().get(DAILY_URL, params={"date_req": on_date.strftime("%d/%m/%Y")}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content.decode("cp1251")


def fetch_currency_ref() -> str:
    """Reference of currencies with daily-set rates."""
    resp = _session().get(VALFULL_URL, params={"d": 0}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content.decode("cp1251")


def fetch_key_rate(date_from: date, date_to: date) -> str:
    """Key rate history for the period via the DailyInfo SOAP service."""
    body = KEY_RATE_ENVELOPE.format(date_from=date_from.isoformat(), date_to=date_to.isoformat())
    resp = _session().post(
        SOAP_URL,
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": '"http://web.cbr.ru/KeyRateXML"'},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.content.decode("utf-8")
