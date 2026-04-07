"""Direct VCI data access helpers used by the data-pipeline DAGs.

This module replaces the vnstock runtime dependency for the evening batch
by calling the Vietcap endpoints directly and shaping the payloads into the
same tabular contracts the loaders already expect.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://trading.vietcap.com.vn/data-mt/graphql"
TRADING_URL = "https://trading.vietcap.com.vn/api/"
OHLC_PATH = "chart/OHLCChart/gap-chart"
NEWS_LANG = "vi"

DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

_FALLBACK_VN_TICKERS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]


def _camel_to_snake(name: str) -> str:
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    value = re.sub("([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.replace("__", "_").lower()


def _request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers=DEFAULT_HEADERS, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(
            f"HTTP {exc.code} from {url}: {exc.read().decode('utf-8', 'ignore')}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to reach {url}: {exc}") from exc


def _graphql(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    response = _request_json(
        "POST",
        GRAPHQL_URL,
        payload={"query": query, "variables": variables or {}},
    )
    if isinstance(response, dict) and response.get("errors"):
        raise RuntimeError(f"VCI GraphQL error: {response['errors']}")
    return response.get("data", {}) if isinstance(response, dict) else {}


def _company_type_code(symbol: str) -> str:
    query = """
    query Query($ticker: String!) {
      CompanyListingInfo(ticker: $ticker) {
        icbName4
      }
    }
    """
    data = _graphql(query, {"ticker": symbol})
    listing_info = data.get("CompanyListingInfo") or {}
    icb_name = str(listing_info.get("icbName4") or "").strip().lower()

    if "ngân hàng" in icb_name:
        return "NH"
    if "môi giới chứng khoán" in icb_name or "chứng khoán" in icb_name:
        return "CK"
    if "bảo hiểm" in icb_name:
        return "BH"
    return "CT"


def _financial_ratio_metadata() -> pd.DataFrame:
    query = """
    query Query {
      ListFinancialRatio {
        id
        type
        name
        unit
        isDefault
        fieldName
        en_Type
        en_Name
        tagName
        comTypeCode
        order
      }
    }
    """
    data = _graphql(query)
    rows = data.get("ListFinancialRatio") or []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame.columns = [_camel_to_snake(column) for column in frame.columns]
    return frame


def _build_financial_frame(
    symbol: str,
    period: str,
    *,
    report_types: set[str] | None = None,
) -> pd.DataFrame:
    metadata = _financial_ratio_metadata()
    if metadata.empty:
        return pd.DataFrame()

    if report_types:
        normalized_types = {
            str(report_type).strip().lower()
            for report_type in report_types
            if str(report_type).strip()
        }
        if "type" not in metadata.columns:
            return pd.DataFrame()
        metadata = metadata[
            metadata["type"].astype(str).str.strip().str.lower().isin(normalized_types)
        ].copy()
        if metadata.empty:
            return pd.DataFrame()

    company_type = _company_type_code(symbol)
    if company_type != "CT":
        metadata = metadata[metadata["com_type_code"].isin(["CT", company_type])].copy()
    else:
        metadata = metadata[metadata["com_type_code"] == "CT"].copy()

    metadata = metadata.drop_duplicates(subset=["field_name"], keep="first")
    field_names = [str(field) for field in metadata["field_name"].tolist() if field]
    if not field_names:
        return pd.DataFrame()

    selection = "\n          ".join(
        ["ticker", "yearReport", "lengthReport", "updateDate", *field_names]
    )
    query = f"""
    query Query($ticker: String!, $period: String!) {{
      CompanyFinancialRatio(ticker: $ticker, period: $period) {{
        ratio {{
          {selection}
        }}
        period
      }}
    }}
    """
    data = _graphql(query, {"ticker": symbol, "period": period})
    ratio_rows = ((data.get("CompanyFinancialRatio") or {}).get("ratio")) or []
    frame = pd.DataFrame(ratio_rows)
    if frame.empty:
        return frame

    translation = metadata.set_index("field_name")[["en_name", "name"]].fillna("")
    rename_map: dict[str, str] = {}
    for column in frame.columns:
        if column in translation.index:
            label = (
                translation.loc[column, "en_name"]
                or translation.loc[column, "name"]
                or column
            )
            rename_map[column] = str(label)
    frame = frame.rename(columns=rename_map)

    ordered_columns: list[str] = [
        column
        for column in ["ticker", "yearReport", "lengthReport", "updateDate"]
        if column in frame.columns
    ]
    ordered_columns.extend(
        [
            str(rename_map.get(field_name, field_name))
            for field_name in metadata.sort_values(["order", "field_name"])[
                "field_name"
            ].tolist()
            if str(rename_map.get(field_name, field_name)) in frame.columns
        ]
    )
    ordered_columns = list(dict.fromkeys(ordered_columns))
    return frame[ordered_columns]


def _find_columns(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    column_list = list(columns)
    normalized = [str(column).lower() for column in column_list]
    for candidate in candidates:
        candidate_lower = candidate.lower()
        for index, column in enumerate(normalized):
            if candidate_lower in column:
                return column_list[index]
    return None


def _extract_source_host(url: object) -> str | None:
    if not isinstance(url, str) or not url.strip():
        return None
    host = urlparse(url.strip()).netloc
    return host or None


def list_active_vn_stock_tickers(
    db_url: str | None, *, raise_on_fallback: bool = False
) -> list[dict[str, str]]:
    if not db_url:
        if raise_on_fallback:
            raise RuntimeError("SUPABASE_DB_URL is not set")
        return [
            {"symbol": ticker, "asset_id": "fallback"}
            for ticker in _FALLBACK_VN_TICKERS
        ]

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, symbol, metadata
                    FROM market_data.assets
                    WHERE asset_class = 'STOCK'
                      AND market = 'VN'
                      AND status = 'active'
                    ORDER BY symbol
                    """
                )
                rows = cursor.fetchall()
    except Exception as exc:
        if raise_on_fallback:
            raise RuntimeError(f"Could not query market_data.assets: {exc}") from exc
        logger.warning("Falling back to seed VN tickers after query failure: %s", exc)
        return [
            {"symbol": ticker, "asset_id": "fallback"}
            for ticker in _FALLBACK_VN_TICKERS
        ]

    tickers: list[dict[str, str]] = []
    seen_symbols: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol or symbol in seen_symbols:
            continue

        metadata = row.get("metadata") or {}
        symbol_type = str(metadata.get("symbol_type") or "").strip().upper()
        if symbol_type and symbol_type != "STOCK":
            continue

        tickers.append({"symbol": symbol, "asset_id": row.get("id") or "fallback"})
        seen_symbols.add(symbol)

    if tickers:
        return tickers

    if raise_on_fallback:
        raise RuntimeError("market_data.assets returned zero active VN stock tickers")
    return [
        {"symbol": ticker, "asset_id": "fallback"} for ticker in _FALLBACK_VN_TICKERS
    ]


def fetch_stock_price(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    end_stamp = int(
        (end_dt + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp()
    )
    business_days = pd.bdate_range(start=start_date, end=end_date)
    count_back = int(len(business_days) + 1)

    payload = {
        "timeFrame": "ONE_DAY",
        "symbols": [symbol],
        "to": end_stamp,
        "countBack": count_back,
    }
    data = _request_json("POST", f"{TRADING_URL}{OHLC_PATH}", payload=payload)
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    if isinstance(data, list) and data:
        first_item = data[0]
        if isinstance(first_item, dict) and isinstance(first_item.get("o"), list):
            data = pd.DataFrame(
                {
                    "t": first_item.get("t"),
                    "o": first_item.get("o"),
                    "h": first_item.get("h"),
                    "l": first_item.get("l"),
                    "c": first_item.get("c"),
                    "v": first_item.get("v"),
                }
            ).to_dict("records")

    frame = pd.DataFrame(data or [])
    if frame.empty:
        return frame

    rename_map = {
        "t": "trading_date",
        "time": "trading_date",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    }
    frame = frame.rename(
        columns={
            column: rename_map[column]
            for column in frame.columns
            if column in rename_map
        }
    )

    required_cols = ["trading_date", "open", "high", "low", "close", "volume"]
    frame = frame[[column for column in required_cols if column in frame.columns]]
    if "trading_date" in frame.columns:
        if pd.api.types.is_numeric_dtype(frame["trading_date"]):
            frame["trading_date"] = pd.to_datetime(
                frame["trading_date"], unit="s", errors="coerce"
            )
        else:
            frame["trading_date"] = pd.to_datetime(
                frame["trading_date"], errors="coerce"
            )
        frame["trading_date"] = frame["trading_date"].dt.date

    frame["ticker"] = symbol
    frame["source"] = "vci"
    for column in ["open", "high", "low", "close"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    if "volume" in frame.columns:
        frame["volume"] = (
            pd.to_numeric(frame["volume"], errors="coerce").fillna(0).astype(int)
        )
    return frame


def fetch_financial_ratios(symbol: str, period: str = "Q") -> pd.DataFrame:
    return _build_financial_frame(symbol, period)


def fetch_income_statement(symbol: str, period: str = "Q") -> pd.DataFrame:
    return _build_financial_frame(
        symbol,
        period,
        report_types={"Chỉ tiêu kết quả kinh doanh", "income statement"},
    )


def fetch_balance_sheet(symbol: str, period: str = "Q") -> pd.DataFrame:
    return _build_financial_frame(
        symbol,
        period,
        report_types={"Chỉ tiêu cân đối kế toán", "balance sheet"},
    )


def fetch_company_events(symbol: str) -> pd.DataFrame:
    query = """
    query Query($ticker: String!, $lang: String!) {
      OrganizationEvents(ticker: $ticker) {
        id
        organCode
        ticker
        eventTitle
        en_EventTitle
        publicDate
        issueDate
        sourceUrl
        eventListCode
        ratio
        value
        recordDate
        exrightDate
        eventListName
        en_EventListName
      }
    }
    """
    data = _graphql(query, {"ticker": symbol, "lang": NEWS_LANG})
    rows = data.get("OrganizationEvents") or []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame = frame.rename(
        columns={
            "id": "event_id",
            "eventTitle": "event_title",
            "en_EventTitle": "event_title_en",
            "publicDate": "public_date",
            "issueDate": "event_date",
            "sourceUrl": "source_url",
            "eventListCode": "event_type",
            "eventListName": "event_description",
            "en_EventListName": "event_description_en",
            "recordDate": "record_date",
            "exrightDate": "exright_date",
        }
    )

    if "event_date" not in frame.columns:
        frame["event_date"] = frame.get("record_date")

    for column in ["event_date", "public_date", "exright_date"]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date

    if "event_id" in frame.columns:
        frame["event_id"] = frame["event_id"].astype(str)
    else:
        frame["event_id"] = None
    if "event_description_en" in frame.columns:
        frame["event_description"] = frame["event_description_en"].fillna(
            frame.get("event_description")
        )
    if "event_type" in frame.columns:
        frame["event_type"] = frame["event_type"].fillna(frame.get("organCode"))
    else:
        frame["event_type"] = frame.get("organCode")
    required_cols = [
        "event_id",
        "event_date",
        "public_date",
        "exright_date",
        "event_title",
        "event_type",
        "event_description",
    ]
    for column in required_cols:
        if column not in frame.columns:
            frame[column] = None
    return frame[required_cols]


def fetch_company_news(symbol: str) -> pd.DataFrame:
    query = """
    query Query($ticker: String!, $lang: String!) {
      News(ticker: $ticker, langCode: $lang) {
        id
        organCode
        ticker
        newsTitle
        newsSourceLink
        createdAt
        publicDate
        updatedAt
        newsId
        closePrice
        referencePrice
        floorPrice
        ceilingPrice
        percentPriceChange
      }
    }
    """
    data = _graphql(query, {"ticker": symbol, "lang": NEWS_LANG})
    rows = data.get("News") or []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame = frame.rename(
        columns={
            "id": "news_id",
            "newsTitle": "title",
            "newsSourceLink": "news_source_link",
            "publicDate": "publish_date",
            "closePrice": "price_at_publish",
            "referencePrice": "reference_price",
            "percentPriceChange": "price_change_ratio",
        }
    )
    frame["ticker"] = symbol
    if "news_source_link" in frame.columns:
        frame["source"] = frame["news_source_link"].apply(_extract_source_host)
    else:
        frame["source"] = None
    if "price_at_publish" in frame.columns and "reference_price" in frame.columns:
        frame["price_change"] = pd.to_numeric(
            frame["price_at_publish"], errors="coerce"
        ) - pd.to_numeric(frame["reference_price"], errors="coerce")
    frame["publish_date"] = pd.to_datetime(frame["publish_date"], errors="coerce")
    if "price_change_ratio" in frame.columns:
        frame["price_change_ratio"] = pd.to_numeric(
            frame["price_change_ratio"], errors="coerce"
        )
    for column in [
        "price_at_publish",
        "price_change",
        "price_change_ratio",
        "rsi",
        "rs",
    ]:
        if column not in frame.columns:
            frame[column] = 0.0
    required_cols = [
        "ticker",
        "publish_date",
        "title",
        "source",
        "price_at_publish",
        "price_change",
        "price_change_ratio",
        "rsi",
        "rs",
        "news_id",
    ]
    return frame[required_cols]


def fetch_company_overview(symbol: str) -> pd.DataFrame:
    query = """
    query Query($ticker: String!) {
      CompanyListingInfo(ticker: $ticker) {
        ticker
        companyProfile
        en_CompanyProfile
        icbName2
        enIcbName2
        icbName3
        enIcbName3
        icbName4
        enIcbName4
        __typename
      }
    }
    """
    data = _graphql(query, {"ticker": symbol})
    listing_info = data.get("CompanyListingInfo") or {}
    if not listing_info:
        return pd.DataFrame()

    frame = pd.DataFrame([listing_info])
    frame = frame.rename(
        columns={
            "ticker": "symbol",
            "companyProfile": "company_profile",
            "en_CompanyProfile": "company_profile_en",
            "icbName2": "icb_name2",
            "enIcbName2": "icb_name2_en",
            "icbName3": "icb_name3",
            "enIcbName3": "icb_name3_en",
            "icbName4": "icb_name4",
            "enIcbName4": "icb_name4_en",
        }
    )
    if "symbol" not in frame.columns:
        frame["symbol"] = symbol
    return frame


def fetch_vn_listing_symbols() -> pd.DataFrame:
    url = "https://trading.vietcap.com.vn/api/price/symbols/getAll"
    data = _request_json("GET", url)
    frame = pd.DataFrame(data or [])
    if frame.empty:
        return frame

    frame.columns = [_camel_to_snake(column) for column in frame.columns]
    frame = frame.rename(columns={"board": "exchange"})
    return frame


def fetch_vn_industry_metadata() -> pd.DataFrame:
    query = """
    query Query {
      CompaniesListingInfo {
        ticker
        organName
        enOrganName
        icbName3
        enIcbName3
        icbName2
        enIcbName2
        icbName4
        enIcbName4
        comTypeCode
        icbCode1
        icbCode2
        icbCode3
        icbCode4
      }
    }
    """
    data = _graphql(query)
    rows = data.get("CompaniesListingInfo") or []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame.columns = [_camel_to_snake(column) for column in frame.columns]
    frame = frame.rename(columns={"ticker": "symbol"})
    if "icb_code" not in frame.columns:
        icb_code_candidates = [
            column
            for column in ["icb_code4", "icb_code3", "icb_code2", "icb_code1"]
            if column in frame.columns
        ]
        if icb_code_candidates:
            frame["icb_code"] = frame[icb_code_candidates].bfill(axis=1).iloc[:, 0]
        else:
            frame["icb_code"] = None
    return frame
