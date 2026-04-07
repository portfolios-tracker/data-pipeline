from __future__ import annotations

from typing import Any


def ensure_corporate_events_table_exists(cur: Any) -> None:
    cur.execute(
        """
        SELECT
            to_regclass('market_data.corporate_events') IS NOT NULL AS has_events
        """
    )
    (has_events,) = cur.fetchone()

    if not has_events:
        raise RuntimeError("Required table market_data.corporate_events does not exist")


def fetch_tickers_for_refresh(cur: Any, events_lookback_days: int) -> list[str]:
    cur.execute(
        """
        SELECT DISTINCT a.symbol
        FROM market_data.corporate_events ce
        JOIN market_data.assets a
            ON a.id = ce.asset_id
        LEFT JOIN (
            SELECT ticker, MAX(ingested_at) AS last_price_ingested_at
            FROM market_data.market_data_prices
            GROUP BY ticker
        ) mp
            ON mp.ticker = a.symbol
        WHERE a.symbol IS NOT NULL
          AND (
              mp.last_price_ingested_at IS NULL
              OR ce.ingested_at > mp.last_price_ingested_at
              OR (
                  ce.ingested_at IS NULL
                  AND COALESCE(ce.exright_date, ce.event_date, ce.public_date)
                      >= CURRENT_DATE - (%s * INTERVAL '1 day')
              )
          )
        """,
        (events_lookback_days,),
    )
    rows = cur.fetchall()
    return [row[0] for row in rows]
