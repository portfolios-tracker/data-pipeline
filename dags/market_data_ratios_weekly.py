from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from pendulum import timezone

from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)
from dags.etl_modules.orchestrators import ratios_orchestrator
from dags.etl_modules.settings import get_env, get_env_int

# CONFIG
SUPABASE_DB_URL = get_env("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = get_env_int("DB_UPSERT_BATCH_SIZE", 100)
FINANCE_PROVIDER_POOL = get_env("FINANCE_PROVIDER_POOL", "kbs_finance")
NUMERIC_SANITIZE_COLUMNS = (
    "pe_ratio",
    "pb_ratio",
    "ps_ratio",
    "p_cashflow_ratio",
    "eps",
    "bvps",
    "market_cap",
    "roe",
    "roa",
    "roic",
    "financial_leverage",
    "dividend_yield",
    "net_profit_margin",
    "debt_to_equity",
    "current_ratio",
    "quick_ratio",
    "interest_coverage",
    "asset_turnover",
    "inventory_turnover",
    "receivable_turnover",
    "revenue_growth",
    "profit_growth",
    "operating_margin",
    "gross_margin",
    "free_cash_flow",
)
RATIO_COLUMNS = (
    "asset_id",
    "fiscal_date",
    "year",
    "quarter",
    "source_provider",
    "pe_ratio",
    "pb_ratio",
    "ps_ratio",
    "p_cashflow_ratio",
    "eps",
    "bvps",
    "market_cap",
    "roe",
    "roa",
    "roic",
    "financial_leverage",
    "dividend_yield",
    "net_profit_margin",
    "debt_to_equity",
    "current_ratio",
    "quick_ratio",
    "interest_coverage",
    "asset_turnover",
    "inventory_turnover",
    "receivable_turnover",
    "revenue_growth",
    "profit_growth",
    "operating_margin",
    "gross_margin",
    "free_cash_flow",
)
RATIOS_UPSERT_SQL = """
INSERT INTO market_data.financial_ratios
    (asset_id, fiscal_date, year, quarter, source_provider,
    pe_ratio, pb_ratio,
     ps_ratio, p_cashflow_ratio, eps, bvps, market_cap, roe,
     roa, roic, financial_leverage, dividend_yield,
     net_profit_margin, debt_to_equity,
     current_ratio, quick_ratio, interest_coverage,
     asset_turnover, inventory_turnover, receivable_turnover,
     revenue_growth, profit_growth, operating_margin,
     gross_margin, free_cash_flow)
VALUES %s
ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
    year              = EXCLUDED.year,
    quarter           = EXCLUDED.quarter,
    source_provider   = EXCLUDED.source_provider,
    pe_ratio          = EXCLUDED.pe_ratio,
    pb_ratio          = EXCLUDED.pb_ratio,
    ps_ratio          = EXCLUDED.ps_ratio,
    p_cashflow_ratio  = EXCLUDED.p_cashflow_ratio,
    eps               = EXCLUDED.eps,
    bvps              = EXCLUDED.bvps,
    market_cap        = EXCLUDED.market_cap,
    roe               = EXCLUDED.roe,
    roa               = EXCLUDED.roa,
    roic              = EXCLUDED.roic,
    financial_leverage = EXCLUDED.financial_leverage,
    dividend_yield    = EXCLUDED.dividend_yield,
    net_profit_margin = EXCLUDED.net_profit_margin,
    debt_to_equity    = EXCLUDED.debt_to_equity,
    current_ratio     = EXCLUDED.current_ratio,
    quick_ratio       = EXCLUDED.quick_ratio,
    interest_coverage = EXCLUDED.interest_coverage,
    asset_turnover    = EXCLUDED.asset_turnover,
    inventory_turnover = EXCLUDED.inventory_turnover,
    receivable_turnover = EXCLUDED.receivable_turnover,
    revenue_growth    = EXCLUDED.revenue_growth,
    profit_growth     = EXCLUDED.profit_growth,
    operating_margin  = EXCLUDED.operating_margin,
    gross_margin      = EXCLUDED.gross_margin,
    free_cash_flow    = EXCLUDED.free_cash_flow,
    ingested_at       = NOW()
"""

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

local_tz = timezone("Asia/Bangkok")


with DAG(
    dag_id="market_data_ratios_weekly",
    default_args=default_args,
    schedule="0 19 * * 0",  # 7 PM Vietnam Time Sunday
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["financials", "ratios", "supabase", "weekly"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task(show_return_value_in_logs=False)
    def list_ratio_assets():
        return ratios_orchestrator.list_ratio_assets()

    @task(show_return_value_in_logs=False)
    def chunk_ratio_assets(assets):
        return ratios_orchestrator.chunk_assets(
            assets,
            chunk_size=DB_UPSERT_BATCH_SIZE,
        )

    @task
    def process_ratio_chunk(chunk_payload):
        return ratios_orchestrator.process_ratio_chunk(
            chunk_payload,
            db_url=SUPABASE_DB_URL,
            batch_size=DB_UPSERT_BATCH_SIZE,
            upsert_sql=RATIOS_UPSERT_SQL,
            ratio_columns=RATIO_COLUMNS,
            numeric_sanitize_columns=NUMERIC_SANITIZE_COLUMNS,
        )

    @task(trigger_rule="all_done")
    def finalize_ratio_load(chunk_results):
        return ratios_orchestrator.finalize_ratio_load(chunk_results)

    ratio_assets = list_ratio_assets()
    ratio_chunks = chunk_ratio_assets(ratio_assets)
    chunk_summaries = process_ratio_chunk.override(pool=FINANCE_PROVIDER_POOL).expand(
        chunk_payload=ratio_chunks
    )
    final_summary = finalize_ratio_load(chunk_summaries)

    ratio_assets >> ratio_chunks >> chunk_summaries >> final_summary
