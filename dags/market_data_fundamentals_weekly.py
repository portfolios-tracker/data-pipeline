from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import TaskGroup, task
from pendulum import timezone

from dags.etl_modules.adapters.market_data_repository import MarketDataRepository
from dags.etl_modules.fetcher import FetcherFundamentalsProvider
from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)
from dags.etl_modules.orchestrators import fundamentals_orchestrator
from dags.etl_modules.settings import get_env, get_env_int

SUPABASE_DB_URL = get_env("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = get_env_int("DB_UPSERT_BATCH_SIZE", 100)
FINANCE_PROVIDER_POOL = get_env("FINANCE_PROVIDER_POOL", "kbs_finance")

INCOME_STATEMENT_COLUMNS = (
    "asset_id",
    "fiscal_date",
    "year",
    "quarter",
    "source_provider",
    "revenue",
    "cost_of_goods_sold",
    "gross_profit",
    "operating_profit",
    "net_profit_post_tax",
    "selling_expenses",
    "admin_expenses",
    "financial_income",
    "financial_expenses",
    "other_income",
    "other_expenses",
    "ebitda",
)
BALANCE_SHEET_COLUMNS = (
    "asset_id",
    "fiscal_date",
    "year",
    "quarter",
    "source_provider",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "cash_and_equivalents",
    "short_term_assets",
    "long_term_assets",
    "short_term_liabilities",
    "long_term_liabilities",
)
INCOME_STATEMENTS_UPSERT_SQL = """
INSERT INTO market_data.income_statements
    (asset_id, fiscal_date, year, quarter, source_provider,
     revenue,
     cost_of_goods_sold, gross_profit, operating_profit,
     net_profit_post_tax, selling_expenses, admin_expenses,
     financial_income, financial_expenses, other_income,
     other_expenses, ebitda)
VALUES %s
ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
    year                = EXCLUDED.year,
    quarter             = EXCLUDED.quarter,
    source_provider     = EXCLUDED.source_provider,
    revenue             = EXCLUDED.revenue,
    cost_of_goods_sold  = EXCLUDED.cost_of_goods_sold,
    gross_profit        = EXCLUDED.gross_profit,
    operating_profit    = EXCLUDED.operating_profit,
    net_profit_post_tax = EXCLUDED.net_profit_post_tax,
    selling_expenses    = EXCLUDED.selling_expenses,
    admin_expenses      = EXCLUDED.admin_expenses,
    financial_income    = EXCLUDED.financial_income,
    financial_expenses  = EXCLUDED.financial_expenses,
    other_income        = EXCLUDED.other_income,
    other_expenses      = EXCLUDED.other_expenses,
    ebitda              = EXCLUDED.ebitda,
    ingested_at         = NOW()
"""
BALANCE_SHEETS_UPSERT_SQL = """
INSERT INTO market_data.balance_sheets
    (asset_id, fiscal_date, year, quarter, source_provider,
     total_assets,
     total_liabilities, total_equity, cash_and_equivalents,
     short_term_assets, long_term_assets, short_term_liabilities,
     long_term_liabilities)
VALUES %s
ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
    year                   = EXCLUDED.year,
    quarter                = EXCLUDED.quarter,
    source_provider        = EXCLUDED.source_provider,
    total_assets           = EXCLUDED.total_assets,
    total_liabilities      = EXCLUDED.total_liabilities,
    total_equity           = EXCLUDED.total_equity,
    cash_and_equivalents   = EXCLUDED.cash_and_equivalents,
    short_term_assets      = EXCLUDED.short_term_assets,
    long_term_assets       = EXCLUDED.long_term_assets,
    short_term_liabilities = EXCLUDED.short_term_liabilities,
    long_term_liabilities  = EXCLUDED.long_term_liabilities,
    ingested_at            = NOW()
"""

FUNDAMENTALS_PROVIDER = FetcherFundamentalsProvider()
MARKET_DATA_REPOSITORY = MarketDataRepository()

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

local_tz = timezone("Asia/Bangkok")


with DAG(
    dag_id="market_data_fundamentals_weekly",
    default_args=default_args,
    schedule="0 19 * * 0",  # 7 PM Vietnam Time Sunday
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["financials", "supabase", "fundamentals", "weekly"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task(show_return_value_in_logs=False)
    def extract_income_statements():
        return fundamentals_orchestrator.extract_income_statements(
            provider=FUNDAMENTALS_PROVIDER
        )

    @task
    def load_income_statements(data):
        return fundamentals_orchestrator.load_income_statements(
            data,
            provider=FUNDAMENTALS_PROVIDER,
            repository=MARKET_DATA_REPOSITORY,
            db_url=SUPABASE_DB_URL,
            batch_size=DB_UPSERT_BATCH_SIZE,
            upsert_sql=INCOME_STATEMENTS_UPSERT_SQL,
            income_columns=INCOME_STATEMENT_COLUMNS,
        )

    @task(show_return_value_in_logs=False)
    def extract_balance_sheets():
        return fundamentals_orchestrator.extract_balance_sheets(
            provider=FUNDAMENTALS_PROVIDER
        )

    @task
    def load_balance_sheets(data):
        return fundamentals_orchestrator.load_balance_sheets(
            data,
            provider=FUNDAMENTALS_PROVIDER,
            repository=MARKET_DATA_REPOSITORY,
            db_url=SUPABASE_DB_URL,
            batch_size=DB_UPSERT_BATCH_SIZE,
            upsert_sql=BALANCE_SHEETS_UPSERT_SQL,
            balance_columns=BALANCE_SHEET_COLUMNS,
        )

    @task(trigger_rule="all_done")
    def finalize_fundamentals_load(income_summary, balance_summary):
        return fundamentals_orchestrator.finalize_fundamentals_load(
            income_summary,
            balance_summary,
        )

    with TaskGroup("fundamental_pipeline", tooltip="Income Statements") as fund_group:
        e_inc = extract_income_statements.override(pool=FINANCE_PROVIDER_POOL)()
        l_inc = load_income_statements(e_inc)
        e_inc >> l_inc

    with TaskGroup(
        "balance_sheet_group", tooltip="Quarterly Balance Sheets"
    ) as balance_group:
        e_bal = extract_balance_sheets.override(pool=FINANCE_PROVIDER_POOL)()
        l_bal = load_balance_sheets(e_bal)
        e_bal >> l_bal

    final_summary = finalize_fundamentals_load(l_inc, l_bal)

    [fund_group, balance_group] >> final_summary
