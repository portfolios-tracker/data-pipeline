CREATE DATABASE IF NOT EXISTS market_dwh;

CREATE DATABASE IF NOT EXISTS market_dwh;

-- Table: Date Dimension
CREATE TABLE IF NOT EXISTS market_dwh.dim_date (
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    quarter_label String,
    month_name String,
    day_name String
) ENGINE = ReplacingMergeTree ()
ORDER BY date;

INSERT INTO
    market_dwh.dim_date
SELECT *
FROM (
        WITH
            toDate ('2015-01-01') as start_date, toDate ('2035-12-31') as end_date
        SELECT
            date, toYear (date) as year, toQuarter (date) as quarter, toMonth (date) as month, toDayOfMonth (date) as day, toDayOfWeek (date) as day_of_week, if (toDayOfWeek (date) >= 6, 1, 0) as is_weekend, concat(
                'Q', toString (toQuarter (date)), ' ', toString (toYear (date))
            ) as quarter_label, monthName (date) as month_name, CASE toDayOfWeek (date)
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
                WHEN 7 THEN 'Sunday'
                ELSE ''
            END as day_name
        FROM (
                SELECT arrayJoin (
                        range (
                            toUInt32 (end_date) - toUInt32 (start_date) + 1
                        )
                    ) + start_date AS date
            )
    )
WHERE
    date NOT IN (
        SELECT date
        FROM market_dwh.dim_date
    );
-- Idempotency check

-- Table: Daily Stock Prices
CREATE TABLE IF NOT EXISTS market_dwh.fact_stock_daily (
    ticker String,
    trading_date Date,
    open Decimal64 (2),
    high Decimal64 (2),
    low Decimal64 (2),
    close Decimal64 (2),
    volume UInt64,
    ma_50 Float64 DEFAULT 0,
    ma_200 Float64 DEFAULT 0,
    rsi_14 Float64 DEFAULT 0,
    daily_return Float64 DEFAULT 0,
    macd Float64 DEFAULT 0,
    macd_signal Float64 DEFAULT 0,
    macd_hist Float64 DEFAULT 0,
    source String DEFAULT 'vnstock',
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree (ingested_at)
ORDER BY (ticker, trading_date);

-- Table: Quarterly Financial Ratios
CREATE TABLE IF NOT EXISTS market_dwh.fact_financial_ratios (
    ticker String,
    fiscal_date Date,
    year UInt16,
    quarter UInt8,

-- VALUATION
pe_ratio Float64,
pb_ratio Float64,
ps_ratio Float64,
p_cashflow_ratio Float64,
eps Float64,
bvps Float64,
market_cap Float64,
roe Float64,
roa Float64,
roic Float64,
net_profit_margin Float64,
debt_to_equity Float64,
financial_leverage Float64,
dividend_yield Float64,
ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (ticker, year, quarter);

-- Table: Company Metadata
CREATE TABLE IF NOT EXISTS market_dwh.dim_stock_companies (
    symbol String,
    organ_name String,
    exchange String,
    sector String,
    industry String,
    sub_industry String,
    icb_code String,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree (ingested_at)
ORDER BY symbol;

-- Table: Dividend History
CREATE TABLE IF NOT EXISTS market_dwh.fact_dividends (
    ticker String,
    exercise_date Date,
    cash_year UInt16,
    cash_dividend_percentage Float64,
    stock_dividend_percentage Float64,
    issue_method String,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree (ingested_at)
ORDER BY (ticker, exercise_date);

-- Table: Income Statement
-- Vietnamese companies can report revenue in Trillions (10^12+), which risks overflow in smaller types.
CREATE TABLE IF NOT EXISTS market_dwh.fact_income_statement (
    ticker String,
    fiscal_date Date,
    year UInt16,
    quarter UInt8,
    revenue Decimal128 (2),
    cost_of_goods_sold Decimal128 (2),
    gross_profit Decimal128 (2),
    operating_profit Decimal128 (2),
    net_profit_post_tax Decimal128 (2),
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree (ingested_at)
ORDER BY (ticker, year, quarter);

CREATE TABLE IF NOT EXISTS market_dwh.fact_news (
    ticker String,
    publish_date DateTime,
    title String,
    source String,
    price_at_publish Float64,
    price_change Float64,
    price_change_ratio Float64,
    rsi Float64,
    rs Float64,
    news_id UInt64,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree (ingested_at)
ORDER BY (ticker, publish_date, news_id);

-- ==========================================
-- ANALYTICAL VIEWS
-- ==========================================

-- View 1: Master Daily Data (Joins Prices with Company Names)
CREATE OR REPLACE VIEW market_dwh.view_market_daily_master AS
SELECT
    f.ticker AS ticker,
    d.organ_name AS company_name,
    d.exchange AS exchange,
    d.sector AS sector,
    d.industry AS industry,
    f.trading_date AS trading_date,
    f.open AS open,
    f.high AS high,
    f.low AS low,
    f.close AS
close,
f.volume AS volume,
f.daily_return AS daily_return,
f.ma_50 AS ma_50,
f.ma_200 AS ma_200,
f.rsi_14 AS rsi_14,
f.macd AS macd,
f.macd_signal AS macd_signal,
f.macd_hist AS macd_hist,
r.market_cap AS market_cap_snapshot,
r.roic AS roic,
toFloat64 (
    f.close - lagInFrame (f.close, 20) OVER (
        PARTITION BY
            f.ticker
        ORDER BY f.trading_date
    )
) / NULLIF(
    toFloat64 (
        lagInFrame (f.close, 20) OVER (
            PARTITION BY
                f.ticker
            ORDER BY f.trading_date
        )
    ),
    0
) * 100 AS return_1m
FROM market_dwh.fact_stock_daily AS f
    LEFT JOIN market_dwh.dim_stock_companies AS d ON f.ticker = d.symbol ASOF
    LEFT JOIN market_dwh.fact_financial_ratios AS r ON f.ticker = r.ticker
    AND f.trading_date >= r.fiscal_date;

-- View 2: Daily Valuation Tracker
-- Logic: Uses ASOF JOIN to map Daily Prices to the most recent Quarterly Report
CREATE OR REPLACE VIEW market_dwh.view_valuation_daily AS
SELECT d.ticker, d.trading_date, d.close,

-- 1. METRICS THAT ARE OKAY TO BE STATIC (From Quarterly Table)
r.roe,
r.roic,
r.debt_to_equity,
r.net_profit_margin,
r.eps, -- The raw Earnings Per Share
r.fiscal_date AS last_report_date,

-- 2. DYNAMIC METRICS
-- Instead of showing the old 'pe_ratio' from the table, we calculate it live.
-- Formula: (Current Daily Price * 1000) / Quarterly EPS
-- Note: Stock prices in Vietnam are quoted in thousands of VND, but EPS is in VND
toFloat64 (d.close * 1000) / NULLIF(r.eps, 0) AS daily_pe_ratio,

-- Comparison: Keep the snapshot just for reference (optional)
r.pe_ratio AS snapshot_pe_quarter_end
FROM market_dwh.fact_stock_daily AS d ASOF
    LEFT JOIN market_dwh.fact_financial_ratios AS r ON d.ticker = r.ticker
    AND d.trading_date >= r.fiscal_date;

-- View 3: Fundamental Health Scanner
-- Logic: Uses CTE + LAG Window Function for safe Year-Over-Year Growth calculation
CREATE OR REPLACE VIEW market_dwh.view_fundamental_health AS
WITH
    metrics_with_lag AS (
        SELECT
            ticker,
            year,
            quarter,
            fiscal_date,
            revenue,
            net_profit_post_tax,
            gross_profit,
            -- Look back 4 rows (1 year)
            lag(revenue, 4) OVER (
                PARTITION BY
                    ticker
                ORDER BY fiscal_date
            ) as revenue_last_year,
            lag(net_profit_post_tax, 4) OVER (
                PARTITION BY
                    ticker
                ORDER BY fiscal_date
            ) as profit_last_year
        FROM market_dwh.fact_income_statement
    )
SELECT
    m.ticker,
    m.year,
    m.quarter,
    m.fiscal_date,
    -- Raw Metrics
    m.revenue,
    m.net_profit_post_tax,
    m.gross_profit,
    -- Ratios
    r.roe,
    r.net_profit_margin,
    r.debt_to_equity,
    toFloat64 (
        m.revenue - m.revenue_last_year
    ) / abs(
        toFloat64 (
            NULLIF(m.revenue_last_year, 0)
        )
    ) * 100 AS revenue_growth_yoy,
    toFloat64 (
        m.net_profit_post_tax - m.profit_last_year
    ) / abs(
        toFloat64 (NULLIF(m.profit_last_year, 0))
    ) * 100 AS profit_growth_yoy
FROM metrics_with_lag AS m
    LEFT JOIN market_dwh.fact_financial_ratios AS r ON m.ticker = r.ticker
    AND m.fiscal_date = r.fiscal_date
ORDER BY m.ticker, m.year, m.quarter;