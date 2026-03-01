-- ===================================================================
-- portfolios_tracker_dw — ClickHouse Data Warehouse Schema
-- ===================================================================
-- Single source of truth for the entire DW DDL.
-- Executed by: services/data-pipeline/scripts/init_clickhouse_schema.py
--
-- Conventions:
--   Database     : portfolios_tracker_dw
--   Engine       : ReplacingMergeTree(ingested_at) — idempotent upserts
--   Money values : Decimal64(2) for raw market data
--   Computed     : Float64 for derived / calculated values
--   Partitioning : Added to large fact tables for efficient pruning & TTL
--   ORDER BY     : Optimised for the most common query patterns
--   No OPTIMIZE TABLE FINAL in automated DAGs — rely on FINAL in reads
-- ===================================================================

CREATE DATABASE IF NOT EXISTS portfolios_tracker_dw;

-- ===================================================================
-- DIMENSION: Date
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.dim_date (
    date           Date,
    year           UInt16,
    quarter        UInt8,
    month          UInt8,
    day            UInt8,
    day_of_week    UInt8,
    is_weekend     UInt8,
    quarter_label  String,
    month_name     String,
    day_name       String
) ENGINE = ReplacingMergeTree()
ORDER BY date;

-- Seed 2015-01-01 → 2035-12-31 (idempotent — skips existing rows)
INSERT INTO portfolios_tracker_dw.dim_date
SELECT *
FROM (
    WITH
        toDate('2015-01-01') AS start_date,
        toDate('2035-12-31') AS end_date
    SELECT
        date,
        toYear(date)          AS year,
        toQuarter(date)       AS quarter,
        toMonth(date)         AS month,
        toDayOfMonth(date)    AS day,
        toDayOfWeek(date)     AS day_of_week,
        if(toDayOfWeek(date) >= 6, 1, 0) AS is_weekend,
        concat('Q', toString(toQuarter(date)), ' ', toString(toYear(date))) AS quarter_label,
        monthName(date)       AS month_name,
        CASE toDayOfWeek(date)
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE ''
        END AS day_name
    FROM (
        SELECT arrayJoin(range(toUInt32(end_date) - toUInt32(start_date) + 1)) + start_date AS date
    )
)
WHERE date NOT IN (SELECT date FROM portfolios_tracker_dw.dim_date);

-- ===================================================================
-- DIMENSION: Assets (Single Table Inheritance — STI)
-- ===================================================================
-- Unified schema for all asset types.
-- Discriminator: asset_class (STOCK, CRYPTO, BOND, ETF, FOREX, COMMODITY)
-- Market: Country code for stocks (VN, US, UK, JP); empty for CRYPTO/FOREX
-- Partitioned by asset_class for efficient per-type analytics.
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.dim_assets (
    -- Core identifiers
    symbol                  String,
    name_en                 String,
    name_local              String                  DEFAULT '',
    -- Classification (STI discriminator + market)
    asset_class             LowCardinality(String),   -- STOCK, CRYPTO, BOND, ETF, FOREX, COMMODITY
    market                  LowCardinality(String)  DEFAULT '',   -- VN, US, UK, JP
    currency                LowCardinality(String),   -- VND, USD, GBP, JPY
    exchange                LowCardinality(String)  DEFAULT '',   -- HOSE, NASDAQ, NYSE
    -- Common metadata
    sector                  LowCardinality(String)  DEFAULT '',
    industry                String                  DEFAULT '',
    logo_url                String                  DEFAULT '',
    description             String                  DEFAULT '',
    -- Type-specific metadata (flexible Map)
    -- STOCK : {isin, cik, sedol, cusip, figi}
    -- CRYPTO: {coingecko_id, chain, contract_address, is_stablecoin}
    -- COMMODITY: {provider, unit}
    external_api_metadata   Map(String, String)     DEFAULT map(),
    -- Data lineage
    source                  LowCardinality(String)  DEFAULT '',   -- vnstock, yfinance, coingecko
    is_active               UInt8                   DEFAULT 1,
    ingested_at             DateTime                DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (asset_class, market, symbol)
PARTITION BY asset_class;

-- ===================================================================
-- FACT: Daily Stock Prices
-- ===================================================================
-- Largest table — partitioned by month for efficient range scans and TTL.
-- ORDER BY (ticker, trading_date) optimises per-stock time-series queries.
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.fact_stock_daily (
    ticker         String,
    trading_date   Date,
    open           Decimal64(2),
    high           Decimal64(2),
    low            Decimal64(2),
    close          Decimal64(2),
    volume         UInt64,
    ma_50          Float64    DEFAULT 0,
    ma_200         Float64    DEFAULT 0,
    rsi_14         Float64    DEFAULT 0,
    daily_return   Float64    DEFAULT 0,
    macd           Float64    DEFAULT 0,
    macd_signal    Float64    DEFAULT 0,
    macd_hist      Float64    DEFAULT 0,
    source         LowCardinality(String) DEFAULT 'vnstock',
    ingested_at    DateTime   DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, trading_date)
PARTITION BY toYYYYMM(trading_date);

-- ===================================================================
-- FACT: Quarterly Financial Ratios
-- ===================================================================
-- fiscal_date in ORDER BY enables ASOF JOIN against daily prices.
-- Partitioned by year for efficient full-quarter scans.
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.fact_financial_ratios (
    ticker               String,
    fiscal_date          Date,
    year                 UInt16,
    quarter              UInt8,
    -- Valuation
    pe_ratio             Float64,
    pb_ratio             Float64,
    ps_ratio             Float64,
    p_cashflow_ratio     Float64,
    eps                  Float64,
    bvps                 Float64,
    market_cap           Float64,
    roe                  Float64,
    roa                  Float64,
    roic                 Float64,
    net_profit_margin    Float64,
    debt_to_equity       Float64,
    financial_leverage   Float64,
    dividend_yield       Float64,
    ingested_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, fiscal_date)
PARTITION BY toYear(fiscal_date);

-- ===================================================================
-- FACT: Dividend History
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.fact_dividends (
    ticker                      String,
    exercise_date               Date,
    cash_year                   UInt16,
    cash_dividend_percentage    Float64,
    stock_dividend_percentage   Float64,
    issue_method                String,
    ingested_at                 DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, exercise_date);

-- ===================================================================
-- FACT: Income Statement
-- ===================================================================
-- Vietnamese companies report revenue in Trillions (10^12+);
-- Decimal128(2) prevents overflow.
-- Partitioned by year for efficient YoY analysis.
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.fact_income_statement (
    ticker               String,
    fiscal_date          Date,
    year                 UInt16,
    quarter              UInt8,
    revenue              Decimal128(2),
    cost_of_goods_sold   Decimal128(2),
    gross_profit         Decimal128(2),
    operating_profit     Decimal128(2),
    net_profit_post_tax  Decimal128(2),
    ingested_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, year, quarter)
PARTITION BY toYear(fiscal_date);

-- ===================================================================
-- FACT: Market News
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.fact_news (
    ticker              String,
    publish_date        DateTime,
    title               String,
    source              String,
    price_at_publish    Float64,
    price_change        Float64,
    price_change_ratio  Float64,
    rsi                 Float64,
    rs                  Float64,
    news_id             UInt64,
    ingested_at         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, publish_date, news_id)
PARTITION BY toYYYYMM(publish_date);

-- ===================================================================
-- DERIVED: Backward-Adjusted OHLCV Prices
-- ===================================================================
-- Populated by the refresh_adjusted_prices Airflow DAG.
-- Raw prices (fact_stock_daily) are NEVER modified; this table is purely
-- derived and can be truncated + rebuilt at any time.
--
-- Includes full OHLCV for charting; adj_factor applied to all price columns.
-- raw_close kept as reference anchor.
-- ===================================================================
CREATE TABLE IF NOT EXISTS portfolios_tracker_dw.adjusted_ohlcv (
    ticker          String,
    trading_date    Date,
    adjusted_open   Float64,                           -- Backward-adjusted open
    adjusted_high   Float64,                           -- Backward-adjusted high
    adjusted_low    Float64,                           -- Backward-adjusted low
    adjusted_close  Float64,                           -- Backward-adjusted close
    adjusted_volume Float64  DEFAULT 0,                -- Volume (inverse-adjusted for splits)
    raw_close       Decimal64(2),                      -- Original close from fact_stock_daily
    adj_factor      Float64  DEFAULT 1.0,              -- Cumulative adjustment factor
    ingested_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, trading_date)
PARTITION BY toYYYYMM(trading_date);


-- ===================================================================
-- ANALYTICAL VIEWS
-- ===================================================================

-- View 1: Master Daily Data
-- Joins daily prices with dim_assets metadata and nearest quarterly ratios.
-- Uses ASOF JOIN for ratios (inequality on fiscal_date).
-- dim_assets join is a plain equi-join on ticker = symbol.
CREATE OR REPLACE VIEW portfolios_tracker_dw.view_market_daily_master AS
SELECT
    f.ticker                     AS ticker,
    d.name_en                    AS company_name,
    d.exchange                   AS exchange,
    d.sector                     AS sector,
    d.industry                   AS industry,
    f.trading_date               AS trading_date,
    f.open                       AS open,
    f.high                       AS high,
    f.low                        AS low,
    f.close                      AS close,
    f.volume                     AS volume,
    f.daily_return               AS daily_return,
    f.ma_50                      AS ma_50,
    f.ma_200                     AS ma_200,
    f.rsi_14                     AS rsi_14,
    f.macd                       AS macd,
    f.macd_signal                AS macd_signal,
    f.macd_hist                  AS macd_hist,
    r.market_cap                 AS market_cap_snapshot,
    r.roic                       AS roic,
    toFloat64(
        f.close - lagInFrame(f.close, 20) OVER (PARTITION BY f.ticker ORDER BY f.trading_date)
    ) / NULLIF(
        toFloat64(lagInFrame(f.close, 20) OVER (PARTITION BY f.ticker ORDER BY f.trading_date)),
        0
    ) * 100                      AS return_1m
FROM portfolios_tracker_dw.fact_stock_daily AS f
LEFT JOIN portfolios_tracker_dw.dim_assets AS d
    ON f.ticker = d.symbol AND d.asset_class = 'STOCK' AND d.market = 'VN'
ASOF LEFT JOIN portfolios_tracker_dw.fact_financial_ratios AS r
    ON f.ticker = r.ticker AND f.trading_date >= r.fiscal_date;

-- View 2: Daily Valuation Tracker
-- ASOF JOIN maps daily prices to the most recent quarterly report.
-- daily_pe_ratio is computed live from current price and quarterly EPS.
-- Note: VN stock prices are in thousands of VND; EPS is in VND → multiply by 1000.
CREATE OR REPLACE VIEW portfolios_tracker_dw.view_valuation_daily AS
SELECT
    d.ticker,
    d.trading_date,
    d.close,
    r.roe,
    r.roic,
    r.debt_to_equity,
    r.net_profit_margin,
    r.eps,
    r.fiscal_date                AS last_report_date,
    toFloat64(d.close * 1000) / NULLIF(r.eps, 0)  AS daily_pe_ratio,
    r.pe_ratio                   AS snapshot_pe_quarter_end
FROM portfolios_tracker_dw.fact_stock_daily AS d
ASOF LEFT JOIN portfolios_tracker_dw.fact_financial_ratios AS r
    ON d.ticker = r.ticker AND d.trading_date >= r.fiscal_date;

-- View 3: Fundamental Health Scanner
-- CTE + LAG for safe Year-Over-Year growth calculation (look-back 4 quarters).
CREATE OR REPLACE VIEW portfolios_tracker_dw.view_fundamental_health AS
WITH metrics_with_lag AS (
    SELECT
        ticker,
        year,
        quarter,
        fiscal_date,
        revenue,
        net_profit_post_tax,
        gross_profit,
        lag(revenue, 4)              OVER (PARTITION BY ticker ORDER BY fiscal_date) AS revenue_last_year,
        lag(net_profit_post_tax, 4)  OVER (PARTITION BY ticker ORDER BY fiscal_date) AS profit_last_year
    FROM portfolios_tracker_dw.fact_income_statement
)
SELECT
    m.ticker,
    m.year,
    m.quarter,
    m.fiscal_date,
    m.revenue,
    m.net_profit_post_tax,
    m.gross_profit,
    r.roe,
    r.net_profit_margin,
    r.debt_to_equity,
    toFloat64(m.revenue - m.revenue_last_year)
        / abs(toFloat64(NULLIF(m.revenue_last_year, 0))) * 100   AS revenue_growth_yoy,
    toFloat64(m.net_profit_post_tax - m.profit_last_year)
        / abs(toFloat64(NULLIF(m.profit_last_year, 0))) * 100    AS profit_growth_yoy
FROM metrics_with_lag AS m
LEFT JOIN portfolios_tracker_dw.fact_financial_ratios AS r
    ON m.ticker = r.ticker AND m.fiscal_date = r.fiscal_date
ORDER BY m.ticker, m.year, m.quarter