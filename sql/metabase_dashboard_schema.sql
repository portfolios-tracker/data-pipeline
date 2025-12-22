-- 1. Current Daily price and previous day's closing price for the given ticker
SELECT *
FROM (
    SELECT
        trading_date,
        close
    FROM market_dwh.view_market_daily_master
    WHERE ticker = {{ticker}}
    ORDER BY trading_date DESC
    LIMIT 2
)
ORDER BY trading_date ASC

-- 2. Market Cap (Bn VND) for the given ticker
SELECT
    argMax(market_cap_snapshot, trading_date) as "Market Cap (Bn VND)"
FROM market_dwh.view_market_daily_master
WHERE ticker = {{ticker}}

-- 3. Quarterly EPS (VND) for the given ticker
SELECT
    argMax(eps, trading_date) as "EPS"
FROM market_dwh.view_valuation_daily
WHERE ticker = {{ticker}}

-- 4. P/E Ratio (TTM) for the given ticker
SELECT
    argMax(daily_pe_ratio, trading_date) as "P/E (TTM)"
FROM market_dwh.view_valuation_daily
WHERE ticker = {{ticker}}

-- 5. Recent News impacting the given ticker
SELECT
    publish_date,
    source,
    title,
    price_change_ratio as "Impact"
FROM market_dwh.fact_news
WHERE ticker = {{ticker}}
ORDER BY publish_date DESC
LIMIT 10

-- 6. Historical Prices for the given ticker
SELECT
	trading_date,
    close,
	volume,
    ma_50,
    ma_200
FROM market_dwh.view_market_daily_master
WHERE ticker = {{ticker}}
AND {{trading_date_filter}}
ORDER BY trading_date

-- 7. Historical MACD for the given ticker
SELECT
    trading_date,
    macd,
    macd_signal,
    macd_hist
FROM market_dwh.view_market_daily_master
WHERE ticker = {{ticker}}
AND {{trading_date_filter}}
ORDER BY trading_date

-- 8. Fundamental Health Metrics for the given ticker
SELECT 
    fiscal_date,
    revenue,
    net_profit_post_tax
FROM market_dwh.view_fundamental_health
WHERE ticker = {{ticker}}
ORDER BY fiscal_date ASC

-- 9. Valuation Metrics for the given ticker
SELECT 
    trading_date,
    daily_pe_ratio
FROM market_dwh.view_valuation_daily
WHERE ticker = {{ticker}}
AND {{trading_date_filter}}
ORDER BY trading_date

-- 10. Profitability Metrics for the given ticker
SELECT 
    fiscal_date,
    roe as "ROE %",
    net_profit_margin as "Net Margin %"
FROM market_dwh.view_fundamental_health
WHERE ticker = {{ticker}}
ORDER BY fiscal_date ASC

-- 11. Growth Metrics for the given ticker
SELECT 
    fiscal_date,
    revenue_growth_yoy,
    profit_growth_yoy
FROM market_dwh.view_fundamental_health
WHERE ticker = {{ticker}}
AND (revenue_growth_yoy IS NOT NULL OR profit_growth_yoy IS NOT NULL)
ORDER BY fiscal_date ASC