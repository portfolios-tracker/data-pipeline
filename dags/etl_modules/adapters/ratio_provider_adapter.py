from dags.etl_modules.fetcher import fetch_financial_ratios, get_active_vn_stock_tickers


class RatioProviderAdapter:
    def list_assets(self) -> list[dict[str, str]]:
        return get_active_vn_stock_tickers(raise_on_fallback=True)

    def fetch_ratios(self, symbol: str, asset_id: str):
        return fetch_financial_ratios(symbol, asset_id)
