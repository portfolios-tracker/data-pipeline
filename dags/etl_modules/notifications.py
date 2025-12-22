import os
import logging
import requests
from datetime import datetime
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


def get_latest_stock_data(tickers):
    """
    Fetches the latest stock data from ClickHouse views for the given tickers.
    Returns a dictionary with ticker as key and technical/fundamental data as value.
    """
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
    clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
        )

        stock_data = {}

        for ticker in tickers:
            # Get latest market data with technical indicators
            market_query = f"""
            SELECT 
                ticker,
                trading_date,
                close,
                volume,
                daily_return,
                ma_50,
                ma_200,
                rsi_14,
                macd,
                macd_signal,
                macd_hist,
                return_1m,
                sector,
                industry
            FROM market_dwh.view_market_daily_master
            WHERE ticker = '{ticker}'
            ORDER BY trading_date DESC
            LIMIT 1
            """

            market_result = client.query(market_query)

            if market_result.result_rows:
                row = market_result.result_rows[0]
                stock_data[ticker] = {
                    "trading_date": str(row[1]),
                    "close": float(row[2]),
                    "volume": int(row[3]),
                    "daily_return": float(row[4]),
                    "ma_50": float(row[5]),
                    "ma_200": float(row[6]),
                    "rsi_14": float(row[7]),
                    "macd": float(row[8]),
                    "macd_signal": float(row[9]),
                    "macd_hist": float(row[10]),
                    "return_1m": float(row[11]) if row[11] is not None else 0,
                    "sector": str(row[12]) if row[12] else "N/A",
                    "industry": str(row[13]) if row[13] else "N/A",
                }

                # Get latest valuation data
                valuation_query = f"""
                SELECT 
                    daily_pe_ratio,
                    roe,
                    roic,
                    debt_to_equity,
                    net_profit_margin,
                    eps
                FROM market_dwh.view_valuation_daily
                WHERE ticker = '{ticker}'
                ORDER BY trading_date DESC
                LIMIT 1
                """

                val_result = client.query(valuation_query)

                if val_result.result_rows:
                    val_row = val_result.result_rows[0]
                    stock_data[ticker].update(
                        {
                            "pe_ratio": float(val_row[0])
                            if val_row[0] is not None
                            else 0,
                            "roe": float(val_row[1]) if val_row[1] is not None else 0,
                            "roic": float(val_row[2]) if val_row[2] is not None else 0,
                            "debt_to_equity": float(val_row[3])
                            if val_row[3] is not None
                            else 0,
                            "net_profit_margin": float(val_row[4])
                            if val_row[4] is not None
                            else 0,
                            "eps": float(val_row[5]) if val_row[5] is not None else 0,
                        }
                    )

        return stock_data

    except Exception as e:
        logging.error(f"Failed to fetch stock data from database: {e}")
        return {}


def summarize_news_with_gemini(news_data):
    """
    Uses Gemini AI to generate an intelligent summary of the news with current technical data.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        logging.warning("Gemini API key not found. Using basic summary.")
        return None

    # Prepare news data for Gemini
    news_by_ticker = {}
    for item in news_data:
        ticker = item.get("ticker")
        if ticker not in news_by_ticker:
            news_by_ticker[ticker] = []
        news_by_ticker[ticker].append(
            {
                "title": item.get("title", ""),
                "price_change": item.get("price_change_ratio", 0),
                "source": item.get("source", ""),
            }
        )

    # Fetch latest stock data from database
    tickers = list(news_by_ticker.keys())
    stock_data = get_latest_stock_data(tickers)

    # Create enhanced prompt for Gemini with technical data
    prompt = f"""You are a financial analyst summarizing Vietnamese stock market news for {datetime.now().strftime("%Y-%m-%d")}.

Here is today's news data with current technical and fundamental indicators:

"""

    for ticker, articles in sorted(news_by_ticker.items()):
        prompt += f"\n{ticker}:"

        # Add technical data if available
        if ticker in stock_data:
            data = stock_data[ticker]
            prompt += f"""
  Current Price: {data["close"]:.2f} VND (1000s)
  Daily Return: {data["daily_return"]:.2f}%
  1-Month Return: {data["return_1m"]:.2f}%
  RSI(14): {data["rsi_14"]:.2f}
  MACD: {data["macd"]:.2f} (Signal: {data["macd_signal"]:.2f}, Hist: {data["macd_hist"]:.2f})
  MA50: {data["ma_50"]:.2f}, MA200: {data["ma_200"]:.2f}
  P/E Ratio: {data["pe_ratio"]:.2f}
  ROE: {data["roe"]:.2f}%, ROIC: {data["roic"]:.2f}%
  Debt/Equity: {data["debt_to_equity"]:.2f}
  Sector: {data["sector"]}, Industry: {data["industry"]}
"""

        prompt += "\n  News Articles:\n"
        for i, article in enumerate(articles[:5], 1):
            prompt += f"  {i}. {article['title']} (Price change at publish: {article['price_change']:.2f}%)\n"

        if len(articles) > 5:
            prompt += f"  ...and {len(articles) - 5} more articles\n"

    prompt += """

Please provide:
1. A brief overall market sentiment (2-3 sentences)
2. Key highlights for each stock with technical analysis context:
   - Interpret the news in light of current technical indicators (RSI, MACD, moving averages)
   - Mention if the stock is overbought/oversold, trending up/down
   - Note any divergences between news sentiment and technical signals
3. Any notable trends or patterns you observe
4. Brief investment considerations based on the combination of news and technicals

Keep the summary concise, professional, and actionable for investors. Format using simple text, no markdown."""

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    headers = {"Content-Type": "application/json", "X-goog-api-key": gemini_api_key}

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 800},
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()

        # Extract the generated text
        if "candidates" in result and len(result["candidates"]) > 0:
            text = result["candidates"][0]["content"]["parts"][0]["text"]
            logging.info("Gemini summary generated successfully")
            return text
        else:
            logging.warning("No summary generated by Gemini")
            return None

    except Exception as e:
        logging.error(f"Failed to generate Gemini summary: {e}")
        return None


def send_telegram_news_summary(news_data):
    """
    Sends a formatted news summary to Telegram with Gemini AI analysis.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logging.warning("Telegram credentials not found. Skipping news notification.")
        return

    if not news_data:
        logging.info("No news data to send.")
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # Try to get Gemini summary
    gemini_summary = summarize_news_with_gemini(news_data)

    if gemini_summary:
        # Send AI-generated summary
        header = f"📰 *AI Market News Summary - {today}*\n🤖 _Powered by Gemini AI_\n\n"
        text = header + gemini_summary
    else:
        # Fallback to basic summary
        header = f"📰 *Market News Summary - {today}*\n\n"

        # Group news by ticker
        news_by_ticker = {}
        for item in news_data:
            ticker = item.get("ticker")
            if ticker not in news_by_ticker:
                news_by_ticker[ticker] = []
            news_by_ticker[ticker].append(item)

        message_parts = [header]

        for ticker, articles in sorted(news_by_ticker.items()):
            message_parts.append(f"*{ticker}* ({len(articles)} articles)")

            # Show up to 3 most recent articles per ticker
            for i, article in enumerate(articles[:3], 1):
                title = article.get("title", "No title")[:100]  # Truncate long titles
                price_change = article.get("price_change_ratio", 0)
                price_emoji = (
                    "📈" if price_change > 0 else "📉" if price_change < 0 else "➡️"
                )

                message_parts.append(
                    f"{i}. {title}... {price_emoji} {price_change:.2f}%"
                )

            if len(articles) > 3:
                message_parts.append(f"   _...and {len(articles) - 3} more_")

            message_parts.append("")  # Empty line between tickers

        text = "\n".join(message_parts)

    # Telegram message limit is 4096 characters
    if len(text) > 4000:
        text = text[:3900] + "\n\n_...message truncated_"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Telegram news summary sent: {len(news_data)} articles")
    except Exception as e:
        logging.error(f"Failed to send Telegram news summary: {e}")


def send_telegram_message(context, status):
    """
    Sends a Telegram notification based on the DAG run status.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logging.warning("Telegram credentials not found. Skipping notification.")
        return

    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id")
    execution_date = context.get("logical_date") or context.get("execution_date")

    if status == "FAILED":
        emoji = "🔴"
        task_instance = context.get("task_instance")
        task_id = task_instance.task_id if task_instance else "Unknown"
        exception = context.get("exception")
        text = (
            f"{emoji} *DAG Failed*\n"
            f"DAG: `{dag_id}`\n"
            f"Task: `{task_id}`\n"
            f"Run ID: `{run_id}`\n"
            f"Time: `{execution_date}`\n"
            f"Error: `{exception}`"
        )
    else:
        emoji = "🟢"
        text = (
            f"{emoji} *DAG Success*\n"
            f"DAG: `{dag_id}`\n"
            f"Run ID: `{run_id}`\n"
            f"Time: `{execution_date}`"
        )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Telegram notification sent for {dag_id}")
    except Exception as e:
        logging.error(f"Failed to send Telegram notification: {e}")


def send_success_notification(context):
    send_telegram_message(context, status="SUCCESS")


def send_failure_notification(context):
    send_telegram_message(context, status="FAILED")
