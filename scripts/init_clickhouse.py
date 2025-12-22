import clickhouse_connect
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


def init_db():
    print(
        f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} as {CLICKHOUSE_USER}"
    )
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
        raise

    # 1. Read and Execute Schema from SQL File
    sql_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "sql",
        "init_schema.sql",
    )
    print(f"Reading schema from {sql_file_path}...")

    try:
        with open(sql_file_path, "r") as f:
            sql_script = f.read()

        # Split by semicolon to get individual commands
        # Filter out empty strings/whitespace
        commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

        for command in commands:
            print(f"Executing: {command[:50]}...")
            client.command(command)

        print("ClickHouse Schema Initialized.")

    except FileNotFoundError:
        print(f"Error: SQL file not found at {sql_file_path}")
        raise
    except Exception as e:
        print(f"Error executing schema script: {e}")
        raise

    # 5. Enrich Data: Fetch Company List from vnstock
    print("Fetching company list from vnstock...")
    try:
        import sys

        # Ensure project root is in path
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from scripts.manual_load_data import update_company_dimension

        update_company_dimension(client)

    except Exception as e:
        print(f"Failed to fetch or insert company data: {e}")
        # We don't raise here to allow the script to complete schema init even if fetch fails
        pass

    # 6. Load Initial Data (5 Years)
    print("Loading 5 years of historical data...")
    try:
        # Import here to avoid issues if dependencies aren't met during top-level import
        # Ensure sys.path is set correctly for imports in manual_load_data
        import sys

        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        from scripts.manual_load_data import manual_load
        from datetime import datetime, timedelta

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")

        # Call manual_load
        manual_load(start_date, end_date)
        print("Historical data loaded.")
    except Exception as e:
        print(f"Failed to load historical data: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    init_db()
