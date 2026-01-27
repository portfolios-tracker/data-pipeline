import clickhouse_connect
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


def init_schema():
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
        raw_commands = sql_script.split(";")

        for idx, command in enumerate(raw_commands):
            # Strip whitespace
            command = command.strip()

            # Skip empty commands or comments-only blocks
            if not command or all(
                line.strip().startswith("--") or not line.strip()
                for line in command.split("\n")
            ):
                continue

            # Log a preview of what we're executing
            first_line = command.split("\n")[0][:80]
            print(f"[{idx + 1}/{len(raw_commands)}] Executing: {first_line}...")

            try:
                client.command(command)
            except Exception as e:
                print(f"Error executing command {idx + 1}: {e}")
                print(f"Command preview: {command[:200]}...")
                raise

        print("ClickHouse Schema Initialized successfully.")

    except FileNotFoundError:
        print(f"Error: SQL file not found at {sql_file_path}")
        raise
    except Exception as e:
        print(f"Error executing schema script: {e}")
        raise


if __name__ == "__main__":
    init_schema()
