
import yaml
import snowflake.connector
import sys

CONFIG_PATH = "snowflake_connections.yaml"
SECTION = "connections.my_example_connection"

def main():
    try:
        # Load YAML config
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        conn_cfg = cfg.get("connections", {}).get("my_example_connection", {})
        if not conn_cfg:
            raise ValueError(f"Section [{SECTION}] not found in {CONFIG_PATH}")

        # Build connection parameters
        params = {
            "account": conn_cfg.get("account"),
            "user": conn_cfg.get("user"),
            "authenticator": conn_cfg.get("authenticator", "externalbrowser"),
            "role": conn_cfg.get("role"),
            "warehouse": conn_cfg.get("warehouse"),
            "database": conn_cfg.get("database"),
            "schema": conn_cfg.get("schema"),
        }

        print("Connecting to Snowflake via externalbrowser...")
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()

        # Quick checks
        checks = [
            ("SELECT CURRENT_VERSION()", "Snowflake version"),
            ("SELECT CURRENT_ACCOUNT()", "Account"),
            ("SELECT CURRENT_ROLE()", "Role"),
            ("SELECT CURRENT_WAREHOUSE()", "Warehouse"),
            ("SELECT CURRENT_DATABASE()", "Database"),
            ("SELECT CURRENT_SCHEMA()", "Schema"),
            ("SELECT CURRENT_USER()", "User"),
        ]
        for sql, label in checks:
            cursor.execute(sql)
            print(f"{label}: {cursor.fetchone()[0]}")

        cursor.execute("SELECT 1 AS ok")
        print("Test SELECT 1 ->", cursor.fetchone()[0])

        cursor.close()
        conn.close()
        print("✅ Connection successful and closed.")

    except Exception as ex:
        print("Error:", ex)
        sys.exit(1)

if __name__ == "__main__":
    main()
