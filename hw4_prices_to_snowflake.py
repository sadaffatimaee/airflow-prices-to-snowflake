from airflow import DAG
from airflow.models import Variable
from typing import List, Dict, Any
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


DAG_ID = "hw4_prices_to_snowflake"
SNOWFLAKE_CONN_ID = "snowflake_conn"
DEFAULT_SYMBOL = Variable.get("HW4_SYMBOL", default_var="AAPL")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["data226", "hw4"],
    default_args=default_args,
    description="HW4 port: Alpha Vantage -> Snowflake (transactional full refresh)",
    params={"symbol": DEFAULT_SYMBOL},
):
    @task
    def get_api_key() -> str:
        return Variable.get("ALPHAVANTAGE_API_KEY")

    @task
    def extract_90(symbol: str, api_key: str) -> List[Dict]:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": api_key,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        ts = data.get("Time Series (Daily)")
        if not ts:
            raise ValueError(f"Unexpected API response: {data}")
        latest_days = sorted(ts.keys(), reverse=True)[:90]
        rows = []
        for ds in latest_days:
            v = ts[ds]
            rows.append({
                "symbol": symbol,
                "date": ds,
                "open": float(v["1. open"]),
                "high": float(v["2. high"]),
                "low": float(v["3. low"]),
                "close": float(v["4. close"]),
                "volume": int(v["5. volume"]),
            })
        rows.sort(key=lambda r: r["date"])
        return rows

    @task
    def ensure_table() -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS RAW.PRICE_DAILY (
          SYMBOL  STRING NOT NULL,
          DATE    DATE   NOT NULL,
          OPEN    NUMBER(18,4),
          CLOSE   NUMBER(18,4),
          HIGH    NUMBER(18,4),
          LOW     NUMBER(18,4),
          VOLUME  NUMBER(38,0),
          CONSTRAINT PK_PRICE_DAILY PRIMARY KEY (SYMBOL, DATE)
        )
        """
        SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(ddl, autocommit=True)

    @task
    def load_transactional(symbol: str, rows: List[Dict]) -> int:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute("DELETE FROM RAW.PRICE_DAILY WHERE SYMBOL = %s", (symbol,))
            insert_sql = """
              INSERT INTO RAW.PRICE_DAILY (SYMBOL, DATE, OPEN, CLOSE, HIGH, LOW, VOLUME)
              VALUES (%(symbol)s, %(date)s::DATE, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s)
            """
            cur.executemany(insert_sql, rows)
            inserted = cur.rowcount or len(rows)
            cur.execute("COMMIT")
            return inserted
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
            conn.close()

    @task
    def verify_counts(symbol: str, expected: int) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        df = hook.get_pandas_df(
            "SELECT COUNT(*) AS C FROM RAW.PRICE_DAILY WHERE SYMBOL = %s",
            parameters=(symbol,),
        )
        total = int(df.iloc[0]["C"])
        assert total == expected, f"Idempotency failed: got {total}, expected {expected}"

    @task
    def log_summary(n: int, symbol: str) -> None:
        print(f"Inserted {n} rows into RAW.PRICE_DAILY for {symbol}")

    api = get_api_key()
    sym = "{{ params.symbol }}"
    rows = extract_90(sym, api)
    created = ensure_table()
    inserted = load_transactional(sym, rows)
    checked = verify_counts(sym, inserted)
    done = log_summary(inserted, sym)

    created >> rows >> inserted >> checked >> done
