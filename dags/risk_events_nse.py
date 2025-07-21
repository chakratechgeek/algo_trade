import requests
import json
from bs4 import BeautifulSoup
from datetime import datetime, time as dt_time, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pytz

DB_CONN_ID = "data_collection_pg"
PG_SCHEMA = "data"
PG_TABLE = "risk_events"
EXCHANGE = "NSE"
MARKET_TZ = pytz.timezone("Asia/Kolkata")

def is_market_open(now=None):
    """Check if current time is market hours Mon-Fri 9:15-15:30 IST"""
    now = now or datetime.now(MARKET_TZ)
    if now.weekday() >= 5:
        return False
    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)
    return market_open <= now.time() <= market_close

def fetch_nse_risk_events():
    """Scrape or fetch risk event data from NSE website."""
    # Replace URL & parsing logic with actual NSE surveillance actions page
    url = "https://www.nseindia.com/regulations/surveillance-actions"
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # TODO: Parse the risk events table properly; placeholder below
    events = []

    # Example: Find all rows in a table with risk events (adjust selectors as needed)
    table = soup.find("table")
    if not table:
        return events

    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        # Example column parsing, update according to actual NSE page structure
        event_type = cols[0].get_text(strip=True).lower().replace(" ", "_")
        symbol = cols[1].get_text(strip=True) or None
        event_time_str = cols[2].get_text(strip=True)
        event_time = None
        try:
            event_time = datetime.strptime(event_time_str, "%d-%b-%Y %H:%M:%S")
            event_time = MARKET_TZ.localize(event_time)
        except Exception:
            event_time = datetime.now(MARKET_TZ)
        details = {"notes": cols[3].get_text(strip=True)}

        events.append({
            "event_type": event_type,
            "exchange": EXCHANGE,
            "symbol": symbol,
            "event_time": event_time,
            "event_details": details
        })
    return events

def ingest_risk_events():
    if not is_market_open():
        print("Market is closed; skipping risk events ingestion.")
        return

    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    conn = pg.get_conn()
    cursor = conn.cursor()

    events = fetch_nse_risk_events()
    inserted, updated, skipped = 0, 0, 0
    for e in events:
        try:
            cursor.execute(f"""
                INSERT INTO {PG_SCHEMA}.{PG_TABLE}
                (event_type, exchange, symbol, event_time, event_details)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (exchange, event_type, symbol, event_time) DO UPDATE
                SET event_details = EXCLUDED.event_details,
                    ingestion_time = now()
            """, (
                e['event_type'],
                e['exchange'],
                e['symbol'],
                e['event_time'],
                json.dumps(e['event_details'])
            ))
            inserted += 1
        except Exception as err:
            print(f"Error inserting event {e}: {err}")
            skipped += 1
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted/Updated: {inserted}, Skipped: {skipped}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20, tzinfo=MARKET_TZ),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="collect_nse_risk_events",
    default_args=default_args,
    schedule_interval="*/2 9-15 * * 1-5",  # Every 2 minutes, Mon-Fri 9AM-3:59PM IST approx
    catchup=False,
    tags=["risk", "market", "nse"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_nse_risk_events",
        python_callable=ingest_risk_events,
    )
