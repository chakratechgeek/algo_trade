import pyotp
from SmartApi.smartConnect import SmartConnect
import requests
import json
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil import parser as dateutil_parser
from variables import *  # Place your API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET, etc.

DB_CONN_ID = "data_collection_pg"
PG_SCHEMA = "data_collection"
BATCH_DAYS = 30     # Max 30 days per AngelOne API request
HIST_MONTHS = 6     # How many months back you want
INTERVAL = "FIVE_MINUTE"

def get_jwt_token():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(api_key=API_KEY)
    login_data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    print("AngelOne login response:", login_data)
    if not (login_data.get("status") and "data" in login_data):
        raise RuntimeError(f"AngelOne login failed: {login_data}")
    try:
        jwt_token = login_data["data"]["jwtToken"]   # Use this for Bearer header
    except Exception as e:
        print("AngelOne login_data (missing jwtToken):", login_data)
        raise RuntimeError("Login failed or jwtToken not found") from e
    obj.setAccessToken(jwt_token)
    return jwt_token

def get_instruments_and_symbol_ids():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    rows = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    symbol_ids = {row[1]: row[0] for row in rows}
    my_symbols = list(symbol_ids.keys())
    JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        scrip_master = requests.get(JSON_URL, timeout=15).json()
    except Exception as e:
        raise Exception(f"Failed to fetch scrip master: {e}")
    instruments = {}
    for item in scrip_master:
        full = item.get("symbol", "")
        if item.get("exch_seg") == "NSE" and full.endswith("-EQ"):
            ticker = full[:-3]
            if ticker in my_symbols:
                instruments[ticker] = item.get("token")
    for sym in my_symbols:
        if sym not in instruments:
            print(f"Warning: {sym}-EQ not found in scrip master, skipping.")
    return instruments, symbol_ids

def fetch_angelone_history(jwt_token, token, interval, from_date, to_date):
    url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData"
    headers = {
        "X-PrivateKey": API_KEY,
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-UserType": "USER",
        "Authorization": jwt_token,  # Already has "Bearer ...", do not add again!
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": interval,
        "fromdate": from_date,
        "todate": to_date
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    print(f"API fetch for token {token}: {from_date} - {to_date} - Status {resp.status_code}")
    if resp.status_code != 200:
        print("AngelOne API error:", resp.text)
        raise Exception(f"AngelOne API error: {resp.text}")
    out = resp.json()
    if out.get('status') is not True or 'data' not in out:
        print("AngelOne response error:", out)
        raise Exception(f"AngelOne API error: {out}")
    return out['data']

def ingest_history_to_db():
    jwt_token = get_jwt_token()
    instruments, symbol_ids = get_instruments_and_symbol_ids()
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    for symbol, token in instruments.items():
        print(f"Fetching history for {symbol}")
        for m in range(HIST_MONTHS * 30 // BATCH_DAYS):
            to_dt = datetime.now() - timedelta(days=m * BATCH_DAYS)
            from_dt = to_dt - timedelta(days=BATCH_DAYS)
            from_str = from_dt.strftime('%Y-%m-%d %H:%M')
            to_str = to_dt.strftime('%Y-%m-%d %H:%M')
            try:
                candles = fetch_angelone_history(jwt_token, token, INTERVAL, from_str, to_str)
            except Exception as e:
                print(f"Fetch error: {symbol} {from_str}-{to_str}: {e}")
                time.sleep(10)
                continue
            print(f"  Got {len(candles)} candles from {from_str} to {to_str}")
            for candle in candles:
                try:
                    pg.run("""
                        INSERT INTO data.ohlcv_history
                        (symbol_id, timestamp, open, high, low, close, volume, interval, adjusted, source, extra_data)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (symbol_id, interval, timestamp) DO NOTHING
                    """, parameters=(
                        symbol_ids[symbol],
                        dateutil_parser.parse(candle[0]),
                        float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5]),
                        "1m", False, "AngelOne REST", json.dumps({})
                    ))

                except Exception as e:
                    print(f"Insert error: {symbol} {candle[0]}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_intraday_ohlcv_history_angelone",
    default_args=default_args,
    schedule_interval=None,  # Run manually for historical backfill
    catchup=False,
    tags=["angelone", "ohlcv", "historical", "prod"],
) as dag:
    fetch_hist_task = PythonOperator(
        task_id="fetch_angelone_intraday_history",
        python_callable=ingest_history_to_db,
    )
