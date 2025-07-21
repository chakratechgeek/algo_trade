import pyotp
from SmartApi.smartConnect import SmartConnect
import websocket
import json
import requests
from datetime import datetime, timedelta
import pytz
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from variables import *

DB_CONN_ID = "data_collection_pg"
TRADING_DAYS = {0, 1, 2, 3, 4}

def get_feed_token():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(api_key=API_KEY)
    data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if data.get("status") and "data" in data and "feedToken" in data["data"]:
        return data["data"]["feedToken"]
    raise RuntimeError("Login failed or feedToken not found")

def get_instruments_and_symbol_ids():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='data_collection')
    rows = pg.get_records("SELECT symbol_id, symbol FROM symbols WHERE   = TRUE")
    symbol_ids = {row[1]: row[0] for row in rows}
    my_symbols = list(symbol_ids.keys())
    JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        scrip_master = requests.get(JSON_URL, timeout=10).json()
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

def is_market_day(dt):
    return dt.weekday() in TRADING_DAYS

def get_minute_key(dt):
    return dt.strftime('%Y-%m-%d %H:%M')

def run_ws_collector():
    FEED_TOKEN = get_feed_token()  # Auto-login!
    instruments, symbol_ids = get_instruments_and_symbol_ids()
    if not instruments:
        print("No valid AngelOne tokens found for any symbols. Exiting.")
        return

    print(f"Streaming: {list(instruments.keys())}")
    candle_buffer = {}

    def insert_ohlcv(symbol, ohlcv):
        try:
            pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='data_collection')
            pg.run("""
                INSERT INTO ohlcv
                  (symbol_id, timestamp, open, high, low, close, volume, interval, adjusted, source, extra_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """, (
                    symbol_ids[symbol],
                    ohlcv["timestamp"],
                    ohlcv["open"],
                    ohlcv["high"],
                    ohlcv["low"],
                    ohlcv["close"],
                    ohlcv["volume"],
                    "1m",
                    False,
                    "AngelOne SmartAPI",
                    json.dumps({})
                ))
            print(f"Inserted OHLCV: {symbol} {ohlcv['timestamp']}")
        except Exception as e:
            print(f"DB Insert Error: {e}")

    def on_message(ws, message):
        nonlocal candle_buffer
        msg = json.loads(message)
        if "data" not in msg:
            return
        for tick in msg["data"]:
            token = tick.get("tk")
            price = float(tick.get("ltp", 0))
            ts = datetime.fromtimestamp(int(tick["ft"]), tz=pytz.timezone("Asia/Kolkata"))
            symbol = None
            for k, v in instruments.items():
                if v == token:
                    symbol = k
                    break
            if not symbol or not is_market_day(ts):
                continue

            minute_key = get_minute_key(ts)
            if symbol not in candle_buffer:
                candle_buffer[symbol] = {}

            if minute_key not in candle_buffer[symbol]:
                candle_buffer[symbol][minute_key] = {
                    "open": price, "high": price, "low": price, "close": price,
                    "volume": 0,
                    "timestamp": ts.replace(second=0, microsecond=0)
                }
            else:
                candle = candle_buffer[symbol][minute_key]
                candle["high"] = max(candle["high"], price)
                candle["low"] = min(candle["low"], price)
                candle["close"] = price

            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            to_delete = []
            for key in list(candle_buffer[symbol]):
                candle_time = candle_buffer[symbol][key]["timestamp"]
                if now - candle_time > timedelta(minutes=1):
                    insert_ohlcv(symbol, candle_buffer[symbol][key])
                    to_delete.append(key)
            for key in to_delete:
                del candle_buffer[symbol][key]

    def on_error(ws, error):
        print("Error:", error)

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)

    def on_open(ws):
        print("WebSocket connected.")
        tokens = list(instruments.values())
        payload = {
            "action": 1,
            "params": {
                "mode": "FULL",
                "tokenList": [{"exchangeType": 1, "tokens": tokens}]
            }
        }
        ws.send(json.dumps(payload))

    ws_url = "wss://smartapisocket.angelone.in/smart-stream"
    header = {
        "x-api-key": API_KEY,
        "x-client-code": CLIENT_CODE,
        "x-feed-token": FEED_TOKEN
    }
    print("Connecting to AngelOne WebSocket...")
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        header=header
    )
    ws.run_forever()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="collect_intraday_ohlcv_angelone",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["angelone", "ohlcv", "marketdata"],
) as dag:
    collect_task = PythonOperator(
        task_id="collect_intraday_ohlcv",
        python_callable=run_ws_collector,
    )
