from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from SmartApi.smartConnect import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import json
import requests
import pytz

from variables import M_API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET

DB_CONN_ID = "data_collection_pg"
TRADING_DAYS = {0, 1, 2, 3, 4}  # Monday=0, Friday=4

def get_instruments_and_symbol_ids():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='data_collection')
    rows = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    symbol_ids = {row[1]: row[0] for row in rows}
    my_symbols = set(symbol_ids.keys())
    JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    scrip_master = requests.get(JSON_URL, timeout=10).json()
    instruments = {}
    for item in scrip_master:
        full = item.get("symbol", "")
        if item.get("exch_seg") == "NSE" and full.endswith("-EQ"):
            ticker = full[:-3]
            if ticker in my_symbols:
                instruments[ticker] = item.get("token")
    return instruments, symbol_ids

def is_market_day(dt):
    return dt.weekday() in TRADING_DAYS

def get_minute_key(dt):
    return dt.strftime('%Y-%m-%d %H:%M')

def run_ws_collector():
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open <= now_ist <= market_close and is_market_day(now_ist)):
        print(f"Market closed. Current IST time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")
        return

    # AngelOne login
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(api_key=M_API_KEY)
    data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not (data.get("status") and "data" in data and "feedToken" in data["data"]):
        raise RuntimeError("Login failed or feedToken not found")
    FEED_TOKEN = data["data"]["feedToken"]
    AUTH_TOKEN = data["data"]["jwtToken"]

    instruments, symbol_ids = get_instruments_and_symbol_ids()
    if not instruments:
        print("No valid AngelOne tokens found for any symbols. Exiting.")
        return

    print(f"[{now_ist.strftime('%Y-%m-%d %H:%M:%S')}] Streaming: {list(instruments.keys())}")

    candle_buffer = {}

    def insert_ohlcv(symbol, ohlcv):
        try:
            pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='data_collection')
            pg.run("""
              INSERT INTO data.ohlcv
              (symbol_id, timestamp, open, high, low, close, volume, interval, adjusted, source, extra_data)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON CONFLICT DO NOTHING
            """,
            parameters=(
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
           )
           )
            print(f"Inserted OHLCV: {symbol} {ohlcv['timestamp']}")
        except Exception as e:
            print(f"DB Insert Error: {e}")

    # SmartWebSocketV2 handler functions
    def on_open(wsapp):
        print("WebSocket connected.")
        correlation_id = "airflow-dag-angelone"
        mode = 1  # FULL
        token_list = [{
            "exchangeType": 1,
            "tokens": list(instruments.values())
        }]
        sws.subscribe(correlation_id, mode, token_list)
        print(f"Subscribed tokens: {token_list}")

    def on_data(wsapp, message):
        # Sample message fields: subscription_mode, exchange_type, token, last_traded_price, exchange_timestamp
        try:
            msg = message if isinstance(message, dict) else json.loads(message)
            token = msg.get("token")
            price = float(msg.get("last_traded_price", 0)) / 100 if "last_traded_price" in msg else None
            ts = datetime.fromtimestamp(msg.get("exchange_timestamp", 0)/1000, tz=pytz.timezone("Asia/Kolkata"))
            symbol = next((k for k, v in instruments.items() if v == str(token)), None)
            if not symbol or not is_market_day(ts) or price is None:
                return
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
        except Exception as ex:
            print(f"Handler error: {ex}")

    def on_error(wsapp, error):
        print("WebSocket Error:", error)

    def on_close(wsapp):
        print("WebSocket closed.")

    # Setup and start WebSocket
    sws = SmartWebSocketV2(
        api_key=M_API_KEY,
        client_code=CLIENT_CODE,
        feed_token=FEED_TOKEN,
        auth_token=AUTH_TOKEN
    )
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close
    sws.connect()  # This will block and run until stopped

# Airflow DAG definition
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

