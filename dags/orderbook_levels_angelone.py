import pyotp
import json
import time
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from SmartApi.smartConnect import SmartConnect
import websocket
import requests
import threading
from variables import *  # Place your API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET, etc.

DB_CONN_ID = "data_collection_pg"
PG_SCHEMA = "data_collection"
SNAPSHOT_FREQ = 5         # seconds (every 5 seconds)
MAX_RUNTIME = 23400       # seconds (6.5 hours for NSE, adjust if needed)

def get_feed_token_and_symbol_map():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(api_key=API_KEY)
    login_data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not (login_data.get("status") and "data" in login_data):
        raise RuntimeError(f"AngelOne login failed: {login_data}")
    feed_token = login_data['data']['feedToken']
    # Get all symbol_id/symbol from DB and map to AngelOne tokens
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    rows = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    symbol_ids = {row[1]: row[0] for row in rows}
    my_symbols = list(symbol_ids.keys())
    JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    scrip_master = requests.get(JSON_URL, timeout=15).json()
    instruments = {}
    for item in scrip_master:
        full = item.get("symbol", "")
        if item.get("exch_seg") == "NSE" and full.endswith("-EQ"):
            ticker = full[:-3]
            if ticker in my_symbols:
                instruments[ticker] = item.get("token")
    token_symbol_map = {v: k for k, v in instruments.items()}
    return feed_token, instruments, symbol_ids, token_symbol_map

def collect_and_store_orderbook_levels():
    feed_token, instruments, symbol_ids, token_symbol_map = get_feed_token_and_symbol_map()
    tokens = list(instruments.values())
    latest_books = {}
    end_time = time.time() + MAX_RUNTIME

    def on_message(ws, message):
        msg = json.loads(message)
        if "data" not in msg:
            return
        for tick in msg["data"]:
            token = tick.get("tk")
            symbol = token_symbol_map.get(token)
            if not symbol:
                continue
            depth = tick.get("depth", {})
            bids = depth.get("buy", [])
            asks = depth.get("sell", [])
            latest_books[symbol] = {
                "bids": bids[:5],
                "asks": asks[:5],
                "ts": datetime.now(pytz.UTC)
            }

    def on_error(ws, error): print("WS Error:", error)
    def on_close(ws, code, msg): print("WS Closed:", code, msg)
    def on_open(ws):
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
        "x-feed-token": feed_token
    }
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        header=header
    )

    ws_thread = threading.Thread(target=ws.run_forever)
    ws_thread.daemon = True
    ws_thread.start()

    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    try:
        while time.time() < end_time:
            now = datetime.now(pytz.UTC)
            for symbol, book in latest_books.items():
                symbol_id = symbol_ids[symbol]
                ts = now
                for side, orders in [("B", book["bids"]), ("A", book["asks"])]:
                    for level, order in enumerate(orders, start=1):
                        price = float(order.get("price", 0))
                        size = int(order.get("qty", 0))
                        try:
                            pg.run("""
                                INSERT INTO data.orderbook_levels
                                    (symbol_id, snapshot_ts, side, level, price, size, source, extra_data)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (symbol_id, snapshot_ts, side, level) DO NOTHING
                            """, parameters=(
                                symbol_id, ts, side, level, price, size, "AngelOne WS", json.dumps(order)
                            ))
                        except Exception as e:
                            print(f"Insert error: {symbol} {side} {level} {ts}: {e}")
            time.sleep(SNAPSHOT_FREQ)
    finally:
        ws.close()
        ws_thread.join(timeout=5)
        print("Finished Level 2 collection run.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="collect_orderbook_levels_angelone",
    default_args=default_args,
    schedule_interval="0 9 * * 1-5",  # 9am IST, Mon–Fri
    catchup=False,
    tags=["angelone", "orderbook", "level2", "prod"],
) as dag:
    collect_task = PythonOperator(
        task_id="collect_orderbook_levels",
        python_callable=collect_and_store_orderbook_levels,
    )
