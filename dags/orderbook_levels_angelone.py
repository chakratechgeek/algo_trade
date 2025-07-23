import os
import json
import time
import pyotp
import requests
import pytz
import threading
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from variables import M_API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET

DB_CONN_ID  = "data_collection_pg"
PG_SCHEMA   = "data_collection"
SCRIP_URL   = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
SYMBOLS     = ["SBIN", "RELIANCE", "HDFCBANK"]  # Change as needed
WS_MODE     = 3  # FULL depth (orderbook)
MARKET_TZ   = pytz.timezone("Asia/Kolkata")
MAX_LEVELS  = 5

def get_symbol_tokens():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    rows = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    symbol_ids = {row[1]: row[0] for row in rows}
    scrip_master = requests.get(SCRIP_URL, timeout=10).json()
    tokens = {}
    for item in scrip_master:
        full = item.get("symbol", "")
        if item.get("exch_seg") == "NSE" and full.endswith("-EQ"):
            ticker = full[:-3]
            if ticker in symbol_ids:
                tokens[item.get("token")] = {"symbol": ticker, "symbol_id": symbol_ids[ticker]}
    print("[MAP] tokens:", tokens)
    return tokens

def get_auth():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(M_API_KEY)
    data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not (data.get("status") and "data" in data):
        raise RuntimeError("AngelOne login failed: %s" % data)
    print("[AUTH] Login successful. Feed token (masked):", data["data"]["feedToken"][:10], "...")
    return obj, data["data"]["feedToken"], data["data"]["jwtToken"]

def collect_orderbook_levels():
    obj, feed_token, jwt_token = get_auth()
    tokens = get_symbol_tokens()
    if not tokens:
        print("[ERR] No valid tokens found, aborting.")
        return
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    stop_flag = threading.Event()
    last_tick = [time.time()]

    def insert_levels(symbol_id, snapshot_ts, side, levels, source, extra_data):
        for level, order in enumerate(levels, start=1):
            price = float(order.get("price", 0))
            size  = int(order.get("quantity", 0))
            try:
                pg.run("""
                    INSERT INTO data.orderbook_levels
                    (symbol_id, snapshot_ts, side, level, price, size, source, extra_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol_id, snapshot_ts, side, level) DO NOTHING
                """, parameters=(
                    symbol_id, snapshot_ts, side, level, price, size, source, json.dumps(order)
                ))
                print(f"[DB] Inserted: {symbol_id} {side}{level} {price}@{size}")
            except Exception as e:
                print(f"[DB] Insert error {symbol_id} {side}{level}: {e}")

    def on_data(wsapp, message):
        print("[WS] RAW MESSAGE:", message)
        tick = message if isinstance(message, dict) else json.loads(message)
        token = str(tick.get("token")) or str(tick.get("tk"))
        symbol_info = tokens.get(token)
        if not symbol_info:
            print(f"[WARN] Unmapped token {token}")
            return
        symbol_id = symbol_info["symbol_id"]
        snapshot_ts = tick.get("exchange_timestamp")
        if snapshot_ts:
            snapshot_ts = datetime.fromtimestamp(int(snapshot_ts)/1000, tz=pytz.UTC)
        else:
            snapshot_ts = datetime.utcnow().replace(tzinfo=pytz.UTC)
        bids = tick.get("best_5_buy_data", [])[:MAX_LEVELS]
        asks = tick.get("best_5_sell_data", [])[:MAX_LEVELS]
        insert_levels(symbol_id, snapshot_ts, "B", bids, "AngelOne", tick)
        insert_levels(symbol_id, snapshot_ts, "A", asks, "AngelOne", tick)
        last_tick[0] = time.time()

    def on_open(wsapp):
        print("[WS] Connected. Subscribing (mode=3)...")
        token_list = [{"exchangeType": 1, "tokens": list(tokens.keys())}]
        #owsapp.subscribe("orderbook-dag", WS_MODE, token_list)
        sws.subscribe("orderbook-dag", WS_MODE, token_list)
        print("[WS] Subscribed tokens:", token_list)

    def on_error(wsapp, error):
        print("[WS] Error:", error)
        wsapp.close_connection()

    def on_close(wsapp, code, msg):
        print("[WS] Closed:", code, msg)
        stop_flag.set()

    sws = SmartWebSocketV2(
        api_key=M_API_KEY,
        client_code=CLIENT_CODE,
        feed_token=feed_token,
        auth_token=jwt_token
    )
    sws.on_open  = on_open
    sws.on_data  = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    t = threading.Thread(target=sws.connect, daemon=True)
    t.start()
    try:
        while not stop_flag.is_set():
            # If no ticks in 2 minutes, exit
            if time.time() - last_tick[0] > 120:
                print("[MAIN] No ticks for 2 minutes, exiting.")
                sws.close_connection()
                break
            time.sleep(5)
    except KeyboardInterrupt:
        print("[MAIN] Ctrl+C pressed, closing connection.")
        sws.close_connection()
        t.join(timeout=5)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="collect_orderbook_levels_angelone",
    default_args=default_args,
    schedule_interval="15 9 * * 1-5",  # 09:15 IST, Mon-Fri
    catchup=False,
    tags=["angelone", "orderbook", "level2", "prod"],
) as dag:
    collect_task = PythonOperator(
        task_id="collect_angelone_orderbook_levels",
        python_callable=collect_orderbook_levels,
    )

