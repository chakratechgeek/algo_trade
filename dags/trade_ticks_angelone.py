import os
import pyotp
import json
import time
import pytz
import threading
import requests
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
MARKET_TZ   = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE= datetime.strptime("15:30", "%H:%M").time()

def is_market_open():
    now = datetime.now(MARKET_TZ).time()
    return (MARKET_OPEN <= now <= MARKET_CLOSE)

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
    return tokens

def get_auth():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = SmartConnect(M_API_KEY)
    data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not (data.get("status") and "data" in data):
        raise RuntimeError("AngelOne login failed: %s" % data)
    return obj, data["data"]["feedToken"], data["data"]["jwtToken"]

def collect_trade_ticks():
    obj, feed_token, jwt_token = get_auth()
    tokens = get_symbol_tokens()
    if not tokens:
        print("No valid tokens found, aborting.")
        return
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)

    stop_flag = threading.Event()
    last_tick = [time.time()]
    sws = None

    def insert_tick(symbol_id, ts, price, qty, trade_type, trade_id, buyer_code, seller_code, extra):
        try:
            pg.run("""
                INSERT INTO data.trade_ticks
                (symbol_id, timestamp, price, quantity, trade_type, trade_id,
                 buyer_code, seller_code, source, extra_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, parameters=(
                symbol_id, ts, price, qty, trade_type, trade_id,
                buyer_code, seller_code, "AngelOne", json.dumps(extra)
            ))
        except Exception as e:
            print(f"DB insert error: {e}")

    def on_data(wsapp, message):
        tick = message if isinstance(message, dict) else json.loads(message)
        # Accept both single-dict and data-list messages
        if "data" in tick:
            ticks = tick["data"]
        elif "token" in tick:
            ticks = [tick]
        else:
            print("Unknown tick format:", tick)
            return

        for t in ticks:
            token = str(t.get("token"))
            symbol_info = tokens.get(token)
            if not symbol_info:
                continue
            symbol_id = symbol_info["symbol_id"]

            # For LTP mode
            ts = t.get("exchange_timestamp")
            if ts:
                ts = datetime.fromtimestamp(int(ts)/1000, tz=pytz.UTC)
            else:
                ts = datetime.utcnow().replace(tzinfo=pytz.UTC)

            price = float(t.get("last_traded_price", 0))/100.0
            qty = int(t.get("quantity", 0)) if t.get("quantity") else 0

            # Some fields may be unavailable in LTP
            trade_type  = t.get("trade_type")
            trade_id    = t.get("trade_id")
            buyer_code  = t.get("buyer_code")
            seller_code = t.get("seller_code")
            insert_tick(symbol_id, ts, price, qty, trade_type, trade_id, buyer_code, seller_code, t)
        last_tick[0] = time.time()

    # --- CALLBACK SIGNATURES FIXED BELOW ---

    def on_open(wsapp):
        print("WebSocket connected.")
        token_list = [{"exchangeType": 1, "tokens": list(tokens.keys())}]
        # FIX: reference outer SmartWebSocketV2 object, not wsapp
        nonlocal sws
        sws.subscribe("tick-dag", 1, token_list)  # mode=1 for LTP
        print("Subscribed tokens:", token_list)

    def on_error(wsapp, error):
        print("WebSocket error:", error)

    def on_close(wsapp, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)
        stop_flag.set()

    # ---- Setup & Run ----
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

    print("Waiting for ticks (Ctrl+C to stop)...")
    try:
        while not stop_flag.is_set():
            if not is_market_open():
                print("Market closed, shutting down.")
                sws.close_connection()
                break
            if time.time() - last_tick[0] > 60:
                print("No ticks received in 60s, closing connection.")
                sws.close_connection()
                break
            time.sleep(5)
    except KeyboardInterrupt:
        print("Interrupted by user, closing socket.")
        sws.close_connection()
        t.join(timeout=5)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="collect_trade_ticks_angelone",
    default_args=default_args,
    schedule_interval="15 9 * * 1-5",  # 09:15 IST, Mon-Fri
    catchup=False,
    tags=["angelone", "tickdata", "prod"],
) as dag:
    collect_task = PythonOperator(
        task_id="collect_angelone_trade_ticks",
        python_callable=collect_trade_ticks,
    )

