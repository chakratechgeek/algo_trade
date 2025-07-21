import pyotp
import time
import threading
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from SmartApi.smartWebSocketV2 import SmartWebSocketV2  # You may need to use correct import!
from variables import *  # your keys

DB_CONN_ID = "data_collection_pg"
PG_SCHEMA = "data_collection"
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:30"

def is_market_open():
    now = datetime.now().astimezone().time()
    return (now >= datetime.strptime(MARKET_OPEN, "%H:%M").time()
            and now <= datetime.strptime(MARKET_CLOSE, "%H:%M").time())

def get_jwt_token():
    import SmartApi.smartConnect as sc
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj = sc.SmartConnect(api_key=API_KEY)
    login_data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not (login_data.get("status") and "data" in login_data):
        raise RuntimeError(f"AngelOne login failed: {login_data}")
    jwt_token = login_data["data"]["jwtToken"]
    obj.setAccessToken(jwt_token)
    return jwt_token, obj

def get_symbol_tokens():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    rows = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    # Fetch AngelOne tokens
    import requests
    JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    scrip_master = requests.get(JSON_URL, timeout=15).json()
    symbol_ids = {row[1]: row[0] for row in rows}
    tokens = {}
    for item in scrip_master:
        full = item.get("symbol", "")
        if item.get("exch_seg") == "NSE" and full.endswith("-EQ"):
            ticker = full[:-3]
            if ticker in symbol_ids:
                tokens[ticker] = {"symbol_id": symbol_ids[ticker], "token": item.get("token")}
    return tokens

def run_tick_collector():
    jwt_token, obj = get_jwt_token()
    tokens = get_symbol_tokens()
    token_list = [v["token"] for v in tokens.values()]
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2

    def on_data(wsapp, message):
        # Parse AngelOne tick data
        try:
            import json
            tick = json.loads(message)
            if 'data' not in tick: return
            for d in tick['data']:
                token = d.get('tk')
                symbol_id = None
                for k, v in tokens.items():
                    if v["token"] == token:
                        symbol_id = v["symbol_id"]
                        break
                if symbol_id is None: continue
                ts = datetime.fromtimestamp(int(d["ft"]), tz=None)
                price = float(d["ltp"])
                qty = int(d.get("qty", 0))
                pg.run("""
                    INSERT INTO data.trade_ticks
                    (symbol_id, timestamp, price, quantity, trade_type, source, extra_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, parameters=(symbol_id, ts, price, qty, d.get("ttype", ""), "AngelOne SmartAPI", json.dumps(d)))
        except Exception as e:
            print("Tick insert error:", e)

    def on_open(wsapp):
        print("WebSocket connected.")
        wsapp.subscribe(token_list, mode="FULL")  # Adjust mode if needed

    def on_error(wsapp, error):
        print("WebSocket error:", error)

    def on_close(wsapp):
        print("WebSocket closed.")

    # Connect websocket
    s = SmartWebSocketV2(
        api_key=API_KEY,
        client_code=CLIENT_CODE,
        feed_token=jwt_token,
        on_message=on_data,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    print("Connecting to SmartAPI WebSocket for tick data...")
    s.connect()

    # Run until market close
    while is_market_open():
        time.sleep(10)
    s.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="collect_trade_ticks_angelone",
    default_args=default_args,
    schedule_interval="15 9 * * 1-5",  # Every weekday 09:15 IST
    catchup=False,
    tags=["angelone", "tickdata", "prod"],
) as dag:
    collect_ticks_task = PythonOperator(
        task_id="collect_angelone_trade_ticks",
        python_callable=run_tick_collector,
    )
