import requests
import csv
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

NSE_CORP_ACTIONS_URL = "https://www1.nseindia.com/corporates/datafiles/CA_ALL_FORTHCOMING.csv"

def fetch_nse_corp_actions():
    resp = requests.get(
        NSE_CORP_ACTIONS_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv",
            "Referer": "https://www.nseindia.com/",
        },
        timeout=20
    )
    resp.raise_for_status()
    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    return list(reader)

def parse_value_from_purpose(purpose):
    # Extract dividend value like: "Final Dividend - Rs 10 Per Share"
    if purpose and "Dividend" in purpose:
        match = re.search(r'Rs\.?\s*([\d.]+)', purpose)
        return float(match.group(1)) if match else None
    # You can extend this for splits/bonuses if you wish
    return None

def parse_date_safe(date_str):
    # Supports empty/null and handles common date errors
    try:
        if not date_str or date_str.strip() == "":
            return None
        return datetime.strptime(date_str.strip(), "%d-%b-%Y").date()
    except Exception:
        return None

def ingest_corporate_actions():
    pg = PostgresHook(postgres_conn_id='data_collection_pg', schema='data_collection')
    companies = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")
    symbol_map = {symbol: symbol_id for symbol_id, symbol in companies}

    try:
        nse_actions = fetch_nse_corp_actions()
        for action in nse_actions:
            symbol = action.get('SYMBOL')
            action_type = action.get('PURPOSE')
            ex_date = parse_date_safe(action.get('EX DATE'))
            record_date = parse_date_safe(action.get('RECORD DATE'))
            book_closure_start = parse_date_safe(action.get('BOOK CLOSURE START DATE'))
            book_closure_end = parse_date_safe(action.get('BOOK CLOSURE END DATE'))
            value = parse_value_from_purpose(action_type)
            details = action.get('REMARKS', '')
            source = "NSE"
            isin = action.get('ISIN', None)
            extra_data = {
                "raw": action
            }

            # Match symbol
            symbol_id = symbol_map.get(symbol)
            if not symbol_id:
                continue

            # Deduplication
            exists = pg.get_first(
                """
                SELECT 1 FROM data.corporate_actions
                 WHERE symbol_id=%s AND ex_date=%s AND action_type=%s AND source=%s
                """,
                parameters=(symbol_id, ex_date, action_type, source)
            )
            if exists:
                continue

            pg.run(
                """
                INSERT INTO data.corporate_actions
                  (symbol_id, isin, ex_date, record_date, book_closure_start,
                   book_closure_end, action_type, value, details, source, extra_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                parameters=(
                    symbol_id, isin, ex_date, record_date, book_closure_start,
                    book_closure_end, action_type, value, details, source, extra_data
                )
            )
            print(f"Inserted: {symbol} | {action_type} | {ex_date}")
    except Exception as e:
        print(f"Error in NSE corporate actions ingestion: {e}")

# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='collect_corporate_actions_daily',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['corporate_actions', 'india', 'stocks']
) as dag:
    ingest_actions = PythonOperator(
        task_id='ingest_corporate_actions',
        python_callable=ingest_corporate_actions,
    )
    #ingest_actions.set_downstream(ingest_actions)   