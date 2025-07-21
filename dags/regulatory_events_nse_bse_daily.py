# pip install bse apache-airflow[postgres]
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from bse import BSE
import json

DB_CONN_ID = "data_collection_pg"
SCHEMA = "data_collection"
SYMBOL_TABLE = "data.symbols"
EVENTS_TABLE = "data.regulatory_events"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def ingest_bse_announcements(**context):
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=SCHEMA)
    # Create a lowercase stripped mapping from company name to symbol_id
    symbols = pg.get_records(f"SELECT symbol_id, name FROM {SYMBOL_TABLE} WHERE active = TRUE")
    symbol_id_by_name = {row[1].strip().lower(): row[0] for row in symbols if row[1]}

    # Fetch BSE announcements
    with BSE('./') as bse:
        page_no = 1
        announcements = []
        while True:
            res = bse.announcements(page_no=page_no)
            if page_no == 1:
                total = int(res['Table1'][0]['ROWCNT'])
                print(f"Total announcements: {total}")
            announcements.extend(res['Table'])
            print(f"Fetched page {page_no}, total collected: {len(announcements)}")
            page_no += 1
            if len(announcements) >= total:
                break

    inserted = 0
    for ann in announcements:
        # Try mapping by SLONGNAME
        company_name = ann.get('SLONGNAME', '').strip().lower()
        symbol_id = symbol_id_by_name.get(company_name)

        # If not found, fallback to parsing NEWSSUB for company name
        if not symbol_id:
            news_sub = ann.get('NEWSSUB', '')
            parsed_name = news_sub.split(' - ')[0].strip().lower() if news_sub else ''
            symbol_id = symbol_id_by_name.get(parsed_name)
        
        if not symbol_id:
            print(f"Announcement for company '{company_name}' / '{parsed_name}' not found in symbols, skipping.")
            continue

        # Parse event datetime
        event_time_str = ann.get('DT_TM') or ann.get('NEWS_DT')
        try:
            event_time = datetime.fromisoformat(event_time_str)
        except Exception:
            print(f"Invalid event time '{event_time_str}' for symbol_id {symbol_id}, skipping.")
            continue

        event_type = ann.get('SUBCATNAME') or ann.get('CATEGORYNAME') or ann.get('ANNOUNCEMENT_TYPE', 'Unknown')
        details = ann.get('NEWSSUB', '') or ann.get('HEADLINE', '')
        status = ann.get('FILESTATUS', '')
        source = "BSE"
        extra_data = json.dumps(ann)

        # Check for duplicates
        exists = pg.get_first(
            f"""
            SELECT 1 FROM {EVENTS_TABLE}
            WHERE symbol_id = %s AND event_time = %s AND event_type = %s AND source = %s
            """,
            parameters=(symbol_id, event_time, event_type, source)
        )
        if exists:
            continue

        try:
            pg.run(
                f"""
                INSERT INTO {EVENTS_TABLE}
                    (symbol_id, event_time, event_type, details, status, source, extra_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                parameters=(symbol_id, event_time, event_type, details, status, source, extra_data)
            )
            inserted += 1
            print(f"Inserted event {event_type} for symbol_id {symbol_id} on {event_time_str}")
        except Exception as e:
            print(f"Failed to insert event for symbol_id {symbol_id}: {e}")

    print(f"Inserted {inserted} new events into {EVENTS_TABLE}")

with DAG(
    dag_id='bse_regulatory_events_daily',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['bse', 'regulatory', 'india']
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_bse_announcements',
        python_callable=ingest_bse_announcements,
        provide_context=True,
    )
