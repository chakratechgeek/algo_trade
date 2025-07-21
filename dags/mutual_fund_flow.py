import requests
import time
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

DB_CONN_ID = "data_collection_pg"
PG_SCHEMA = "data"
PG_TABLE = "mf_navs"

def fetch_and_store_mf_navs():
    pg = PostgresHook(postgres_conn_id=DB_CONN_ID, schema=PG_SCHEMA)
    conn = pg.get_conn()
    cursor = conn.cursor()

    schemes = requests.get("https://api.mfapi.in/mf", timeout=20).json()
    inserted, updated, skipped = 0, 0, 0

    for scheme in schemes:
        scheme_code = scheme['schemeCode']
        resp = requests.get(f"https://api.mfapi.in/mf/{scheme_code}", timeout=20)
        data = resp.json()
        if data and 'meta' in data and 'data' in data and data['data']:
            nav_item = data['data'][0]
            nav_date = nav_item['date']
            nav = nav_item['nav']
            repurchase = nav_item.get('repurchase_price')
            sale_price = nav_item.get('sale_price')
            # Upsert into mf_navs table
            try:
                cursor.execute(f"""
                    INSERT INTO {PG_SCHEMA}.{PG_TABLE}
                    (scheme_code, scheme_name, nav_date, nav, repurchase, sale_price, extra_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (scheme_code, nav_date) DO UPDATE
                        SET nav = EXCLUDED.nav,
                            repurchase = EXCLUDED.repurchase,
                            sale_price = EXCLUDED.sale_price,
                            extra_data = EXCLUDED.extra_data
                """, (
                    int(scheme_code),
                    data['meta']['scheme_name'],
                    nav_date,
                    float(nav.replace(',', '')),
                    float(repurchase.replace(',', '')) if repurchase else None,
                    float(sale_price.replace(',', '')) if sale_price else None,
                    json.dumps(data['meta'])
                ))
                inserted += 1
            except Exception as e:
                print(f"Insert/Update error for {scheme_code} on {nav_date}: {e}")
                skipped += 1
            time.sleep(0.2)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted/Updated: {inserted}, Skipped: {skipped}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_mf_navs_all_mfapi",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Every day at 6am
    catchup=False,
    tags=["mf", "nav", "prod"],
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_mf_navs",
        python_callable=fetch_and_store_mf_navs,
    )
