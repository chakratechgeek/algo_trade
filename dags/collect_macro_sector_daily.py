import yfinance as yf
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

macro_metrics = [
    # NSE Indices (Yahoo codes)
    ('Nifty_50', '^NSEI', 'Yahoo Finance', 'pts', ['index', 'nifty']),
    ('Nifty_Bank', '^NSEBANK', 'Yahoo Finance', 'pts', ['index', 'banking']),
    ('Nifty_Fin_Service', '^NSEFIN', 'Yahoo Finance', 'pts', ['index', 'finance']),
    ('Nifty_IT', '^CNXIT', 'Yahoo Finance', 'pts', ['index', 'IT']),
    ('Nifty_Midcap_50', '^NSEMDCP50', 'Yahoo Finance', 'pts', ['index', 'midcap']),
    ('Sensex', '^BSESN', 'Yahoo Finance', 'pts', ['index', 'sensex']),
    # Commodities
    ('MCX_Gold', 'GC=F', 'Yahoo Finance', 'USD/oz', ['commodity', 'gold']),
    ('MCX_Silver', 'SI=F', 'Yahoo Finance', 'USD/oz', ['commodity', 'silver']),
    ('Crude_Oil', 'CL=F', 'Yahoo Finance', 'USD/bbl', ['commodity', 'oil']),
    ('Natural_Gas', 'NG=F', 'Yahoo Finance', 'USD/MMBtu', ['commodity', 'natgas']),
    ('Copper', 'HG=F', 'Yahoo Finance', 'USD/lb', ['commodity', 'copper']),
    # Currencies
    ('USD_INR', 'INR=X', 'Yahoo Finance', 'INR', ['forex', 'usd']),
    ('EUR_INR', 'EURINR=X', 'Yahoo Finance', 'INR', ['forex', 'eur']),
    ('GBP_INR', 'GBPINR=X', 'Yahoo Finance', 'INR', ['forex', 'gbp']),
    # Global Indices
    ('S&P_500', '^GSPC', 'Yahoo Finance', 'pts', ['index', 'global']),
    ('Dow_Jones', '^DJI', 'Yahoo Finance', 'pts', ['index', 'global']),
    ('NASDAQ', '^IXIC', 'Yahoo Finance', 'pts', ['index', 'global']),
    ('FTSE_100', '^FTSE', 'Yahoo Finance', 'pts', ['index', 'global']),
    ('Hang_Seng', '^HSI', 'Yahoo Finance', 'pts', ['index', 'asia']),
    ('Nikkei_225', '^N225', 'Yahoo Finance', 'pts', ['index', 'asia']),
    # Government bond yields
    ('India_10Y_Bond', '^IN10YT=RR', 'Yahoo Finance', '%', ['bond', 'india']),
    ('US_10Y_Bond', '^TNX', 'Yahoo Finance', '%', ['bond', 'us']),
    # Add RBI/MCX/GDP placeholders as required for manual/other ETL
]

def fetch_macro_data():
    pg = PostgresHook(postgres_conn_id='data_collection_pg', schema='data_collection')

    for metric, symbol, source, unit, tags in macro_metrics:
        if symbol is None:
            print(f"Skipping {metric}: no symbol/API implemented yet.")
            continue
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2d")
            if not hist.empty:
                last_row = hist.iloc[-1]
                value = float(last_row['Close'])
                timestamp = last_row.name.to_pydatetime()
                # Deduplication
                exists = pg.get_first(
                    "SELECT 1 FROM data.macro_sector WHERE metric=%s AND timestamp=%s AND source=%s",
                    parameters=(metric, timestamp, source)
                )
                if not exists:
                    pg.run(
                        """
                        INSERT INTO data.macro_sector
                        (metric, timestamp, value, unit, source, tags, extra_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        parameters=(metric, timestamp, value, unit, source, tags, '{}')
                    )
                    print(f"Inserted: {metric} | {timestamp} | {value}")
        except Exception as e:
            print(f"Error for {metric}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='collect_macro_sector_daily',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['macro', 'sector', 'market']
) as dag:
    fetch_macro = PythonOperator(
        task_id='fetch_macro_data',
        python_callable=fetch_macro_data,
    )
