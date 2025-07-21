import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_and_store_fundamentals():
    pg = PostgresHook(postgres_conn_id='data_collection_pg', schema='data_collection')
    # Adapt if your schema/table differs!
    companies = pg.get_records("SELECT symbol_id, symbol FROM data.symbols WHERE active = TRUE")

    for symbol_id, symbol in companies:
        yahoo_symbol = symbol if symbol.endswith('.NS') else symbol + '.NS'  # Ensure NSE symbol
        ticker = yf.Ticker(yahoo_symbol)

        # Get quarterly and annual financials
        data_types = [
            ('quarter', ticker.quarterly_financials, 'income'),
            ('annual', ticker.financials, 'income'),
            ('quarter', ticker.quarterly_balance_sheet, 'balance'),
            ('annual', ticker.balance_sheet, 'balance'),
            ('quarter', ticker.quarterly_cashflow, 'cashflow'),
            ('annual', ticker.cashflow, 'cashflow'),
        ]

        for period_type, df, statement_type in data_types:
            if df is None or df.empty:
                continue
            df = df.transpose()
            for idx, row in df.iterrows():
                period = pd.to_datetime(idx).date()
                period_label = idx if isinstance(idx, str) else idx.strftime("%Y-%m-%d")
                for metric, value in row.items():
                    if pd.isna(value):
                        continue
                    # Deduplication: symbol_id, period, metric, source
                    exists = pg.get_first(
                        """
                        SELECT 1 FROM data.fundamentals
                         WHERE symbol_id=%s AND period=%s AND metric=%s AND source=%s
                        """,
                        parameters=(symbol_id, period, metric, 'Yahoo Finance')
                    )
                    if exists:
                        continue

                    pg.run(
                        """
                        INSERT INTO data.fundamentals
                          (symbol_id, period, period_label, statement_type, metric, value,
                           period_type, currency, source, extra_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        parameters=(
                            symbol_id, period, period_label, statement_type, metric, float(value),
                            period_type, 'INR', 'Yahoo Finance', '{}'
                        )
                    )
                    print(f"Inserted: {symbol} | {statement_type} | {metric} | {period}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='collect_fundamentals_yahoo',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['fundamentals', 'india', 'yahoo']
) as dag:
    fetch_fundamentals = PythonOperator(
        task_id='fetch_and_store_fundamentals',
        python_callable=fetch_and_store_fundamentals,
    )
