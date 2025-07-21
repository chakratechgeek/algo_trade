from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow', 'start_date': datetime(2025,7,19), 'retries': 0
}

with DAG(
    'init_data_collection',
    default_args=default_args,
    schedule_interval=None, catchup=False
) as dag:

    create_db = PostgresOperator(
        task_id='01_create_database',
        postgres_conn_id='data_collection_pg',
        sql='sql/01_create_database.sql'
    )

    create_schema = PostgresOperator(
        task_id='02_create_schema',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/02_create_schema.sql'
    )
    create_schema2 = PostgresOperator(
        task_id='02.1_create_schema',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/02.1_create_schema.sql'
    )

    create_tables = PostgresOperator(
        task_id='03_create_tracker_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/03_create_tracker_table.sql'
    )
    create_symbols_table = PostgresOperator(
        task_id='04_create_symbols_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/04_create_symbols_table.sql'
    )
    
    insert_nifty50_symbols = PostgresOperator(
        task_id='05_insert_nifty50_symbols',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/05_insert_nifty50_symbols.sql'
    )
    create_news_table = PostgresOperator(
        task_id='06_create_news_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/06_create_news_table.sql'
    )
    
    create_corporate_action_table = PostgresOperator(
        task_id='07_corporate_actions_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/07_corporate_actions_table.sql'
    )
    
    fundamentals_table = PostgresOperator(
        task_id='08_fundamentals_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/08_fundamentals_table.sql'
    )
    
    macro_sector_table = PostgresOperator(
        task_id='09_macro_sector_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/09_macro_sector_table.sql'
    )
    
    ohlcv_table = PostgresOperator(
        task_id='10_ohlcv_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/10_ohlcv_table.sql'
    )

    regulatory_events_table = PostgresOperator(
        task_id='11_regulatory_events_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/11_regulatory_events_table.sql'
    )

    history_ohlcv_table = PostgresOperator(
        task_id='12_6months_history_ohlcv_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/12_6months_history_ohlcv_table.sql'
    )
    
    orderbook_levels_table = PostgresOperator(
        task_id='13_orderbook_levels_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/13_orderbook_levels_table.sql'
    )

    trade_ticks_table = PostgresOperator(
        task_id='14_trade_ticks_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/14_trade_ticks_table.sql'
    )

    mutual_fund_flows_table = PostgresOperator(
        task_id='15_mutual_fund_flows_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/15_mutual_fund_flow_table.sql'
    )
    risk_events_table = PostgresOperator(
        task_id='16_risk_events_table',
        postgres_conn_id='data_collection_pg',
        database='data_collection',
        sql='sql/16_risk_events_table.sql'
    )

    create_db >> create_schema >> create_schema2 >> create_tables >> create_symbols_table >> insert_nifty50_symbols >> create_news_table >> create_corporate_action_table >>fundamentals_table >> macro_sector_table >> ohlcv_table >> regulatory_events_table >> history_ohlcv_table >> orderbook_levels_table >> trade_ticks_table >> mutual_fund_flows_table >> risk_events_table
