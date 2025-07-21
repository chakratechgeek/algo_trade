import re
import feedparser
import email.utils as eut
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_and_store_news():
    # Connect to Postgres
    pg = PostgresHook(postgres_conn_id='data_collection_pg', schema='data_collection')

    # Fetch active companies
    companies = pg.get_records(
        "SELECT symbol_id, symbol, name FROM data.symbols WHERE active = TRUE"
    )

    # Time window: last 1 hour
    now = datetime.utcnow()
    one_hr_ago = now - timedelta(hours=1)

    for symbol_id, symbol, name in companies:
        query = name.replace(" ", "+")
        rss_url = f"https://news.google.com/rss/search?q={query}+stock"

        try:
            feed = feedparser.parse(rss_url)
        except Exception as e:
            print(f"Failed to fetch RSS for {name}: {e}")
            continue

        for entry in feed.entries:
            try:
                published_dt = datetime(*eut.parsedate(entry.published)[:6])
            except Exception:
                continue

            # Skip old news
            if published_dt < one_hr_ago:
                continue

            # Extract fields and ensure strings
            headline = str(entry.get("title", ""))
            summary = str(getattr(entry, "summary", ""))
            url = str(entry.get("link", ""))
            category = "stock-news"

            source_field = entry.get("source", {})
            if isinstance(source_field, dict):
                source = source_field.get("title") or source_field.get("href") or "Google News"
            else:
                source = str(source_field or "Google News")

            # Deduplicate
            exists = pg.get_first(
                """
                SELECT 1
                  FROM data.news
                 WHERE symbol_id = %s
                   AND headline = %s
                   AND timestamp = %s
                """,
                parameters=(symbol_id, headline, published_dt),
            )
            if exists:
                continue

            # Insert new article
            try:
                pg.run(
                    """
                    INSERT INTO data.news
                      (symbol_id, timestamp, headline, body,
                       sentiment, sentiment_score,
                       news_type, source, url, extra_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    parameters=(
                        symbol_id,
                        published_dt,
                        headline,
                        summary,
                        None,      # sentiment
                        None,      # sentiment_score
                        category,
                        source,
                        url,
                        '{}'       # extra_data JSONB
                    ),
                )
                print(f"Inserted news: {headline}")
            except Exception as e:
                print(f"Error inserting news for {name}: {e}")


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='collect_news_google_hourly',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['news', 'google', 'india']
) as dag:
    fetch_and_store = PythonOperator(
        task_id='fetch_and_store_news',
        python_callable=fetch_and_store_news,
    )
