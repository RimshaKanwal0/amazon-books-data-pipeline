from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------
# CONFIG
# -------------------
POSTGRES_CONN = {
    "host": "postgres",         
    "database": "amazon_book",  
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

AMAZON_URL = "https://www.amazon.com/s?k=books"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9"
}

# -------------------
# TASK FUNCTIONS
# -------------------
def fetch_books(**kwargs):
    """Scrape book data from Amazon and push to XCom as JSON."""
    response = requests.get(AMAZON_URL, headers=HEADERS, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    books = []

    for item in soup.select(".s-result-item")[:10]:  # Limit for demo
        title_tag = item.select_one("h2")
        author_tag = item.select_one(".a-color-secondary .a-row .a-size-base")
        price_tag = item.select_one(".a-price .a-offscreen")
        link_tag = item.select_one("h2 a")

        books.append({
            "title": title_tag.get_text(strip=True) if title_tag else None,
            "author": author_tag.get_text(strip=True) if author_tag else None,
            "price": price_tag.get_text(strip=True) if price_tag else None,
            "link": f"https://www.amazon.com{link_tag['href']}" if link_tag else None
        })

    kwargs["ti"].xcom_push(key="books_json", value=json.dumps(books))


def create_table():
    """Create the target table in Postgres if it doesn't exist."""
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS amazon_books (
            id SERIAL PRIMARY KEY,
            title TEXT,
            author TEXT,
            price TEXT,
            link TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def insert_books(**kwargs):
    """Insert scraped books into Postgres."""
    books_json = kwargs["ti"].xcom_pull(key="books_json", task_ids="fetch_books")
    if not books_json:
        raise ValueError("No books data found in XCom.")
    
    books = json.loads(books_json)
    df = pd.DataFrame(books)

    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO amazon_books (title, author, price, link)
            VALUES (%s, %s, %s, %s)
        """, (row["title"], row["author"], row["price"], row["link"]))
    conn.commit()
    cur.close()
    conn.close()

# -------------------
# DAG
# -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="fetch_and_store_amazon_books_v3",
    default_args=default_args,
    start_date=datetime(2025, 8, 10),
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_books = PythonOperator(
        task_id="fetch_books",
        python_callable=fetch_books
    )

    create_books_table = PythonOperator(
        task_id="create_books_table",
        python_callable=create_table
    )

    insert_books = PythonOperator(
        task_id="insert_books",
        python_callable=insert_books
    )

    fetch_books >> create_books_table>> insert_books
