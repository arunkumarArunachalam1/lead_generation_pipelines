from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os
import requests
from requests.auth import HTTPBasicAuth

DB_DSN = os.getenv("DB_DSN", "host=localhost dbname=Lead_generation user=postgres password=root")
HOP_URL = "http://localhost:8080/hop/asyncRun/?service=Immediate_Mail&runConfig=local"


def fetch_configs():
    """Fetch DAG configs from the database"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        SELECT dag_id, schedule, categories, content, record_limit, email_to, source
        FROM dag_configs
        WHERE is_active = TRUE
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def call_hop_api(**kwargs):
    categories = kwargs.get("categories")
    content = kwargs.get("content")
    record_limit = kwargs.get("record_limit")
    email_to = kwargs.get("email_to")
    source = kwargs.get("source")

    # Add current timestamp
    # current_timestamp = datetime.now().isoformat()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("-----------------------------------------------------------")
    print(f"----------------{current_timestamp}------------------------")
    print("-----------------------------------------------------------")

    payload = {
        "Categories": categories,
        "Content": content,
        "Limit": record_limit,
        "TO": email_to,
        "source": source,
        "Timestamp": current_timestamp,   # ✅ Add timestamp here
    }

    try:
        resp = requests.post(
            HOP_URL,
            data=payload,
            auth=HTTPBasicAuth("cluster", "cluster"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60
        )
        if resp.status_code == 200:
            print(f"✅ Hop workflow triggered successfully at {current_timestamp}")
        else:
            raise Exception(f"❌ Failed to trigger Hop workflow: {resp.status_code} {resp.text}")

    except Exception as e:
        raise Exception(f"🚨 Error calling Hop API: {str(e)}")


def build_dag(dag_id, schedule, categories, content, record_limit, email_to, source):
    """Builds a DAG object dynamically"""
    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
        tags=["dynamic", "hop"],
    ) as dag:

        PythonOperator(
            task_id="trigger_hop_mail",
            python_callable=call_hop_api,
            op_kwargs={
                "categories": categories,
                "content": content,          # ✅ function arg
                "record_limit": record_limit,
                "email_to": email_to,
                "source": source,
            },
        )

    return dag





# ---- Generate DAGs from configs ----
for row in fetch_configs():
    dag_id, schedule, categories, content, limit, email_to, source = row
    globals()[dag_id] = build_dag(
        dag_id, schedule, categories, content, limit, email_to, source
    )

