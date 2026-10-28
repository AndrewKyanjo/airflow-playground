from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="first_sales_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Kampala"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["learning"],
)
def first_sales_pipeline():

    @task
    def extract():
        return [
            {"product": "Notebook", "amount_ugx": 12000},
            {"product": "Pen", "amount_ugx": 3000},
            {"product": "Invalid entry", "amount_ugx": -500},
        ]

    @task
    def clean(rows):
        return [row for row in rows if row["amount_ugx"] > 0]

    @task
    def summarize(rows):
        return {
            "valid_sales": len(rows),
            "total_ugx": sum(row["amount_ugx"] for row in rows),
        }

    @task
    def report(summary):
        print(f"Valid sales: {summary['valid_sales']}")
        print(f"Total sales: UGX {summary['total_ugx']:,}")

    raw_rows = extract()
    clean_rows = clean(raw_rows)
    summary = summarize(clean_rows)
    report(summary)


first_sales_pipeline()
