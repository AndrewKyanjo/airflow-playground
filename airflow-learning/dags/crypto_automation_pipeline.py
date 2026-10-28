import requests
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="crypto_10min_branching",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Kampala"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["crypto", "learning", "branching"],
)
def crypto_pipeline():

    @task.branch
    def check_if_table_exists():
        """Checks Postgres metadata. Returns the name of the next task to run."""
        hook = PostgresHook(postgres_conn_id="my_local_pg")

        # Query Postgres to see if our table exists in the public schema
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'crypto_prices'
            );
        """
        # get_first() returns a tuple like (True,) or (False,)
        table_exists = hook.get_first(sql)[0]

        if table_exists:
            print("Table found! Skipping creation.")
            return "fetch_prices"  # Jumps straight to fetching
        else:
            print("Table missing. Routing to creation task.")
            return "create_table"  # Routes to creation first

    @task
    def create_table():
        """Only runs if the branch task sends it here."""
        hook = PostgresHook(postgres_conn_id="my_local_pg")
        hook.run("""
            CREATE TABLE crypto_prices (
                id SERIAL PRIMARY KEY,
                fetch_time TIMESTAMP,
                coin_name VARCHAR(50),
                price_usd NUMERIC,
                volume_24h NUMERIC,
                change_24h_pct NUMERIC,
                absolute_change_usd NUMERIC
            );
        """)

    # We MUST change the trigger rule, otherwise this task would refuse to run
    # when the 'create_table' task gets skipped.
    @task(
        execution_timeout=timedelta(seconds=30),
        trigger_rule="none_failed_min_one_success",
    )
    def fetch_prices():
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
        response = requests.get(
            url,
            headers={
                "accept": "application/json",
                "x-cg-demo-api-key": "CG-cD8mBHvMRy7w1wwF9rvcXY8u",
            },
        )
        response.raise_for_status()
        return response.json()

    @task
    def validate_data(api_data: dict):
        expected_coins = ["bitcoin", "ethereum", "solana"]
        for coin in expected_coins:
            if coin not in api_data:
                raise AirflowFailException(f"Missing expected coin from API: {coin}")
            if api_data[coin].get("usd", 0) <= 0:
                raise AirflowFailException(
                    f"Invalid price detected for {coin}: {api_data[coin].get('usd')}"
                )
        return api_data

    @task
    def enrich_and_calculate(valid_data: dict):
        enriched_data = {}
        for coin, stats in valid_data.items():
            current_price = stats["usd"]
            pct_change = stats["usd_24h_change"]

            previous_price = current_price / (1 + (pct_change / 100))
            absolute_change = current_price - previous_price

            enriched_data[coin] = {
                "price_usd": current_price,
                "volume_24h": stats["usd_24h_vol"],
                "change_24h_pct": pct_change,
                "absolute_change_usd": round(absolute_change, 2),
            }
        return enriched_data

    @task
    def load_to_postgres(final_data: dict):
        hook = PostgresHook(postgres_conn_id="my_local_pg")
        current_time = pendulum.now("Africa/Kampala").isoformat()

        records_to_insert = []
        for coin, stats in final_data.items():
            records_to_insert.append(
                (
                    current_time,
                    coin,
                    stats["price_usd"],
                    stats["volume_24h"],
                    stats["change_24h_pct"],
                    stats["absolute_change_usd"],
                )
            )

        hook.insert_rows(
            table="crypto_prices",
            rows=records_to_insert,
            target_fields=[
                "fetch_time",
                "coin_name",
                "price_usd",
                "volume_24h",
                "change_24h_pct",
                "absolute_change_usd",
            ],
        )

    # initiallise the tasks
    check_task = check_if_table_exists()
    create_task = create_table()

    # DATA FLOW
    raw_data = fetch_prices()
    validated_data = validate_data(raw_data)
    ready_to_load_data = enrich_and_calculate(validated_data)
    load_to_postgres(ready_to_load_data)

    # BRANCH LOGIC
    check_task >> create_task
    check_task >> raw_data

    # converges back to raw_data
    create_task >> raw_data


crypto_pipeline()
