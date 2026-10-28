import requests
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="crypto_10min_pipeline",
    # Cron expression for "every 10 minutes"
    schedule="*/10 * * * *", 
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Kampala"),
    catchup=False,
    # If the whole pipeline takes longer than 5 mins, something is wrong. Kill it.
    dagrun_timeout=timedelta(minutes=5), 
    tags=["crypto", "learning", "api"],
)
def crypto_pipeline():

    @task
    def create_table():
        """Creates the destination table if it doesn't exist yet."""
        # The hook grabs the credentials from the UI connection we made
        hook = PostgresHook(postgres_conn_id="my_local_pg")
        
        # Execute raw SQL
        hook.run("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                fetch_time TIMESTAMP,
                coin_name VARCHAR(50),
                price_usd NUMERIC,
                volume_24h NUMERIC,
                change_24h NUMERIC
            );
        """)

    @task(execution_timeout=timedelta(seconds=30))
    def fetch_prices():
        """Pulls the latest data from CoinGecko. Kills the task if it takes >30 seconds."""
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
        
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": "CG-cD8mBHvMRy7w1wwF9rvcXY8u"  
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Fails the task if the API returns a 404 or 500 error
        
        return response.json()

    @task
    def load_to_postgres(api_data: dict):
        """Parses the JSON and loads it into the database."""
        hook = PostgresHook(postgres_conn_id="my_local_pg")
        current_time = pendulum.now("Africa/Kampala").isoformat()
        
        records_to_insert = []
        
        # Loop through the JSON response and extract the metrics
        for coin, stats in api_data.items():
            records_to_insert.append((
                current_time,
                coin,
                stats.get("usd"),
                stats.get("usd_24h_vol"),
                stats.get("usd_24h_change")
            ))
            
        # insert_rows is a built-in Hook method that handles the SQL INSERT statement safely
        hook.insert_rows(
            table="crypto_prices",
            rows=records_to_insert,
            target_fields=["fetch_time", "coin_name", "price_usd", "volume_24h", "change_24h"]
        )

    # 1. Define the tasks
    setup_db = create_table()
    raw_data = fetch_prices()
    
    # 2. Define the dependencies
    # We pass raw_data into load_to_postgres, which links them automatically.
    # But we also need to explicitly tell load_to_postgres to wait for setup_db to finish first.
    setup_db >> load_to_postgres(raw_data)

crypto_pipeline()