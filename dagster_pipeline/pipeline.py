from dagster import op, job
import subprocess

@op
def scrape_telegram_data():
    # Replace with actual scraping logic or script call
    subprocess.run(["python", "src/telegram_scrapper.py"], check=True)
    return "scraped"

@op
def load_raw_to_postgres(scrape_result):
    # Replace with actual loading logic or script call
    subprocess.run(["python", "src/load_raw_data.py"], check=True)
    return "loaded"

@op
def run_dbt_transformations(load_result):
    # Run dbt transformations
    subprocess.run(["dbt", "run"], cwd="Shipping-Data-Product/dbt_project", check=True)
    return "dbt_done"

@op
def run_yolo_enrichment(dbt_result):
    # Replace with actual YOLO enrichment logic or script call
    subprocess.run(["python", "src/loadYOLO.py"], check=True)
    return "yolo_done"

@job
def shipping_data_pipeline():
    telegram = scrape_telegram_data()
    raw = load_raw_to_postgres(telegram)
    dbt = run_dbt_transformations(raw)
    run_yolo_enrichment(dbt) 