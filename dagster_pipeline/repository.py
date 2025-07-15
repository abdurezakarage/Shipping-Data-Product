from dagster import repository
from dagster_pipeline.pipeline import shipping_data_pipeline

@repository
def shipping_repo():
    return [shipping_data_pipeline] 