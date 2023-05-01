from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": String},
    op_tags={"kind": "s3"},
    description="Get stock data from an S3 bucket."
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(s3_key)
    stocks = list(map(Stock.from_list, data))
    return stocks


@asset(
    description="Selects date with the highest value from a list of stocks.",
)
def process_data(context: OpExecutionContext, get_s3_data: List[Stock]):
    highest = max(get_s3_data, key=lambda x: x.high)
    aggregation = Aggregation(date=highest.date, high=highest.high)

    return aggregation


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    description="Writes aggregation to Redis.",
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation):
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


@asset(
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    description="Writes aggregation to S3."
)
def put_s3_data(context: OpExecutionContext, process_data: Aggregation):
    context.resources.s3.put_data(str(process_data.date), process_data)


project_assets = load_assets_from_current_module(
    group_name="pipeline",
)


machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    selection=AssetSelection.groups("pipeline"),
    config={
         "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}
    }
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
