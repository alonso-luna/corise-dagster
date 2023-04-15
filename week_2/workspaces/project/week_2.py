from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get list of stocks from an S3 file."
)
def get_s3_data(context: OpExecutionContext):
    result = list()
    for row in context.resources.s3.get_data(key_name=context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        result.append(stock)
    return result


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="List of stocks return the Aggregation with the greatest high",
)
def process_data(context, stocks: List[Stock]):
    max_attr = max(stocks, key=lambda x:x.high)

    return Aggregation(date=max_attr.date, high=max_attr.high)


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Upload Aggregation to Redis."
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    context.resources.redis.put_data(name=str(aggregation.date), value=str(aggregation.high))
    


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Upload Aggregation to S3."
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):

    datefile=datetime.today().strftime('%m_%d_%Y')
    s3_key=f'/aggregation_{datefile}.csv'

    context.resources.s3.put_data(key_name=s3_key, data=aggregation)


@graph
def machine_learning_graph():
    highest_data = process_data(get_s3_data())
    put_redis_data(highest_data)
    put_s3_data(highest_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)
