import logging
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

# Shared default arguments for DAGs
BUCKET_NAME='web-analytics'
DATETIME_FORMAT = "%Y%m%d_%H%M%S_%f"
DATE_FORMAT = "%Y%m%d"
MINIO_CONN = "minio_conn"
CLICKHOUSE_CONN = "clickhouse_conn"
POSTGRES_CONN = "postgres_conn"
POSTGRES_SCHEMA_PUBLIC = "public"
POSTGRES_SCHEMA_REGISTRY = "registry"
REGISTRY_TABLE_NAME = POSTGRES_SCHEMA_REGISTRY + ".feature"

REGISTRY_SELECT_QUERY = f"""
SELECT *
FROM {REGISTRY_TABLE_NAME}
WHERE "name" = %(name)s
ORDER BY created DESC
LIMIT 1;
"""

logger = logging.getLogger("airflow.task")

def check_connection(conn_id: str):
    hook = BaseHook.get_hook(conn_id=conn_id)
    # test_connection returns a tuple: (status_bool, message_str)
    success, message = hook.test_connection()
    if not success:
        raise RuntimeError(f"Connection {conn_id} failed: {message}")
    logger.info(f"Connection {conn_id} is healthy: {message}")


def check_connection_minio(conn_id: str):
    s3_hook = S3Hook(aws_conn_id=conn_id)
    buckets = s3_hook.get_conn().list_buckets()
    status_code = buckets.get("ResponseMetadata").get("HTTPStatusCode")
    if status_code != 200:
        raise RuntimeError(f"Connection {conn_id} failed, status code: {status_code}")
    logger.info(f"Connection {conn_id} is healthy")


def create_bucket_if_not_exists():
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN)
    if not s3_hook.check_for_bucket(BUCKET_NAME):
        # Create the bucket
        s3_hook.create_bucket(
            bucket_name=BUCKET_NAME,
        )
        logger.info(f"Bucket {BUCKET_NAME} is created")


# Convert select query result to a dict
def output_as_dict(results, descriptions):
    """
    results: List of result sets (due to potential split_statements)
    descriptions: List of cursor descriptions containing column metadata
    """
    # Extract column names from the first statement's description
    columns = [col[0] for col in descriptions[0]]
    if isinstance(results[0], list):
        results = results[0]
    # Map each row tuple into a dictionary
    return [dict(zip(columns, row)) for row in results]


def convert_to_parquet(query_results):
    df = pd.DataFrame(query_results)
    # Save to Parquet format temporarily before transfer
    output_path = "/tmp/output_data_" + datetime.now().strftime(DATETIME_FORMAT) + ".parquet"
    df.to_parquet(output_path, engine="pyarrow")
    return output_path


def transfer_to_minio(source_path, feature_name, feature_version):
    now = datetime.now()
    # destination path: feature_name/version/date/filename
    # example: visits_views_per_day/v1/20260905/visits_views_per_day_v1_20260905_1111_123.parquet
    filename = f"{feature_name}_v{feature_version}_{now.strftime(DATETIME_FORMAT)}.parquet"
    dest_path = f"{feature_name}/v{feature_version}/{now.strftime(DATE_FORMAT)}/{filename}"
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN)
    s3_hook.load_file(
                filename=source_path,
                key=dest_path,
                bucket_name=BUCKET_NAME,
                replace=True
            )

