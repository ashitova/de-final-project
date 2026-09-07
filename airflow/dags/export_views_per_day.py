from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Import common utilities
from utils.common import POSTGRES_CONN, CLICKHOUSE_CONN, MINIO_CONN, REGISTRY_SELECT_QUERY,POSTGRES_SCHEMA_PUBLIC, POSTGRES_SCHEMA_REGISTRY, convert_to_parquet, transfer_to_minio, output_as_dict, check_connection, check_connection_minio, create_bucket_if_not_exists

FEATURE_NAME = "views_per_day"

SELECT_QUERY="""
SELECT DATE_TRUNC('day', visit_datetime) AS visit_date
, SUM(page_views ) AS sum_page_views
FROM web_analytics 
GROUP BY visit_date
ORDER BY visit_date
;"""

CLICKHOUSE_TABLE = FEATURE_NAME
INSERT_QUERY = "INSERT INTO {} (id, visit_date, sum_page_views, created_at, feature_version) VALUES({}, '{}', {}, now(), '{}');"


def query_result_to_parquet(ti=None, **kwargs):
    query_results = ti.xcom_pull(task_ids="run_query")
    return convert_to_parquet(query_results)

def parquet_to_minio(ti=None, **kwargs):
    source_path = ti.xcom_pull(task_ids="query_result_to_parquet")
    feature_version = ti.xcom_pull(task_ids="get_registry_record").get("version")
    transfer_to_minio(source_path, FEATURE_NAME, feature_version)

def form_insert_statement(ti=None, **kwargs):
    query_results = ti.xcom_pull(task_ids="run_query")
    feature_version = ti.xcom_pull(task_ids="get_registry_record").get("version")
    insert_query = []
    i = 1
    for q in query_results:
        st = INSERT_QUERY.format(CLICKHOUSE_TABLE, i, q.get('visit_date').date(), q.get('sum_page_views'), feature_version)
        i += 1
        insert_query.append(st)
    return " ".join(insert_query)

with DAG(
    dag_id=f"export_{FEATURE_NAME}",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=f"DAG to regularly update feature {FEATURE_NAME}",
    tags=["feature", "export", FEATURE_NAME]
) as dag:
    check_postgres = PythonOperator(
        task_id="check_postgres",
        python_callable=check_connection,
        op_kwargs={"conn_id": POSTGRES_CONN},
    )
    check_clickhouse = PythonOperator(
        task_id="check_clickhouse",
        python_callable=check_connection,
        op_kwargs={"conn_id": CLICKHOUSE_CONN},
    )
    check_minio = PythonOperator(
        task_id="check_minio",
        python_callable=check_connection_minio,
        op_kwargs={"conn_id": MINIO_CONN},
    )
    check_minio_bucket = PythonOperator(
        task_id="check_minio_bucket",
        python_callable=create_bucket_if_not_exists,
    )
    get_registry_record = SQLExecuteQueryOperator(
        task_id='get_registry_record',
        conn_id=POSTGRES_CONN,
        hook_params={"schema": POSTGRES_SCHEMA_REGISTRY, "enable_log_db_messages": True}, 
        sql=REGISTRY_SELECT_QUERY,
        show_return_value_in_logs=True,
        parameters={"name": FEATURE_NAME},
        output_processor=output_as_dict,
        do_xcom_push=True,
    )
    run_query = SQLExecuteQueryOperator(
        task_id='run_query',
        conn_id=POSTGRES_CONN,
        hook_params={"schema": POSTGRES_SCHEMA_PUBLIC}, 
        sql=SELECT_QUERY,
        do_xcom_push=True,
        return_last=False,
        show_return_value_in_logs=True,
        output_processor=output_as_dict
    )
    query_result_to_parquet = PythonOperator(
        task_id='query_result_to_parquet', 
        python_callable=query_result_to_parquet,
        provide_context=True,
    )
    parquet_to_minio = PythonOperator(
        task_id='parquet_to_minio', 
        python_callable=parquet_to_minio,
        provide_context=True,
        )
    clear_clickhouse_data = SQLExecuteQueryOperator(
        task_id='clear_clickhouse_data',
        conn_id=CLICKHOUSE_CONN,
        sql=f"TRUNCATE TABLE {CLICKHOUSE_TABLE}",
        show_return_value_in_logs=True,
    )
    generate_insert_sql = PythonOperator(
        task_id='generate_insert_sql', 
        python_callable=form_insert_statement,
        provide_context=True,
    )
    update_clickhouse_feature = SQLExecuteQueryOperator(
            task_id='update_clickhouse_feature',
            conn_id=CLICKHOUSE_CONN,
            split_statements=True, 
            sql="{{ ti.xcom_pull(task_ids='generate_insert_sql') }}",
            show_return_value_in_logs=True,
    )        

check_postgres >> check_clickhouse >> check_minio >> check_minio_bucket >> get_registry_record
get_registry_record >> run_query >> query_result_to_parquet >> parquet_to_minio >> generate_insert_sql
generate_insert_sql >> clear_clickhouse_data >> update_clickhouse_feature
    