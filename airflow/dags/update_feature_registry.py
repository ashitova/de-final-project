from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Import common utilities
from utils.common import POSTGRES_CONN, REGISTRY_TABLE_NAME, REGISTRY_SELECT_QUERY, POSTGRES_SCHEMA_REGISTRY, output_as_dict

INSERT_QUERY = """INSERT INTO {} ("name", "type", "owner", "source", "description", "version", "lifetime", created) VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}', now());"""

def form_registry_record(ti=None, **kwargs):
    query_result = ti.xcom_pull(task_ids="get_registry_record")
    new_version = kwargs['dag_run'].conf['feature_version']
    insert_query = INSERT_QUERY.format(REGISTRY_TABLE_NAME, query_result.get("name"), query_result.get("type"), query_result.get("owner"), query_result.get("source"), \
                                                    query_result.get("description"), new_version, query_result.get("lifetime"))
    return insert_query

with DAG(
    dag_id="update_feature_registry",
    params={"feature_name": '', "feature_version": ''},
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="DAG to update feature registry records",
    tags=["feature_registry", "export"]
) as dag:
    get_registry_record = SQLExecuteQueryOperator(
            task_id='get_registry_record',
            conn_id=POSTGRES_CONN,
            hook_params={"schema": POSTGRES_SCHEMA_REGISTRY, "enable_log_db_messages": True}, 
            sql=REGISTRY_SELECT_QUERY,
            show_return_value_in_logs=True,
            parameters={"name": '{{ dag_run.conf["feature_name"] if dag_run else "undefined" }}'},
            output_processor=output_as_dict,
            do_xcom_push=True,
        )
    generate_registry_record = PythonOperator(
            task_id='generate_registry_record', 
            python_callable=form_registry_record,
            provide_context=True,
        )
    update_registry = SQLExecuteQueryOperator(
            task_id='update_registry',
            conn_id='postgres_conn',
            hook_params={"schema": POSTGRES_SCHEMA_REGISTRY, "enable_log_db_messages": True}, 
            sql="{{ ti.xcom_pull(task_ids='generate_registry_record') }}",
            show_return_value_in_logs=True,
        )

        
get_registry_record >> generate_registry_record >> update_registry
    