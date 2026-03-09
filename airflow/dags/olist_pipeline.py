from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta


def notify_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    print(f"""
    ⚠️ FALHA NO PIPELINE
    DAG: {dag_id}
    Task: {task_id}
    Data de execução: {execution_date}
    Logs: {log_url}
    """)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id='olist_pipeline',
    description='Pipeline completo do Olist: ingestão + dbt run + dbt test',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['olist', 'dbt', 'analytics'],
) as dag:

    ingestao = BashOperator(
        task_id='ingestao_csv_duckdb',
        bash_command='cd /opt/airflow && python ingestion/load_to_duckdb.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/olist_analytics && dbt run --profiles-dir /opt/airflow/olist_analytics',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/olist_analytics && dbt test --profiles-dir /opt/airflow/olist_analytics',
    )

    ingestao >> dbt_run >> dbt_test