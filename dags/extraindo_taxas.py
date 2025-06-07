from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    'Executando_notebook-etl',
    start_date=datetime(2025, 5, 31),
    schedule_interval="0 9 * * *", # todos os dias às 9h
    ) as dag:
    
    extraindo_dados = DatabricksRunNowOperator(
        task_id='Extraindo-conversoes',
        databricks_conn_id='databricks_default',
        job_id=633486297256225,
        notebook_params={
            'data_execucao': '{{data_interval_end.strftime("%Y-%m-%d")}}'
        },
        execution_timeout=timedelta(seconds=1800)
    )

    transformando_dados = DatabricksRunNowOperator(
        task_id='transformando-dados',
        databricks_conn_id='databricks_default',
        job_id=819640078217644,
        execution_timeout=timedelta(seconds=1800)
    )

    enviando_relatorio = DatabricksRunNowOperator(
        task_id='enviando-relatorio',
        databricks_conn_id='databricks_default',
        job_id=376298984851302,
        execution_timeout=timedelta(seconds=1800)
    )

    extraindo_dados >> transformando_dados >> enviando_relatorio

    