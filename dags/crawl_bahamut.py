from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bahamut_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None, ## (*,*,*,*,*)
    catchup=False,
) as dag:

    crawl = BashOperator(
        task_id="crawl",
        bash_command="python /opt/airflow/app/Project_storge.py",
    )

    normalize = BashOperator(
        task_id="normalize",
        bash_command="python /opt/airflow/app/Project_normalize.py",
    )

    groq = BashOperator(
        task_id="GROQ",
        bash_command="python /opt/airflow/app/Project_GROQ.py"
    )

    crawl >> normalize
    crawl >> groq
