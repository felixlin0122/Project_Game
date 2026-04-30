from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bahamut_pipeline",
    start_date=datetime(2024, 1, 1),
    # schedule="0 6 * * *", ## (*,*,*,*,*)
    schedule = "* * * * *" ,
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

    # groq = BashOperator(
    #     task_id="GROQ",
    #     bash_command="python /opt/airflow/app/Project_GROQ_v3.py",
    # )

    # toexcel = BashOperator(
    #     task_id="excel",
    #     bash_command="python /opt/airflow/app/Project_to_excel.py",
    # )
    
    build_chunk = BashOperator(
        task_id="chunk",
        bash_command="python /opt/airflow/app/build_chunk.py",
    )

    create_embeddings = BashOperator(
        task_id="embeddings",
        bash_command="python /opt/airflow/app/index_in_qdrant.py",
    )

    upsert_vector_db = BashOperator(
        task_id="vector_db",
        bash_command="python /opt/airflow/app/Project_to_excel.py",
    )
    
    crawl >> normalize >> build_chunk >> create_embeddings >> upsert_vector_db
    # crawl >> groq >> toexcel
    # [normalize,groq] >> toexcel
=======
    groq = BashOperator(
        task_id="GROQ",
        bash_command="python /opt/airflow/app/Project_GROQ_v3.py",
    )

    toexcel = BashOperator(
        task_id="toexcel",
        bash_command="python /opt/airflow/app/Project_to_excel.py",
    )

    crawl >> normalize
    crawl >> groq
    [groq,normalize]  >> toexcel
>>>>>>> docker-compose
