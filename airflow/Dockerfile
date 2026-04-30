FROM apache/airflow:2.8.4-python3.11

USER airflow
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    lxml \
    pymysql \
    python-dotenv \
    pandas \
    groq \
    qdrant-client \
    sentence-transformers

USER airflow

