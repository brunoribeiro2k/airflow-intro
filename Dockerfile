FROM apache/airflow:3.0.0

COPY my-sdk /opt/airflow/my-sdk

RUN pip install /opt/airflow/my-sdk