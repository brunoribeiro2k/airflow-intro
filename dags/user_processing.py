from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.sensor import PokeReturnValue
import requests



@dag
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get('https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json')
        print(response.status_code)
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=response.json())

    @task
    def extract_user(user: dict) -> dict:
        return {
            "id": user['id'],
            "firstname": user['personalInfo']['firstName'],
            "lastname": user['personalInfo']['lastName'],
            "email": user['personalInfo']['email']
        }

    @task
    def process_user(user: dict):
        user["created_at"] = datetime.now().isoformat()
        import csv
        with open('/tmp/user_info.csv', mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=user.keys())
            writer.writeheader()
            writer.writerow(user)


    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )

    process_user(extract_user(create_table >> is_api_available())) >> store_user()

user_processing()