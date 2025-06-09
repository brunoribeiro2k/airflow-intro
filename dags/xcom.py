from airflow.sdk import dag, task, Context
from typing import Any, Dict

@dag
def xcom_dag():

    @task
    def t1() -> Dict[str, Any]:
        my_val = 42
        my_sentence = "Hello, World!"
        return {'my_val': my_val, 'my_sentence': my_sentence}

    @task
    def t2(data: Dict[str, Any]) -> str:
        my_val = data['my_val']
        my_sentence = data['my_sentence']
        my_val += 10
        my_sentence += " - Updated in T2"
        print(f"T2: {my_sentence} with value {my_val}")
        return {'my_val': my_val, 'my_sentence': my_sentence}

    val = t1()
    t2(val)

xcom_dag()