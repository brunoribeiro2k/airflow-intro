from airflow.sdk import dag, task
from time import sleep

@dag
def celery_dag():
    @task
    def t1():
        print("Task 1 is running")
        sleep(5)
        return "Task 1 completed"

    @task
    def t2():
        print("Task 2 is running")
        sleep(5)
        return "Task 2 completed"

    @task
    def t3():
        print("Task 3 is running")
        sleep(5)
        return "Task 3 completed"
    
    @task
    def t4():
        print("Task 4 is running")
        sleep(5)
        return "Task 4 completed"

    t1() >> [t2(), t3()] >> t4()

celery_dag()