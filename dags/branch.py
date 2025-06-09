from airflow.sdk import dag, task
import random

@dag
def branch_dag():

    @task
    def generate():
        # return 1
        return random.choice([0, 1, 2])

    @task.branch
    def test_1(val: int):
        if val == 1:
            return 'is_1'
        else:
            return 'isnt_1'

    @task
    def is_1(val: int):
        print(f"The value was indeed 1: {val}")

    @task
    def isnt_1(val: int):
        print(f"The value was not 1, it was {val} instead")


    generated = generate()
    test_1(generated) >> [is_1(generated), isnt_1(generated)]


branch_dag()