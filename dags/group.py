from airflow.sdk import dag, task, task_group

@dag
def group():

    @task
    def a():
        print("Returning 42 from task A")
        return 42

    @task_group(default_args={"retries": 2, "retry_delay": 5})
    def my_group(val: int):
        @task
        def b(my_val: int):
            print(f"Returning {my_val + 42} from task B")
            return my_val + 42
        
        @task_group(default_args={"retries": 3, "retry_delay": 3})
        def my_nested_group():

            @task
            def c():
                print("Task C is running")
                return "Task C completed"
            
            @task
            def d():
                print("Task D is running")
                return "Task D completed"

            c() >> d()

        [b(val), my_nested_group()]

    @task
    def e():
        print("Task E is running")
        return "Task E completed"
    
    val = a()
    my_group(val=val) >> e()

group()