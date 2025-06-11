from airflow.sdk import dag, task

@dag
def sql_dag():

    @task.sql(
        conn_id="postgres" # from SQLExecuteQueryOperator
    )
    def get_nb_xcoms():
        """
        Returns the number of XComs in the database.
        """
        return "SELECT COUNT(*) FROM xcom"
    
    get_nb_xcoms()

sql_dag()