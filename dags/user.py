from airflow.sdk import asset, Asset, Context
import requests

@asset(
    schedule="@daily",
    description="A user asset that represents a user in the system.",
    uri="https://randomuser.me/api/",
)
def user(self) -> dict[str]:
    """
    This asset represents a user in the system.
    """
    response = requests.get(self.uri, verify=False)
    return response.json()


@asset(
    schedule=user
)
def user_location(user: Asset, context: Context) -> dict[str]:
    """
    This asset represents the location of a user in the system.
    """
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return user_data['results'][0]['location'] if user_data and 'results' in user_data else {}


@asset(
    schedule=user
)
def user_login(user: Asset, context: Context) -> dict[str]:
    """
    This asset represents the login information of a user in the system.
    """
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return user_data['results'][0]['login'] if user_data and 'results' in user_data else {}