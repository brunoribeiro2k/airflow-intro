from airflow.sdk import asset, Asset, Context
import requests

@asset(
    schedule="@daily",
    uri="https://randomuser.me/api/",
)
def user(self) -> dict[str]:
    response = requests.get(self.uri, verify=False)
    return response.json()


@asset.multi(
    schedule=user,
    outlets=[
        Asset(name="user_location"),
        Asset(name="user_login")
    ]
)
def user_info(user: Asset, context: Context) -> list[dict[str]]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]

