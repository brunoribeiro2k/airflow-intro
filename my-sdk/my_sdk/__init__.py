from typing import Any

__version__ = "0.0.1"
__all__ = ["sql"]

def get_provider_info() -> dict[str, Any]:
    """
    Returns information about the provider.

    :return: A dictionary containing provider information.
    """
    return {
        "package-name": "my-sdk",
        "name": "My SDK",
        "description": "A sample SDK for demonstration purposes.",
        "version": [__version__],
        "task-decorators": [
            {
                "name": "sql,"
                "class-name": "my_sdk.decorators.sql.sql_task",
            }
        ]
        "author": "Bruno Ribeiro"
    }