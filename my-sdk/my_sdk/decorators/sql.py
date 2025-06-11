from typing import Any, Callable, ClassVar, Collection, Mapping
from collections.abc import Sequence
import warnings
from airflow.sdk.bases.decorator import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.sdk.definitions.context import Context


class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    """
    Custom operator that combines Airflow's DecoratedOperator and SQLExecuteQueryOperator
    to enable the creation of SQL-based TaskFlow decorators.

    This operator allows a Python callable to dynamically generate SQL queries at runtime,
    which are then executed as part of the task.
    """
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers
    }

    custom_operator_name: str = "@task.sql"
    overwrite_rtif_after_execution: bool = True

    def __init__(
            self,
            *,
            python_callable: Callable,
            op_args: Collection[Any] | None = None,
            op_kwargs: Mapping[str, Any] | None = None,
            **kwargs
    ) -> None:
        """
        Initialize the SQL-decorated operator.

        Args:
            python_callable (Callable): The Python function that generates the SQL query.
            op_args (Collection[Any] | None): Positional arguments to pass to the callable.
            op_kwargs (Mapping[str, Any] | None): Keyword arguments to pass to the callable.
            **kwargs: Additional keyword arguments for the parent classes.
        """
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"'{self.custom_operator_name}' does not support multiple outputs.",
                UserWarning,
                stacklevel=3)

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs
        )

    def execute(self, context: Context) -> Any:
        """
        Executes the operator by:
        1. Merging the context with operator keyword arguments.
        2. Determining the arguments to pass to the Python callable.
        3. Calling the Python function to generate the SQL query.
        4. Validating the SQL string.
        5. Rendering templates and executing the SQL query using the parent class.

        Args:
            context (Context): The Airflow context for the task instance.

        Returns:
            Any: The result of executing the SQL query.
        """
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.sql = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The return value from the TaskFlow callable must be a non-empty string.")
        
        context["ti"].render_templates()

        return super().execute(context)
    
def sql_task(
        python_callable: Callable | None = None,
        **kwargs
) -> TaskDecorator:
    """
    Decorator to create a SQL task that executes a Python callable to generate SQL queries.

    Args:
        python_callable (Callable): The Python function that generates the SQL query.
        op_args (Collection[Any] | None): Positional arguments to pass to the callable.
        op_kwargs (Mapping[str, Any] | None): Keyword arguments to pass to the callable.
        **kwargs: Additional keyword arguments for the operator.

    Returns:
        _SQLDecoratedOperator: An instance of the custom SQL-decorated operator.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SQLDecoratedOperator,
        **kwargs
    )