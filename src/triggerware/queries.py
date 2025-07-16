from abc import ABC, abstractmethod
from sys import exception
from typing import Any, Callable, TYPE_CHECKING

from triggerware.jrpc import JsonRpcException, ServerErrorException
from triggerware.types import InvalidQueryException, TriggerwareClientException

if TYPE_CHECKING:
    import triggerware as tw
    import triggerware.jrpc as jrpc

from triggerware.interfaces import Query, TriggerwareObject, ResourceRestricted


class QueryRestriction(ResourceRestricted):
    """
    A simple query restriction, supporting row limits and time limits.
    """

    def __init__(self, row_limit: int | None = None, timeout: float | None = None):
        self.row_limit = row_limit
        self.timeout = timeout


class FolQuery(Query):
    """
    A query written in FOL for execution on the Triggerware server.
    """

    def __init__(self, query: str, schema: str = "AP5", /):
        self.query = query
        self.language = "fol"
        self.schema = schema


class SqlQuery(Query):
    """
    A query written in SQL for execution on the Triggerware server.
    """

    def __init__(self, query: str, schema: str = "AP5", /):
        self.query = query
        self.language = "sql"
        self.schema = schema


class AbstractQuery[T](Query, ResourceRestricted, TriggerwareObject):
    base_parameters: dict[str, Any]

    def __init__(
        self,
        client: "tw.TriggerwareClient",
        query: "tw.Query",
        restriction: "tw.ResourceRestricted | None" = None,
        /,
    ):
        self.client = client
        self.query = query.query
        self.language = query.language
        self.schema = query.schema
        self.base_parameters = {
            "query": self.query,
            "language": self.language,
            "namespace": self.schema,
        }

        if restriction is not None:
            self.row_limit = restriction.row_limit
            self.timeout = restriction.timeout
        if self.row_limit is not None:
            self.base_parameters["limit"] = self.row_limit
        if self.timeout is not None:
            self.base_parameters["timelimit"] = self.timeout


class View[T](AbstractQuery[T]):
    """
    A simple reusable view of a query. May be executed any number of times. Unlike other queries,
    A View's "handle" is always None - it is only stored locally.
    """

    def __init__(
        self,
        client: "tw.TriggerwareClient",
        query: "tw.Query",
        restriction: "tw.ResourceRestricted | None" = None,
        /,
    ):
        """
        Initialize a View instance.

        Args:
            client (tw.TriggerwareClient): The Triggerware client.
            query (tw.Query): The query to execute.
            restriction (tw.ResourceRestricted | None, optional): Optional restrictions to apply.
        """
        super().__init__(client, query, restriction)
        self.client.validate_query(self)

    def execute(
        self, restriction: "tw.ResourceRestricted | None" = None
    ) -> "tw.ResultSet[T]":
        """
        Executes the query on the connected triggerware server.

        Args:
            restriction: Optional restrictions to apply to the query.

        Returns:
            tw.ResultSet[T]: A result set from the server.
        """
        from triggerware.result_set import ResultSet

        params = self.base_parameters.copy()
        if restriction is not None:
            if restriction.row_limit is not None:
                params["limit"] = restriction.row_limit
            if restriction.timeout is not None:
                params["timelimit"] = restriction.timeout

        eq_result = None
        try:
            eq_result = self.client.json_rpc.call("execute-query", params)
        except ServerErrorException as e:
            raise e
        except JsonRpcException as e:
            raise InvalidQueryException(e.message)
        return ResultSet(self.client, eq_result)


class PreparedQuery[T](AbstractQuery[T]):
    prepared_params: list[Any]
    input_signature_names: list[str]
    input_signature_types: list[str]
    uses_named_params: bool

    _TYPE_MAP: dict[str, Callable[[str], bool]] = {
        "double": lambda x: isinstance(x, float),
        "integer": lambda x: isinstance(x, int),
        "number": lambda x: isinstance(x, (int, float)),
        "boolean": lambda x: isinstance(x, bool),
        "stringcase": lambda x: isinstance(x, str),
        "stringnocase": lambda x: isinstance(x, str),
        "stringagnostic": lambda x: isinstance(x, str),
        "date": lambda x: isinstance(x, str),
        "time": lambda x: isinstance(x, str),
        "timestamp": lambda x: isinstance(x, str),
        "interval": lambda x: isinstance(x, str),
    }

    def __init__(
        self,
        client: "tw.TriggerwareClient",
        query: "tw.Query",
        restriction: "tw.ResourceRestricted | None" = None,
        /,
    ):
        super().__init__(client, query, restriction)
        registration = self.client.json_rpc.call("prepare-query", self.base_parameters)
        self.prepared_params = [None] * len(registration["inputSignature"])
        self.handle = registration["handle"]
        self.uses_named_params = registration["usesNamedParameters"]
        self.input_signature_names = list(
            map(lambda x: x["attribute"], registration["inputSignature"])
        )
        self.input_signature_types = list(
            map(lambda x: x["type"], registration["inputSignature"])
        )
        if self.handle is not None:
            self.client.register_handle(self.handle)

    def set_parameter(self, position: str | int, param: Any) -> None:
        """Sets an unbound value in the query string to a specific value."""
        from triggerware.types import PreparedQueryException

        if not self.uses_named_params and isinstance(position, str):
            raise PreparedQueryException("This query uses positional parameters.")

        if self.uses_named_params and isinstance(position, int):
            raise PreparedQueryException("This query uses named parameters.")

        try:
            index = (
                self.input_signature_names.index(position)
                if isinstance(position, str)
                else position
            )
            expected_type = self.input_signature_types[index]
        except Exception:
            raise PreparedQueryException("Invalid parameter name or position.")

        if self.language == "sql":
            if not PreparedQuery._TYPE_MAP[expected_type](
                param
            ):  # Assuming type_map exists
                raise PreparedQueryException(
                    f"Expected type {expected_type}, got {type(param).__name__}"
                )

        self.prepared_params[index] = param

    def get_parameter(self, position: str | int) -> Any:
        """Gets the value of a parameter in the query string."""
        from triggerware.types import PreparedQueryException

        if not self.uses_named_params and isinstance(position, str):
            raise PreparedQueryException("This query uses positional parameters.")

        if self.uses_named_params and isinstance(position, int):
            raise PreparedQueryException("This query uses named parameters.")

        try:
            index = (
                self.input_signature_names.index(position)
                if isinstance(position, str)
                else position
            )
            parameter = self.prepared_params[index]
        except Exception:
            raise PreparedQueryException("Invalid parameter name or position.")

        return parameter

    def clone(self) -> "PreparedQuery[T]":
        """Clones this prepared query with the same parameters."""
        clone = PreparedQuery[T](self.client, self, self)
        clone.prepared_params = self.prepared_params[:]
        return clone

    def execute(
        self, restriction: "tw.ResourceRestricted | None" = None
    ) -> "tw.ResultSet[T]":
        """Executes this query on the Triggerware server and returns a ResultSet."""
        from triggerware.result_set import ResultSet

        parameters = {
            "handle": self.handle,
            "inputs": self.prepared_params,
        }
        if restriction:
            if restriction.row_limit is not None:
                parameters["limit"] = restriction.row_limit
            if restriction.timeout is not None:
                parameters["timelimit"] = restriction.timeout

        eq_result = self.client.json_rpc.call("create-resultset", parameters)
        return ResultSet[T](self.client, eq_result)


class PolledQuery[T](ABC, AbstractQuery[T]):
    """
    A PolledQuery is a query that is executed by the TW server on a set schedule.
    As soon as a PolledQuery is created, it is executed by the server, and the response (a set of
    "rows") establishes a "current state" of the query. For each succeeding execution (referred to as
    polling the query):

    - The new answer is compared with the current state, and the differences are sent to the
      Triggerware client in a notification containing a RowsDelta value.
    - The new answer then becomes the current state to be used for comparison with the result of the
      next poll of the query.

    Like any other query, a PolledQuery has a query string, a language (FOL or SQL), and a namespace.

    A polling operation may be performed at any time by executing the Poll method.
    Some details of reporting and polling can be configured with a PolledQueryControlParameters
    value that is supplied to the constructor of a PolledQuery.

    An instantiable subclass of PolledQuery must provide a HandleNotification method to
    handle notifications of changes to the current state. Errors can occur during a polling operation
    (e.g., timeout, inability to contact a data source). When such an error occurs, the TW Server will
    send an "error" notification. An instantiable subclass of PolledQuery may provide a
    HandleError method to handle error notifications.

    Polling may be terminated when Dispose is called.

    If a polling operation is ready to start (whether due to its schedule or an explicit poll request)
    and a previous poll of the query has not completed, the poll operation that is ready to start is
    skipped, and an error notification is sent to the client.
    """

    methodName: str

    def __init__(
        self,
        client: "tw.TriggerwareClient",
        query: "tw.Query",
        restriction: "tw.ResourceRestricted | None" = None,
        controls: "tw.PolledQueryControlParameters | None" = None,
        schedule: "tw.PolledQuerySchedule | None" = None,
    ) -> None:
        """
        Initialize a PolledQuery instance.

        Args:
            client (tw.TriggerwareClient): The Triggerware client.
            query (tw.Query): The query to execute.
            restriction (tw.ResourceRestricted | None, optional): Optional restrictions to apply.
            controls (tw.PolledQueryControlParameters | None, optional): Control parameters for polling.
            schedule (tw.PolledQuerySchedule | None, optional): The polling schedule.
        """
        from triggerware.jrpc import JsonRpcMessageHandler

        super().__init__(client, query, restriction)
        self.methodName = "poll" + str(self.client._poll_counter)  # type: ignore
        self.client._poll_counter += 1  # type: ignore
        self.base_parameters["method"] = self.methodName

        def process_schedule(schedule: "tw.PolledQuerySchedule") -> Any:
            if isinstance(schedule, int):
                return schedule
            if isinstance(schedule, list):
                return [process_schedule(x) for x in schedule]

            schedule.validate()
            return schedule.__dict__

        if schedule:
            self.base_parameters["schedule"] = process_schedule(schedule)
        if controls:
            self.base_parameters["report-initial"] = controls.report_initial
            self.base_parameters["report-unchanged"] = controls.report_unchanged
            self.base_parameters["delay"] = controls.delay

        registration = self.client.json_rpc.call(
            "create-polled-query", self.base_parameters
        )
        self.handle = registration["handle"]
        self.signature = []
        if "signature" in registration:
            self.signature = registration["signature"]
        if self.handle is not None:
            self.client.register_handle(self.handle)

        def notify_handler(delta: dict[str, Any] | list[Any]) -> None:
            print("delta is", delta)
            if "delta" in delta:
                delta = delta["delta"]  # type: ignore
                self.handleNotification(delta["added"], delta["deleted"])  # type: ignore

        self.client.json_rpc.add_method(
            self.methodName, JsonRpcMessageHandler(lambda _: {}, notify_handler)
        )

    def poll_now(self) -> None:
        """
        Perform an on-demand poll of this query (temporarily disregarding the set schedule).
        """
        parameters: dict[str, Any] = {"handle": self.handle}
        if self.timeout is not None:
            parameters["timelimit"] = self.timeout
        self.client.json_rpc.call("poll-now", parameters)

    @abstractmethod
    def handleNotification(self, added: list[Any], deleted: list[Any]) -> None:
        """
        Override this method to handle the polled query's changes in data. The polled query's
        schedule determines when this method will be called.

        Args:
            added (list[Any]): Rows added since the last poll.
            deleted (list[Any]): Rows deleted since the last poll.
        """
        pass
