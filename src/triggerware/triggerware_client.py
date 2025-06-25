from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import triggerware as tw

class TriggerwareClient:
    """
    A TriggerwareClient provides a connection to a triggerware server. A client contains methods for
    issuing a few specific requests that are supported by any Triggerware server. Classes that extend
    TriggerwareClient for specific applications will implement their own application-specific methods
    to make requests that are idiosyncratic to a Triggerware server for that application.

    A TriggerwareClient can also manage Subscriptions. By subscribing to certain kinds of changes,
    the client arranges to be notified when these changes occur in the data accessible to the server.
    """
    _batch_sub_counter = 0
    _sub_counter = 0
    _poll_counter = 0
    _handles: list[int] = []

    sql_mode: str
    default_fol_schema: str | None = None
    default_sql_schema: str | None = None
    default_fetch_size: int | None = None
    default_timeout: float | None = None

    def __init__(self, address: str, port: int):
        """
        Initialize a TriggerwareClient instance.

        Args:
            address (str): The server address.
            port (int): The server port.
        """
        from triggerware.jrpc import JsonRpcClient
        self.json_rpc = JsonRpcClient(address, port)

    def execute_query(
        self,
        query: "tw.Query",
        restriction: "tw.ResourceRestricted | None" = None
    ) -> "tw.ResultSet":
        """
        Executes a query on the connected server. The result is the same as if a tw View had been
        created and executed.

        Args:
            query (tw.Query): The query to execute.
            restriction (tw.ResourceRestricted | None, optional): Optional restrictions to apply to the query.

        Returns:
            tw.ResultSet: The result set from the server.
        """
        from triggerware.queries import View
        view = View(self, query, restriction)
        return view.execute()

    def validate_query(self, query: "tw.Query"):
        """
        Validate a query string on the server. This method will raise an InvalidQueryException if
        the query contains errors.

        Args:
            query (tw.Query): The query to validate.

        Raises:
            InvalidQueryException: If the query contains errors.
            InternalErrorException: If an internal error occurs.
            ServerErrorException: If a server error occurs.
        """
        from triggerware.jrpc import InternalErrorException, ServerErrorException, JsonRpcException
        from triggerware.types import InvalidQueryException
        params = [
            query.query,
            query.language,
            query.schema,
        ]
        try:
            self.json_rpc.call('validate', params)
        except InternalErrorException as e:
            raise e
        except ServerErrorException as e:
            raise e
        except JsonRpcException as e:
            raise InvalidQueryException(e.message)

    def get_rel_data(self) -> "list[tw.RelDataGroup]":
        """
        Fetches a connection of connectors that Triggerware currently supports. The result is
        compartmentalized into groups, sorted by use case. Each element in a group contains
        signature information for how to use the conncetor in a query.

        Returns:
            list[tw.RelDataGroup]: The list of RelDataGroup objects.
        """
        from triggerware.types import RelDataElement, RelDataGroup
        raw_groups = self.json_rpc.call('reldata2017', [])
        groups = []
        if (isinstance(raw_groups, list)):
            for raw_group in raw_groups:
                elements = []
                for i in range(2, len(raw_group)):
                    raw_element = raw_group[i]
                    element = RelDataElement()
                    element.name = raw_element[0]
                    element.signature_names = raw_element[1]
                    element.signature_types = raw_element[2]
                    element.usage = raw_element[3]
                    element.no_idea = raw_element[4]
                    element.description = raw_element[5]
                    elements.append(element)
                group = RelDataGroup()
                group.name = raw_group[0]
                group.symbol = raw_group[1]
                group.elements = elements
                groups.append(group)
        return groups

    def close(self):
        """
        Closes the connection to the server.
        """
        self.json_rpc.close()

    def register_handle(self, handle: int):
        """
        Register a handle with this client.

        Args:
            handle (int): The handle to register.
        """
        self._handles.append(handle)

