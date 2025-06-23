from typing import Any, TYPE_CHECKING
if TYPE_CHECKING:
    import triggerware as tw

from triggerware.interfaces import TriggerwareObject, ResourceRestricted

class ResultSet[T](TriggerwareObject, ResourceRestricted):
    """
    Represents a result set from the server after executing a query. Result sets are iterable and
    can fetch new rows using next(resultset), or, for multiple rows, resultset.pull(n). Each 'row'
    is a tuple of values determined by the signature of the executed query.
    """
    cache: list[T]
    cache_idx: int
    exhausted: bool

    def __init__(
        self,
        client: "tw.TriggerwareClient",
        eq_result: dict[str, Any],
        row_limit: int | None = None,
        timeout: float | None = None,
    ) -> None:
        self.client = client
        self.handle = None if 'handle' not in eq_result else eq_result['handle']
        self.row_limit = row_limit if row_limit is not None else client.default_fetch_size
        self.timeout = timeout if timeout is not None else client.default_timeout
        self.signature = []
        self.cache = []
        self.exhausted = False
        if 'signature' in eq_result:
            self.signature = eq_result['signature']
        if 'batch' in eq_result:
            if 'tuples' in eq_result['batch']:
                self.cache = eq_result['batch']['tuples']
                self.exhausted = self.handle == None

    def __iter__(self) -> "ResultSet[T]":
        return self
    
    def __next__(self) -> T:
        if self.cache_idx >= len(self.cache):
            if self.exhausted:
                raise StopIteration

            result = self.client.json_rpc.call("next-resultset-batch", [self.handle, self.row_limit, self.timeout])
            self.cache = result["batch"]["tuples"]
            self.cache_idx = 0
            self.exhausted = result["batch"]["exhausted"]

            if not self.cache:
                raise StopIteration

        value = self.cache[self.cache_idx]
        self.cache_idx += 1
        return value

    def pull(self, n: int) -> list[T]:
        """
        Fetches the next n rows from the result set.
        :param n: The number of rows to fetch. Will fetch fewer if the result set is exhausted.
        """
        items = []
        for _ in range(n):
            try:
                items.append(next(self))
            except StopIteration:
                break
        return items








