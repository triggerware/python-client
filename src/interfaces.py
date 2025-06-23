from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import triggerware as tw

class TriggerwareObject:
    """
    Anything that stores a connection to a triggerware client. Most of these objects have a handle,
    but it is useful to represent certain handleless things, such as Views, as triggerware objects
    since they need direct access to the triggerware client.
    """
    client:"tw.TriggerwareClient" 
    handle: int | None = None


class ResourceRestricted:
    """
    Anything that requires resource limits, such as queries that need to be executed.
    """
    row_limit: int | None = None
    timeout: float | None = None


class Query:
    """
    Anything with a query, language, and schema.
    """
    query: str
    language: str
    schema: str


