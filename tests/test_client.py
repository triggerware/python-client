import pytest

import src.triggerware as tw

def test_view():
    client = tw.TriggerwareClient("localhost", 5221)
    query = tw.FolQuery("((a) s.t. (inflation 1991 1995 a))")
    view = client.execute_query(query)
    assert len(view.cache) != 0
    for a in view.cache:
        assert a is not None