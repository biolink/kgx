import pytest
from kgx.transformer import Transformer
from kgx.source.source import DEFAULT_NODE_CATEGORY, Source


@pytest.mark.parametrize(
    "node",
    [
        {"name": "Node A", "description": "Node without an ID"},
        {"node_id": "A", "description": "Node without an ID and name"},
        {"name": "Node A", "description": "Node A", "category": "biolink:NamedThing"},
    ],
)
def test_validate_incorrect_node(node):
    """
    Test basic validation of a node, where the node is invalid.
    """
    t = Transformer()
    s = Source(t)
    assert not s.validate_node(node)


@pytest.mark.parametrize(
    "node",
    [
        {
            "id": "A",
            "name": "Node A",
            "description": "Node A",
            "category": ["biolink:NamedThing"],
        },
        {"id": "A", "name": "Node A", "description": "Node A"},
    ],
)
def test_validate_correct_node(node):
    """
    Test basic validation of a node, where the node is valid.
    """
    t = Transformer()
    s = Source(t)
    n = s.validate_node(node)
    assert n is not None
    assert "category" in n
    assert n["category"][0] == DEFAULT_NODE_CATEGORY


@pytest.mark.parametrize(
    "edge",
    [
        {"predicate": "biolink:related_to"},
        {"subject": "A", "predicate": "biolink:related_to"},
        {"subject": "A", "object": "B"},
    ],
)
def test_validate_incorrect_edge(edge):
    """
    Test basic validation of an edge, where the edge is invalid.
    """
    t = Transformer()
    s = Source(t)
    assert not s.validate_edge(edge)


@pytest.mark.parametrize(
    "edge",
    [
        {"subject": "A", "object": "B", "predicate": "biolink:related_to"},
        {
            "subject": "A",
            "object": "B",
            "predicate": "biolink:related_to",
            "relation": "RO:000000",
        },
    ],
)
def test_validate_correct_edge(edge):
    """
    Test basic validation of an edge, where the edge is valid.
    """
    t = Transformer()
    s = Source(t)
    e = s.validate_edge(edge)
    assert e is not None
