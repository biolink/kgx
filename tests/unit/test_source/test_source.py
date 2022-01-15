import pytest

from kgx.transformer import Transformer
from kgx.source.source import DEFAULT_NODE_CATEGORY, Source


@pytest.mark.parametrize(
    "node",
    [
        {"name": "Node A", "description": "Node without an ID"},
        {"node_id": "A", "description": "Node without an ID,  name and category"},
        {"name": "Node A", "description": "Node A", "category": "biolink:NamedThing"},
        {"id": "", "name": "hgnc:0", "description": "Node without empty 'id' value", "category": "biolink:NamedThing"},
        {"id": "hgnc:1234", "description": "Node without name", "category": "biolink:NamedThing"},
        {"id": "hgnc:5678", "name": "Node A", "description": "Node without category"},
    ],
)
def test_validate_incorrect_node(node):
    """
    Test basic validation of a node, where the node is invalid.
    """
    t = Transformer()
    s = Source(t)
    result = s.validate_node(node)
    if len(t.get_errors("Error")) > 0:
        assert result is None
    else:
        assert result is not None
    t.write_report()


@pytest.mark.parametrize(
    "node",
    [
        {
            "id": "A",
            "name": "Node A",
            "description": "Node A",
            "category": ["biolink:NamedThing"],
        },
        {
            "id": "A",
            "name": "Node A",
            "description": "Node A"
        },
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
    if len(t.get_errors()) > 0:
        assert len(t.get_errors("Error")) == 0
        assert len(t.get_errors("Warning")) > 0
        t.write_report(None, "Warning")


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
    assert len(t.get_errors()) > 0
    t.write_report()


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
    assert len(t.get_errors()) == 0
    t.write_report()


@pytest.mark.parametrize(
    "node",
    [
        {
            "id": "hgnc:1234",
            "name": "some node",
            "description": "Node without name",
            "category": "biolink:NamedThing",
            "some_field": "don't care!"
        },
    ],
)
def test_incorrect_node_filters(node):
    """
    Test filtering of a node
    """
    t = Transformer()
    s = Source(t)
    filters = {
        "some_field": {"bad_node_filter": 1}
    }
    s.set_node_filters(filters)
    s.check_node_filter(node)
    assert len(t.get_errors("Error")) > 0
    t.write_report()


@pytest.mark.parametrize(
    "edge",
    [
        {
            "subject": "A",
            "predicate": "biolink:related_to",
            "object": "B",
            "some_field": "don't care here either!"
        },
    ],
)
def test_incorrect_edge_filters(edge):
    """
    Test filtering of an edge
    """
    t = Transformer()
    s = Source(t)
    filters = {
        "some_field": {"bad_edge_filter": 1}
    }
    s.set_edge_filters(filters)
    s.check_edge_filter(edge)
    assert len(t.get_errors("Error")) > 0
    t.write_report()
