import os

from kgx.graph.nx_graph import NxGraph
from kgx.sink import ParquetSink
from kgx.transformer import Transformer
from tests import TARGET_DIR

from pyarrow.parquet import read_table


def test_write_parquet():
    """
    Write a graph to a Parquet file using ParquetSink.
    """
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing", "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_node("C", id="C", **{"name": "Node C", "category": ["biolink:NamedThing"]})
    graph.add_node("D", id="D", **{"name": "Node D", "category": ["biolink:NamedThing"]})
    graph.add_node("E", id="E", **{"name": "Node E", "category": ["biolink:NamedThing"]})
    graph.add_node("F", id="F", **{"name": "Node F", "category": ["biolink:NamedThing"]})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": "biolink:sub_class_of"}
    )

    t = Transformer()
    s = ParquetSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph"),
        format="parquet",
        node_properties={"id", "name", "category"},
        edge_properties={"subject", "predicate", "object", "relation"},
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    nodes = read_table(os.path.join(TARGET_DIR, "test_graph_nodes.parquet")).to_pandas()
    edges = read_table(os.path.join(TARGET_DIR, "test_graph_edges.parquet")).to_pandas()

    assert len(nodes) == 6
    assert len(edges) == 6
