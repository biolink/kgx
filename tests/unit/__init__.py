import docker
import pytest
from neo4j import GraphDatabase

from kgx.graph.nx_graph import NxGraph

CONTAINER_NAME = "kgx-neo4j-unit-test"
DEFAULT_NEO4J_URL = "neo4j://localhost:7687"
DEFAULT_NEO4J_USERNAME = "neo4j"
DEFAULT_NEO4J_PASSWORD = "test"
DEFAULT_NEO4J_DATABASE = "neo4j"


def check_container():
    """
    Check whether the container with the name ``CONTAINER_NAME``
    is currently running.
    """
    try:
        client = docker.from_env()
        status = False
        try:
            c = client.containers.get(CONTAINER_NAME)
            if c.status == "running":
                status = True
        except:
            status = False
    except:
        status = False
    return status


@pytest.fixture(scope="function")
def clean_database():
    """
    Delete all nodes and edges in Neo4j test container.
    """
    with GraphDatabase.driver(
            DEFAULT_NEO4J_URL,
            auth=(DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD)
    ) as http_driver:
        q = "MATCH (n) DETACH DELETE (n)"
        try:
            session = http_driver.session()
            session.run(q)
        except Exception as e:
            print(e)


# this is a bit of misnomer: yes, it processes a stream
# but then loads it into in memory data structures.
# This could be problematic for huge graphs?
def load_graph_dictionary(g):
    """
    Process a given stream into a nodes and edges dictionary.
    """
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                key = (rec[0], rec[1])
                if key in edges:
                    edges[key].append(rec[-1])
                else:
                    edges[key] = [rec[-1]]
            else:
                nodes[rec[0]] = rec[-1]
    return nodes, edges


def get_graph(source):
    """
    Returns a series of defined graphs.
    """
    g1 = NxGraph()
    g1.name = "Graph 1"
    g1.add_node(
        "A",
        **{
            "id": "A",
            "name": "Node A",
            "category": ["biolink:NamedThing"],
            "source": source,
        }
    )
    g1.add_node(
        "B",
        **{
            "id": "B",
            "name": "Node B",
            "category": ["biolink:NamedThing"],
            "source": source,
        }
    )
    g1.add_node(
        "C",
        **{
            "id": "C",
            "name": "Node C",
            "category": ["biolink:NamedThing"],
            "source": source,
        }
    )
    g1.add_edge(
        "B",
        "A",
        **{
            "subject": "B",
            "object": "A",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )

    g2 = NxGraph()
    g2.add_node(
        "A", **{"id": "A", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_node(
        "B", **{"id": "B", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_node(
        "C", **{"id": "C", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_node(
        "D", **{"id": "D", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_node(
        "E", **{"id": "E", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_node(
        "F", **{"id": "F", "category": ["biolink:NamedThing"], "source": source}
    )
    g2.add_edge(
        "B",
        "A",
        **{
            "subject": "B",
            "object": "A",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )
    g2.add_edge(
        "C",
        "B",
        **{
            "subject": "C",
            "object": "B",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )
    g2.add_edge(
        "D",
        "C",
        **{
            "subject": "D",
            "object": "C",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )
    g2.add_edge(
        "D",
        "A",
        **{
            "subject": "D",
            "object": "A",
            "predicate": "biolink:related_to",
            "source": source,
        }
    )
    g2.add_edge(
        "E",
        "D",
        **{
            "subject": "E",
            "object": "D",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )
    g2.add_edge(
        "F",
        "D",
        **{
            "subject": "F",
            "object": "D",
            "predicate": "biolink:subclass_of",
            "source": source,
        }
    )

    g3 = NxGraph()
    g3.add_node(
        "A", **{"id": "A", "category": ["biolink:NamedThing"], "source": source}
    )
    g3.add_node(
        "B", **{"id": "B", "category": ["biolink:NamedThing"], "source": source}
    )
    g3.add_edge(
        "A",
        "B",
        **{
            "subject": "A",
            "object": "B",
            "predicate": "biolink:related_to",
            "source": source,
        }
    )

    g4 = NxGraph()
    g4.add_node(
        "A",
        **{
            "id": "A",
            "category": ["biolink:Gene"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "B",
        **{
            "id": "B",
            "category": ["biolink:Gene"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "A1",
        **{
            "id": "A1",
            "category": ["biolink:Protein"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "A2",
        **{
            "id": "A2",
            "category": ["biolink:Protein"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "B1",
        **{
            "id": "B1",
            "category": ["biolink:Protein"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "X",
        **{
            "id": "X",
            "category": ["biolink:Drug"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_node(
        "Y",
        **{
            "id": "Y",
            "category": ["biolink:Drug"],
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_edge(
        "A",
        "A1",
        **{
            "subject": "A",
            "object": "A1",
            "predicate": "biolink:has_gene_product",
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_edge(
        "A",
        "A2",
        **{
            "subject": "A",
            "object": "A2",
            "predicate": "biolink:has_gene_product",
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_edge(
        "B",
        "B1",
        **{
            "subject": "B",
            "object": "B1",
            "predicate": "biolink:has_gene_product",
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_edge(
        "X",
        "A1",
        **{
            "subject": "X",
            "object": "A1",
            "predicate": "biolink:interacts_with",
            "provided_by": source,
            "source": source,
        }
    )
    g4.add_edge(
        "Y",
        "B",
        **{
            "subject": "Y",
            "object": "B",
            "predicate": "biolink:interacts_with",
            "provided_by": source,
            "source": source,
        }
    )
    return [g1, g2, g3, g4]
