import os
import pprint

import docker
import pytest
from neo4jrestclient.client import GraphDatabase

CONTAINER_NAME = 'kgx-neo4j-unit-test'
DEFAULT_NEO4J_URL = 'http://localhost:8484'
DEFAULT_NEO4J_USERNAME = 'neo4j'
DEFAULT_NEO4J_PASSWORD = 'test'


RESOURCE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'resources')
TARGET_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'target')


def check_container():
    try:
        client = docker.from_env()
        status = False
        try:
            c = client.containers.get(CONTAINER_NAME)
            if c.status == 'running':
                status = True
        except:
            status = False
    except:
        print("Could not connect to local Docker daemon")
        status = False
    return status


@pytest.fixture(scope='function')
def clean_slate():
    http_driver = GraphDatabase(DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    q = "MATCH (n) DETACH DELETE (n)"
    print(q)
    http_driver.query(q)


def print_graph(g):
    pprint.pprint([x for x in g.nodes(data=True)])
    pprint.pprint([x for x in g.edges(data=True)])

