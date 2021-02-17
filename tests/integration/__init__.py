import pytest
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.query import CypherException

CONTAINER_NAME = 'kgx-neo4j-integration-test'
DEFAULT_NEO4J_URL = 'http://localhost:7474'
DEFAULT_NEO4J_USERNAME = 'neo4j'
DEFAULT_NEO4J_PASSWORD = 'test'


@pytest.fixture(scope='function')
def clean_slate():
    http_driver = GraphDatabase(DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    q = "MATCH (n) DETACH DELETE (n)"
    print(q)
    try:
        http_driver.query(q)
    except CypherException as ce:
        print(ce)
