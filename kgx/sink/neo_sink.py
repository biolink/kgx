from typing import List, Union

from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.query import CypherException

from kgx import NeoTransformer, Transformer
from kgx.config import get_logger
from kgx.sink.sink import Sink

log = get_logger()

class NeoSink(Sink):

    CACHE_SIZE = 100000
    node_cache = {}
    edge_cache = {}
    node_count = 0
    edge_count = 0
    CATEGORY_DELIMITER = '|'
    CYPHER_CATEGORY_DELIMITER = ':'
    _seen_categories = set()

    def __init__(self, uri, username, password, **kwargs):
        super().__init__()
        self.http_driver: GraphDatabase = GraphDatabase(uri, username=username, password=password)

    def write_node(self, record):
        sanitized_category = self.sanitize_category(record['category'])
        category = self.CATEGORY_DELIMITER.join(sanitized_category)
        if self.node_count >= self.CACHE_SIZE:
            self._write_node()
            self.node_cache.clear()
            self.node_count = 0
        if category not in self.node_cache:
            self.node_cache[category] = [record]
        else:
            self.node_cache[category].append(record)
        self.node_count += 1

    def _write_node(self):
        batch_size = 10000
        categories = self.node_cache.keys()
        filtered_categories = [x for x in categories if x not in self._seen_categories]
        self.create_constraints(filtered_categories)
        for category in self.node_cache.keys():
            log.debug("Generating UNWIND for category: {}".format(category))
            cypher_category = category.replace(self.CATEGORY_DELIMITER, self.CYPHER_CATEGORY_DELIMITER)
            query = NeoTransformer.generate_unwind_node_query(cypher_category)
            log.debug(query)
            nodes = self.node_cache[category]
            for x in range(0, len(nodes), batch_size):
                y = min(x + batch_size, len(nodes))
                log.debug(f"Batch {x} - {y}")
                batch = nodes[x:y]
                try:
                    self.http_driver.query(query, params={'nodes': batch})
                except CypherException as ce:
                    log.error(ce)

    def write_edge(self, record):
        if self.edge_count >= self.CACHE_SIZE:
            self._write_edge()
            self.edge_cache.clear()
            self.edge_count = 0
        #self.validate_edge(data)
        edge_predicate = record['predicate']
        if edge_predicate in self.edge_cache:
            self.edge_cache[edge_predicate].append(record)
        else:
            self.edge_cache[edge_predicate] = [record]
        self.edge_count += 1

    def _write_edge(self):
        batch_size = 10000
        for predicate in self.edge_cache.keys():
            query = self.generate_unwind_edge_query(predicate)
            log.info(query)
            edges = self.edge_cache[predicate]
            for x in range(0, len(edges), batch_size):
                y = min(x + batch_size, len(edges))
                batch = edges[x:y]
                log.debug(f"Batch {x} - {y}")
                try:
                    self.http_driver.query(query, params={"relationship": predicate, "edges": batch})
                except CypherException as ce:
                    log.error(ce)

    @staticmethod
    def sanitize_category(category: List) -> List:
        """
        Sanitize category for use in UNWIND cypher clause.
        This method adds escape characters to each element in category
        list to ensure the category is processed correctly.

        Parameters
        ----------
        category: List
            Category

        Returns
        -------
        List
            Sanitized category list

        """
        return [f"`{x}`" for x in category]

    @staticmethod
    def generate_unwind_edge_query(edge_predicate: str) -> str:
        """
        Generate UNWIND cypher query for saving edges into Neo4j.

        Query uses ``self.DEFAULT_NODE_CATEGORY`` to quickly lookup the required subject and object node.

        Parameters
        ----------
        edge_predicate: str
            Edge label as string

        Returns
        -------
        str
            The UNWIND cypher query

        """

        query = f"""
        UNWIND $edges AS edge
        MATCH (s:`{NeoTransformer.DEFAULT_NODE_CATEGORY}` {{id: edge.subject}}), (o:`{Transformer.DEFAULT_NODE_CATEGORY}` {{id: edge.object}})
        MERGE (s)-[r:`{edge_predicate}`]->(o)
        SET r += edge
        """
        return query

    def create_constraints(self, categories: Union[set, list]) -> None:
        """
        Create a unique constraint on node 'id' for all ``categories`` in Neo4j.

        Parameters
        ----------
        categories: set
            Set of categories

        """
        categories_set = set(categories)
        categories_set.add(f"`{Transformer.DEFAULT_NODE_CATEGORY}`")
        for category in categories_set:
            if self.CATEGORY_DELIMITER in category:
                subcategories = category.split(self.CATEGORY_DELIMITER)
                self.create_constraints(subcategories)
            else:
                query = NeoTransformer.create_constraint_query(category)
                try:
                    self.http_driver.query(query)
                    self._seen_categories.add(category)
                except CypherException as ce:
                    log.error(ce)

    def finalize(self):
        # finish up any entries left in cache
        self._write_node()
        self._write_edge()
