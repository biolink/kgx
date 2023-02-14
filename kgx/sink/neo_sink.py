from typing import List, Union, Any

from neo4j import GraphDatabase, Neo4jDriver, Session
from kgx.config import get_logger
from kgx.error_detection import ErrorType
from kgx.sink.sink import Sink
from kgx.source.source import DEFAULT_NODE_CATEGORY
log = get_logger()


class NeoSink(Sink):
    """
    NeoSink is responsible for writing data as records
    to a Neo4j instance.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs
    uri: str
        The URI for the Neo4j instance.
        For example, http://localhost:7474
    username: str
        The username
    password: str
        The password
    kwargs: Any
        Any additional arguments

    """

    CACHE_SIZE = 100000
    node_cache = {}
    edge_cache = {}
    node_count = 0
    edge_count = 0
    CATEGORY_DELIMITER = "|"
    CYPHER_CATEGORY_DELIMITER = ":"
    _seen_categories = set()

    def __init__(self, owner, uri: str, username: str, password: str, **kwargs: Any):
        if "cache_size" in kwargs:
            self.CACHE_SIZE = kwargs["cache_size"]
        self.http_driver:Neo4jDriver = GraphDatabase.driver(
            uri, auth=(username, password)
        )
        self.session: Session = self.http_driver.session()
        super().__init__(owner)

    def _flush_node_cache(self):
        self._write_node_cache()
        self.node_cache.clear()
        self.node_count = 0

    def write_node(self, record) -> None:
        """
        Cache a node record that is to be written to Neo4j.
        This method writes a cache of node records when the
        total number of records exceeds ``CACHE_SIZE``

        Parameters
        ----------
        record: Dict
            A node record

        """
        sanitized_category = self.sanitize_category(record["category"])
        category = self.CATEGORY_DELIMITER.join(sanitized_category)
        if self.node_count >= self.CACHE_SIZE:
            self._flush_node_cache()
        if category not in self.node_cache:
            self.node_cache[category] = [record]
        else:
            self.node_cache[category].append(record)
        self.node_count += 1

    def _write_node_cache(self) -> None:
        """
        Write cached node records to Neo4j.
        """
        batch_size = 10000
        categories = self.node_cache.keys()
        filtered_categories = [x for x in categories if x not in self._seen_categories]
        self.create_constraints(filtered_categories)
        for category in self.node_cache.keys():
            log.debug("Generating UNWIND for category: {}".format(category))
            cypher_category = category.replace(
                self.CATEGORY_DELIMITER, self.CYPHER_CATEGORY_DELIMITER
            )
            query = self.generate_unwind_node_query(cypher_category)

            log.debug(query)
            nodes = self.node_cache[category]
            for x in range(0, len(nodes), batch_size):
                y = min(x + batch_size, len(nodes))
                log.debug(f"Batch {x} - {y}")
                batch = nodes[x:y]
                try:
                    self.session.run(query, parameters={"nodes": batch})
                except Exception as e:
                    self.owner.log_error(
                        entity=f"{category} Nodes {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    def _flush_edge_cache(self):
        self._flush_node_cache()
        self._write_edge_cache()
        self.edge_cache.clear()
        self.edge_count = 0

    def write_edge(self, record) -> None:
        """
        Cache an edge record that is to be written to Neo4j.
        This method writes a cache of edge records when the
        total number of records exceeds ``CACHE_SIZE``

        Parameters
        ----------
        record: Dict
            An edge record

        """
        if self.edge_count >= self.CACHE_SIZE:
            self._flush_edge_cache()
        # self.validate_edge(data)
        edge_predicate = record["predicate"]
        if edge_predicate in self.edge_cache:
            self.edge_cache[edge_predicate].append(record)
        else:
            self.edge_cache[edge_predicate] = [record]
        self.edge_count += 1

    def _write_edge_cache(self) -> None:
        """
        Write cached edge records to Neo4j.
        """
        batch_size = 10000
        for predicate in self.edge_cache.keys():
            query = self.generate_unwind_edge_query(predicate)
            log.debug(query)
            edges = self.edge_cache[predicate]
            for x in range(0, len(edges), batch_size):
                y = min(x + batch_size, len(edges))
                batch = edges[x:y]
                log.debug(f"Batch {x} - {y}")
                log.debug(edges[x:y])
                try:
                    self.session.run(
                        query, parameters={"relationship": predicate, "edges": batch}
                    )
                except Exception as e:
                    self.owner.log_error(
                        entity=f"{predicate} Edges {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    def finalize(self) -> None:
        """
        Write any remaining cached node and/or edge records.
        """
        self._write_node_cache()
        self._write_edge_cache()

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
    def generate_unwind_node_query(category: str) -> str:
        """
        Generate UNWIND cypher query for saving nodes into Neo4j.

        There should be a CONSTRAINT in Neo4j for ``self.DEFAULT_NODE_CATEGORY``.
        The query uses ``self.DEFAULT_NODE_CATEGORY`` as the node label to increase speed for adding nodes.
        The query also sets label to ``self.DEFAULT_NODE_CATEGORY`` for any node to make sure that the CONSTRAINT applies.

        Parameters
        ----------
        category: str
            Node category

        Returns
        -------
        str
            The UNWIND cypher query

        """
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:`{DEFAULT_NODE_CATEGORY}` {{id: node.id}})
        ON CREATE SET n += node, n:{category}
        ON MATCH SET n += node, n:{category}
        """

        return query

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
        MATCH (s:`{DEFAULT_NODE_CATEGORY}` {{id: edge.subject}}), (o:`{DEFAULT_NODE_CATEGORY}` {{id: edge.object}})
        MERGE (s)-[r:`{edge_predicate}`]->(o)
        SET r += edge
        """
        return query

    def create_constraints(self, categories: Union[set, list]) -> None:
        """
        Create a unique constraint on node 'id' for all ``categories`` in Neo4j.

        Parameters
        ----------
        categories: Union[set, list]
            Set of categories

        """
        categories_set = set(categories)
        categories_set.add(f"`{DEFAULT_NODE_CATEGORY}`")
        for category in categories_set:
            if self.CATEGORY_DELIMITER in category:
                subcategories = category.split(self.CATEGORY_DELIMITER)
                self.create_constraints(subcategories)
            else:
                query = NeoSink.create_constraint_query(category)
                try:
                    self.session.run(query)
                    self._seen_categories.add(category)
                except Exception as e:
                    self.owner.log_error(
                        entity=category,
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    @staticmethod
    def create_constraint_query(category: str) -> str:
        """
        Create a Cypher CONSTRAINT query

        Parameters
        ----------
        category: str
            The category to create a constraint on

        Returns
        -------
        str
            The Cypher CONSTRAINT query

        """
        query = f"CREATE CONSTRAINT IF NOT EXISTS ON (n:{category}) ASSERT n.id IS UNIQUE"
        return query
