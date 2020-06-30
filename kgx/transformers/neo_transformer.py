import logging
import itertools
import click
import networkx as nx
from typing import Tuple, List, Dict, Union

from kgx.transformers.transformer import Transformer
from kgx.utils.kgx_utils import generate_edge_key, current_time_in_millis
from neo4jrestclient.client import GraphDatabase as http_gdb, Node, Relationship
from neo4jrestclient.query import CypherException


class NeoTransformer(Transformer):
    """
    Transformer for reading from and writing to a Neo4j database.
    """

    CATEGORY_DELIMITER = '|'
    CYPHER_CATEGORY_DELIMITER = ':'

    def __init__(self, graph: nx.MultiDiGraph = None, uri: str = None, username: str = None, password: str = None):
        """
        Initialize an instance of NeoTransformer.
        """
        super(NeoTransformer, self).__init__(graph)
        self.http_driver = None
        self.http_driver = http_gdb(uri, username=username, password=password)

    def load(self, start: int = 0, end: int = None, is_directed: bool = True, page_size: int = 50000) -> None:
        """
        Read nodes and edges from a Neo4j database and create a networkx.MultiDiGraph

        Parameters
        ----------
        start: int
            Start for pagination
        end: int
            End for pagination
        is_directed: bool
            Are edges directed or undirected (``True``, by default, since edges in most cases are directed)
        page_size: int
            Size of page (or chunk) to fetch from Neo4j

        """
        if end is None:
            # get total number of records to be fetched from Neo4j
            count = self.count(is_directed=is_directed)
        else:
            count = end - start

        with click.progressbar(length=count, label='Getting {:,} records from Neo4j'.format(count)) as bar:
            time_start = current_time_in_millis()
            for page in self.get_pages(self.get_edges, start, end, page_size=page_size, **{'is_directed': is_directed}):
                self.load_edges(page)
                bar.update(page_size)
            bar.update(count)
            time_end = current_time_in_millis()
            logging.debug("time taken to load edges: {} ms".format(time_end - time_start))

    def count(self, is_directed: bool = True) -> int:
        """
        Get the total count of records to be fetched from the Neo4j database.

        Parameters
        ----------
        is_directed: bool
            Are edges directed or undirected (``True``, by default, since edges in most cases are directed)

        Returns
        -------
        int
            The total count of records

        """
        direction = '->' if is_directed else '-'
        query = f"MATCH (s)-[p]{direction}(o)"

        if self.edge_filters:
            qs = []
            if 'subject_category' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('subject_category', 's', ':', 'OR')})")
            if 'object_category' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('object_category', 'o', ':', 'OR')})")
            if 'edge_label' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('edge_label', 'p', '.')})")
            if 'provided_by' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('provided_by', 'p', '.', 'OR')})")
            query = ' WHERE '
            query += ' AND '.join(qs)
        query += f" RETURN COUNT(*) AS count"

        logging.debug(query)
        try:
            query_result = self.http_driver.query(query)
        except CypherException as ce:
            logging.error(ce)

        for result in query_result:
            return result[0]

    def load_nodes(self, nodes: List) -> None:
        """
        Load nodes into networkx.MultiDiGraph

        Parameters
        ----------
        nodes: List
            A list of nodes

        """
        start = current_time_in_millis()
        for node in nodes:
            self.load_node(node)
        end = current_time_in_millis()
        logging.debug("time taken to load nodes: {} ms".format(end - start))

    def load_node(self, node: Dict) -> None:
        """
        Load node into networkx.MultiDiGraph

        Parameters
        ----------
        node: Dict
            A node

        """
        self.graph.add_node(node['id'], **node)

    def load_edges(self, edges: List) -> None:
        """
        Load edges into networkx.MultiDiGraph

        Parameters
        ----------
        edges: List
            A list of edge records

        """
        start = current_time_in_millis()
        for record in edges:
            self.load_edge(record)
        end = current_time_in_millis()
        logging.debug("time taken to load edges: {} ms".format(end - start))

    def load_edge(self, edge_record: List) -> None:
        """
        Load an edge into networkx.MultiDiGraph

        Parameters
        ----------
        edge_record: List
            A 3-tuple edge record

        """
        subject_node = edge_record[0]
        edge = edge_record[1]
        object_node = edge_record[2]

        if 'subject' not in edge:
            edge['subject'] = subject_node['id']
        if 'object' not in edge:
            edge['object'] = object_node['id']

        if not self.graph.has_node(subject_node['id']):
            self.load_node(subject_node)

        if not self.graph.has_node(object_node['id']):
            self.load_node(object_node)

        key = generate_edge_key(subject_node['id'], edge['edge_label'], object_node['id'])
        self.graph.add_edge(subject_node['id'], object_node['id'], key, **edge)

    def get_pages(self, query_function, start: int = 0, end: int = None, page_size: int = 50000, **kwargs) -> list:
        """
        Get pages of size ``page_size`` from Neo4j.
        Returns an iterator of pages where number of pages is (``end`` - ``start``)/``page_size``

        Parameters
        ----------
        query_function: func
            The function to use to fetch records. Usually this is ``self.get_nodes`` or ``self.get_edges``
        start: int
            Start for pagination
        end: int
            End for pagination
        page_size: int
            Size of each page (``10000``, by default)
        **kwargs: dict
            Any additional arguments that might be relevant for ``query_function``

        Returns
        -------
        list
            An iterator for a list of records from Neo4j. The size of the list is ``page_size``

        """
        # itertools.count(0) starts counting from zero, and would run indefinitely without a return statement.
        # it's distinguished from applying a while loop via providing an index which is formative with the for statement
        for i in itertools.count(0):
            # First halt condition: page pointer exceeds the number of values allowed to be returned in total
            skip = start + (page_size * i)
            limit = page_size if end is None or skip + page_size <= end else end - skip
            if limit <= 0:
                return
            # execute query_function to get records
            records = query_function(skip=skip, limit=limit, **kwargs)

            # Second halt condition: no more data available
            if records:
                """
                * Yield halts execution until next call
                * Thus, the function continues execution upon next call
                * Therefore, a new page is calculated before record is instantiated again
                """
                yield records
            else:
                return

    def get_nodes(self, skip: int = 0, limit: int = 0) -> List:
        """
        Get a page of nodes from the Neo4j database.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for

        Returns
        -------
        list
            A list of nodes

        """
        query = f"MATCH (n)"

        if self.node_filters:
            qs = []
            if 'category' in self.node_filters:
                qs.append(f"({self.get_node_filter('category', 'n', ':', 'OR')})")
            if 'provided_by' in self.node_filters:
                qs.append(f"({self.get_node_filter('provided_by', 'n', '.', 'OR')})")
            query += ' WHERE '
            query += ' AND '.join(qs)

        query += f" RETURN n SKIP {skip}"

        if limit:
            query += f" LIMIT {limit}"

        logging.debug(query)
        try:
            results = self.http_driver.query(query, returns=Node, data_contents=True)
        except CypherException as ce:
            logging.error(ce)
        if results:
            nodes = [node[0] for node in results.rows]
        else:
            nodes = []
        return nodes

    def get_edges(self, skip: int = 0, limit: int = 0, is_directed: bool = True) -> List:
        """
        Get a page of edges from the Neo4j database.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for
        is_directed: bool
            Are edges directed or undirected (``True``, by default, since edges in most cases are directed)

        Returns
        -------
        list
            A list of 3-tuples

        """
        direction = '->' if is_directed else '-'
        query = f"MATCH (s)-[p]{direction}(o)"

        if self.edge_filters:
            qs = []
            if 'subject_category' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('subject_category', 's', ':', 'OR')})")
            if 'object_category' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('object_category', 'o', ':', 'OR')})")
            if 'edge_label' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('edge_label', 'p', '.')})")
            if 'provided_by' in self.edge_filters:
                qs.append(f"({self.get_edge_filter('provided_by', 'p', '.', 'OR')})")
            query += ' WHERE '
            query += ' AND '.join(qs)
        query += f" RETURN s, p, o SKIP {skip}"

        if limit:
            query += f" LIMIT {limit}"

        logging.debug(query)
        try:
            start = current_time_in_millis()
            results = self.http_driver.query(query, returns=(Node, Relationship, Node), data_contents=True)
            end = current_time_in_millis()
            logging.debug(f"Time taken to fetch edges from Neo4j: {end - start} ms")
        except CypherException as ce:
            logging.error(ce)
        if results:
            edges = [x for x in results.rows]
        else:
            edges = []
        return edges

    def save_node(self, nodes_by_category: Dict[str, list], batch_size: int = 10000) -> None:
        """
        Save all nodes into Neo4j using the UNWIND cypher clause.

        Parameters
        ----------
        nodes_by_category: Dict[str, list]
            A dictionary where node category is the key and the value is a list of nodes of that category
        batch_size: int
            Size of batch per transaction (default: 10000)

        """
        logging.info("Saving nodes")
        for category in nodes_by_category.keys():
            logging.debug("Generating UNWIND for category: {}".format(category))
            cypher_category = category.replace(self.CATEGORY_DELIMITER, self.CYPHER_CATEGORY_DELIMITER)
            query = NeoTransformer.generate_unwind_node_query(cypher_category)
            logging.debug(query)
            nodes = nodes_by_category[category]
            time_start = current_time_in_millis()
            for x in range(0, len(nodes), batch_size):
                y = min(x + batch_size, len(nodes))
                logging.debug(f"Batch {x} - {y}")
                batch = nodes[x:y]
                try:
                    self.http_driver.query(query, params={'nodes': batch})
                except CypherException as ce:
                    logging.error(ce)
            time_end = current_time_in_millis()
            logging.debug(f"Time taken to load {category} edges: {time_end - time_start} ms")

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
        MERGE (n:`{Transformer.DEFAULT_NODE_CATEGORY}` {{id: node.id}})
        ON CREATE SET n += node, n:{category}
        ON MATCH SET n += node, n:{category}
        """

        return query

    def save_edge(self, edges_by_edge_label: Dict[str, list], batch_size: int = 10000) -> None:
        """
        Save all edges into Neo4j using the UNWIND cypher clause.

        Parameters
        ----------
        edges_by_edge_label: dict
            A dictionary where edge label is the key and the value is a list of edges with that edge label
        batch_size: int
            Size of batch per transaction (default: 10000)

        """
        logging.info("Saving edges")
        for predicate in edges_by_edge_label.keys():
            query = self.generate_unwind_edge_query(predicate)
            logging.info(query)
            edges = edges_by_edge_label[predicate]
            time_start = current_time_in_millis()
            for x in range(0, len(edges), batch_size):
                y = min(x + batch_size, len(edges))
                batch = edges[x:y]
                logging.debug(f"Batch {x} - {y}")

                try:
                    self.http_driver.query(query, params={"relationship": predicate, "edges": batch})
                except CypherException as ce:
                    logging.error(ce)
            time_end = current_time_in_millis()
            logging.debug(f"Time taken to load {predicate} edges: {time_end - time_start} ms")

    @staticmethod
    def generate_unwind_edge_query(edge_label: str) -> str:
        """
        Generate UNWIND cypher query for saving edges into Neo4j.

        Query uses ``self.DEFAULT_NODE_CATEGORY`` to quickly lookup the required subject and object node.

        Parameters
        ----------
        edge_label: str
            Edge label as string

        Returns
        -------
        str
            The UNWIND cypher query

        """

        query = f"""
        UNWIND $edges AS edge
        MATCH (s:`{NeoTransformer.DEFAULT_NODE_CATEGORY}` {{id: edge.subject}}), (o:`{Transformer.DEFAULT_NODE_CATEGORY}` {{id: edge.object}})
        MERGE (s)-[r:`{edge_label}`]->(o)
        SET r += edge
        """
        return query

    def save(self) -> None:
        """
        Save all nodes and edges from networkx.MultiDiGraph into Neo4j using the UNWIND cypher clause.

        """
        nodes_by_category = {}

        for n, node_data in self.graph.nodes(data=True):
            if 'id' not in node_data:
                node_data['id'] = n
            node_data = self.validate_node(node_data)
            category = self.sanitize_category(node_data['category'])
            category = self.CATEGORY_DELIMITER.join(category)
            if category not in nodes_by_category:
                nodes_by_category[category] = [node_data]
            else:
                nodes_by_category[category].append(node_data)

        edges_by_edge_label = {}
        for u, v, k, data in self.graph.edges(keys=True, data=True):
            self.validate_edge(data)
            edge_label = data['edge_label']
            if edge_label in edges_by_edge_label:
                edges_by_edge_label[edge_label].append(data)
            else:
                edges_by_edge_label[edge_label] = [data]

        # create indexes
        self.create_constraints(set(nodes_by_category.keys()))
        # save all nodes
        self.save_node(nodes_by_category)
        # save all edges
        self.save_edge(edges_by_edge_label)

    def neo4j_report(self) -> None:
        """
        Give a summary on the number of nodes and edges in the Neo4j database.

        """
        try:
            node_results = self.http_driver.query("MATCH (n) RETURN COUNT(*)")
        except CypherException as ce:
            logging.error(ce)

        for r in node_results:
            logging.info("Number of Nodes: {}".format(r[0]))

        try:
            edge_results = self.http_driver.query("MATCH (s)-->(o) RETURN COUNT(*)")
        except CypherException as ce:
            logging.error(ce)

        for r in edge_results:
            logging.info("Number of Edges: {}".format(r[0]))

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
                except CypherException as ce:
                    logging.error(ce)

    def get_node_filter(self, key: str, variable: str = None, prefix: str = None, op: str = None) -> str:
        """
        Get the value for node filter as defined by ``key``.
        This is used as a convenience method for generating cypher queries.

        Parameters
        ----------
        key: str
            Name of the node filter

        Returns
        -------
        str
            Value corresponding to the given node filter `key`, formatted for CQL
        """
        value = ''
        if key in self.node_filters and self.node_filters[key]:
            if isinstance(self.node_filters[key], (list, set, tuple)):
                if key in {'category'}:
                    formatted = [f"{variable}{prefix}`{x}`" for x in self.node_filters[key]]
                    value = f" {op} ".join(formatted)
                elif key in {'provided_by'}:
                    formatted = [f"'{x}' IN {variable}{prefix}{key}" for x in self.node_filters['provided_by']]
                    value = f" {op} ".join(formatted)
                else:
                    formatted = []
                    for v in self.node_filters[key]:
                        formatted.append(f"{variable}{prefix}{key} = '{v}'")
                    value = f" {op} ".join(formatted)
            elif isinstance(self.node_filters[key], str):
                value = f"{variable}{prefix}{key} = '{self.node_filters[key]}'"
            else:
                logging.error(f"Unexpected {key} node filter of type {type(self.node_filters[key])}")
        return value

    def get_edge_filter(self, key: str, variable: str = None, prefix: str = None, op: str = None) -> str:
        """
        Get the value for edge filter as defined by ``key``.
        This is used as a convenience method for generating cypher queries.

        Parameters
        ----------
        key: str
            Name of the edge filter

        Returns
        -------
        str
            Value corresponding to the given edge filter `key`, formatted for CQL
        """
        value = ''
        if key in self.edge_filters and self.edge_filters[key]:
            if isinstance(self.edge_filters[key], (list, set, tuple)):
                if key in {'subject_category', 'object_category'}:
                    formatted = [f"{variable}{prefix}`{x}`" for x in self.edge_filters[key]]
                    value = f" {op} ".join(formatted)
                elif key == 'edge_label':
                    formatted = [f"'{x}'" for x in self.edge_filters['edge_label']]
                    value = f"type({variable}) IN [{', '.join(formatted)}]"
                elif key == 'provided_by':
                    formatted = [f"'{x}' IN {variable}{prefix}{key}" for x in self.edge_filters['provided_by']]
                    value = f" {op} ".join(formatted)
                else:
                    formatted = []
                    for v in self.edge_filters[key]:
                        formatted.append(f"{variable}{prefix}{key} = '{v}'")
                    value = f" {op} ".join(formatted)
            elif isinstance(self.edge_filters[key], str):
                value = f"{variable}{prefix}{key} = '{self.edge_filters[key]}'"
            else:
                logging.error(f"Unexpected {key} edge filter of type {type(self.edge_filters[key])}")
        return value


    @staticmethod
    def sanitize_category(category: list):
        """
        Sanitize category for use in UNWIND cypher clause.
        This method adds escape characters to each element in category
        list to ensure the category is processed correctly.

        Parameters
        ----------
        category: list
            Category

        Returns
        -------
        list
            Sanitized category list

        """
        return [f"`{x}`" for x in category]

    @staticmethod
    def create_constraint_query(category):
        query = f"CREATE CONSTRAINT ON (n:{category}) ASSERT n.id IS UNIQUE"
        return query
