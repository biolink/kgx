import itertools
import logging
import uuid
from typing import Tuple, List, Dict

import click
from neo4jrestclient.client import GraphDatabase as http_gdb, Node, Relationship
from neo4jrestclient.query import CypherException

from .transformer import Transformer


class NeoTransformer(Transformer):
    """
    Transformer for reading from and writing to a Neo4j database.
    """

    def __init__(self, graph=None, host=None, port=None, username=None, password=None):
        super(NeoTransformer, self).__init__(graph)
        self.http_driver = None
        http_uri = f'http://{host}:{port}'
        self.http_driver = http_gdb(http_uri, username=username, password=password)

    def load(self, start=0, end=None, is_directed=True) -> None:
        """
        Read nodes and edges from a Neo4j database and create a networkx.MultiDiGraph

        Parameters
        ----------
        start: int
            Start for pagination
        end: int
            End for pagination
        is_directed: bool
            Are edges directed or undirected (`True`, by default, since edges in most cases are directed)

        """
        PAGE_SIZE = 10_000

        if end is None:
            # get total number of records to be fetched from Neo4j
            count = self.count(is_directed=is_directed)
        else:
            count = end - start

        with click.progressbar(length=count, label='Getting {:,} records from Neo4j'.format(count)) as bar:
            time_start = self.current_time_in_millis()
            for page in self.get_pages(self.get_edges, start, end, page_size=PAGE_SIZE, is_directed=is_directed):
                self.load_edges(page)
                bar.update(PAGE_SIZE)
            bar.update(count)
            time_end = self.current_time_in_millis()
            logging.debug("time taken to load edges: {} ms".format(time_end - time_start))

    def count(self, is_directed=True) -> int:
        """
        Get the total count of records to be fetched from the Neo4j database.

        Parameters
        ----------
        is_directed: bool
            Are edges directed or undirected ('True', by default, since edges in most cases are directed)

        Returns
        -------
        int
            The total count of records
        """
        direction = '->' if is_directed else '-'
        query = f"""
        MATCH (s{self.get_filter('subject_category')})-[p{self.get_filter('edge_label')}]{direction}(o{self.get_filter('object_category')})
        RETURN COUNT(*) AS count;
        """

        logging.info("Query: {}".format(query))
        try:
            query_result = self.http_driver.query(query)
        except CypherException as ce:
            logging.error(ce)

        for result in query_result:
            return result[0]

    def load_nodes(self, nodes: List[Node]) -> None:
        """
        Load nodes into networkx.MultiDiGraph

        Parameters
        ----------
        nodes: List[neo4jrestclient.client.Node]
            A list of node records

        """
        start = self.current_time_in_millis()
        for node in nodes:
            self.load_node(node)
        end = self.current_time_in_millis()
        logging.debug("time taken to load nodes: {} ms".format(end - start))

    def load_node(self, node: Node) -> None:
        """
        Load node from neo4jrestclient.client.Node into networkx.MultiDiGraph

        Parameters
        ----------
        node: neo4jrestclient.client.Node
            A node

        """

        attributes = {}
        for key, value in node.properties.items():
            attributes[key] = value

        node_labels = [x._label for x in node.labels]

        if 'category' not in attributes:
            attributes['category'] = node_labels
        else:
            if isinstance(attributes['category'], str):
                attributes['category'] = [attributes['category']]

        for l in node_labels:
            if l not in attributes['category']:
                attributes['category'].append(l)

        if Transformer.DEFAULT_NODE_LABEL not in attributes['category']:
            attributes['category'].append(Transformer.DEFAULT_NODE_LABEL)

        node_id = node['id'] if 'id' in node else node.id

        self.graph.add_node(node_id, **attributes)

    def load_edges(self, edges: List) -> None:
        """
        Load edges into networkx.MultiDiGraph

        Parameters
        ----------
        edges: List
            A list of edge records

        """
        start = self.current_time_in_millis()
        for record in edges:
            edge = record[1]
            self.load_edge(edge)
        end = self.current_time_in_millis()
        logging.debug("time taken to load edges: {} ms".format(end - start))

    def load_edge(self, edge: Relationship) -> None:
        """
        Load an edge from neo4jrestclient.client.Relationship into networkx.MultiDiGraph

        Parameters
        ----------
        edge: neo4jrestclient.client.Relationship
            An edge

        """
        edge_key = str(uuid.uuid4())
        edge_subject = edge.start
        edge_predicate = edge.properties
        edge_object = edge.end

        subject_id = edge_subject['id'] if 'id' in edge_subject else edge_subject.id
        object_id = edge_object['id'] if 'id' in edge_object else edge_object.id

        attributes = {}

        for key, value in edge_predicate.items():
            attributes[key] = value

        # TODO: Is this code residual from attempting to adapt to several drivers?
        if 'subject' not in attributes:
            attributes['subject'] = subject_id
        if 'object' not in attributes:
            attributes['object'] = object_id
        if 'edge_label' not in attributes:
            attributes['edge_label'] = edge.type

        if not self.graph.has_node(subject_id):
            self.load_node(edge_subject)

        if not self.graph.has_node(object_id):
            self.load_node(edge_object)

        self.graph.add_edge(subject_id, object_id, edge_key, **attributes)

    def get_pages(self, query_function, start: int = 0, end: int = None, page_size: int = 10_000, **kwargs) -> list:
        """
        Get pages of size `page_size` from Neo4j.
        Returns an iterator of pages where number of pages is (`end` - `start`)/`page_size`

        Parameters
        ----------
        query_function: func
            The function to use to fetch records. Usually this is `self.get_nodes` or `self.get_edges`
        start: int
            Start for pagination
        end: int
            End for pagination
        page_size: int
            Size of each page (`10000`, by default)
        **kwargs: dict
            Any additional arguments that might be relevant for `query_function`

        Returns
        -------
        list
            An iterator for a list of records from Neo4j. The size of the list is `page_size`

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

    def get_nodes(self, skip: int = 0, limit: int = 0) -> List[Node]:
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
            A list of neo4jrestclient.client.Node records

        """

        if limit == 0 or limit is None:
            query = f"""
            MATCH (n)
            WHERE n{self.get_filter('subject_category')} OR n{self.get_filter('object_category')}
            RETURN n
            SKIP {skip}
            """
        else:
            query = f"""
             MATCH (n)
             WHERE n{self.get_filter('subject_category')} OR n{self.get_filter('object_category')}
             RETURN n
             SKIP {skip} LIMIT {limit}
             """

        logging.debug(query)

        # Filter out all the associated metadata to ensure the results are clean
        try:
            results = self.http_driver.query(query, returns=Node)
        except CypherException as ce:
            logging.error(ce)
        logging.debug("Results: {}".format(results))
        nodes = [node for node in results]
        logging.debug("Tidied results: {}".format(nodes))
        return nodes

    def get_edges(self, skip: int = 0, limit: int = 0, is_directed: bool = True) -> List[Tuple[Node, Relationship, Node]]:
        """
        Get a page of edges from the Neo4j database.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for
        is_directed: bool
            Are edges directed or undirected (`True`, by default, since edges in most cases are directed)

        Returns
        -------
        list
            A list of 3-tuples of the form (neo4jrestclient.client.Node, neo4jrestclient.client.Relationship, neo4jrestclient.client.Node)

        """

        direction = '->' if is_directed else '-'
        query = f"""
        MATCH (s{self.get_filter('subject_category')})-[p{self.get_filter('edge_label')}]{direction}(o{self.get_filter('object_category')})
        RETURN s, p, o
        SKIP {skip}
        """

        if limit:
            query += f" LIMIT {limit}"

        if skip < limit:
            logging.debug(query)
            try:
                results = self.http_driver.query(query, returns=(Node, Relationship, Node))
            except CypherException as ce:
                logging.error(ce)
            edge_triples = [x for x in results]
            return edge_triples
        return []

    def save_node(self, obj: dict) -> None:
        """
        Load a node into Neo4j.

        Parameters
        ----------
        obj: dict
            A dictionary that represents a node and its properties.
            The node must have 'id' property. For all other necessary properties, refer to the BioLink Model.

        """
        obj = self.validate_node(obj)
        category = obj.pop('category')[0]

        properties = ', '.join('n.{0}=${0}'.format(k) for k in obj.keys())
        query = f"MERGE (n:`{category}` {{id: $id}}) SET {properties}"
        logging.debug(query)
        try:
            self.http_driver.query(query, params=obj)
        except CypherException as ce:
            logging.error(ce)

    def save_node_unwind(self, nodes_by_category: Dict[str, list]) -> None:
        """
        Save all nodes into Neo4j using the UNWIND cypher clause.

        Parameters
        ----------
        nodes_by_category: Dict[str, list]
            A dictionary where node category is the key and the value is a list of nodes of that category

        """
        for category in nodes_by_category.keys():
            logging.info("Generating UNWIND for category: {}".format(category))
            query = self.generate_unwind_node_query(category)
            logging.info(query)
            try:
                self.http_driver.query(query, params={'nodes': nodes_by_category[category]})
            except CypherException as ce:
                logging.error(ce)

    def generate_unwind_node_query(self, category: str) -> str:
        """
        Generate UNWIND cypher query for saving nodes into Neo4j.

        There should be a CONSTRAINT in Neo4j for `self.DEFAULT_NODE_LABEL`.
        The query uses `self.DEFAULT_NODE_LABEL` as the node label to increase speed for adding nodes.
        The query also sets label to `self.DEFAULT_NODE_LABEL` for any node to make sure that the CONSTRAINT applies.

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
        MERGE (n:`{self.DEFAULT_NODE_LABEL}` {{id: node.id}})
        ON CREATE SET n += node, n:{category}
        """

        return query

    def save_edge_unwind(self, edges_by_edge_label: Dict[str, list]) -> None:
        """
        Save all edges into Neo4j using the UNWIND cypher clause.

        Parameters
        ----------
        edges_by_edge_label: dict
            A dictionary where edge label is the key and the value is a list of edges with that edge label

        """
        for predicate in edges_by_edge_label:
            query = self.generate_unwind_edge_query(predicate)
            logging.info(query)
            edges = edges_by_edge_label[predicate]
            for i in range(0, len(edges), 1000):
                end = i + 1000
                subset = edges[i:end]
                logging.info("edges subset: {}-{} for predicate {}".format(i, end, predicate))
                time_start = self.current_time_in_millis()
                try:
                    self.http_driver.query(query, params={"relationship": predicate, "edges": subset})
                except CypherException as ce:
                    logging.error(ce)
                time_end = self.current_time_in_millis()
                logging.debug("time taken to load edges: {} ms".format(time_end - time_start))

    def generate_unwind_edge_query(self, edge_label: str) -> str:
        """
        Generate UNWIND cypher query for saving edges into Neo4j.

        Query uses `self.DEFAULT_NODE_LABEL` to quickly lookup the required subject and object node.

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
        MATCH (s:`{self.DEFAULT_NODE_LABEL}` {{id: edge.subject}}), (o:`{self.DEFAULT_NODE_LABEL}` {{id: edge.object}})
        MERGE (s)-[r:`{edge_label}`]->(o)
        SET r += edge
        """
        return query

    def save_edge(self, obj: dict) -> None:
        """
        Load an edge into Neo4j.

        Parameters
        ----------
        obj: dict
            A dictionary that represents an edge and its properties.
            The edge must have 'subject', 'edge_label' and 'object' properties. For all other necessary properties, refer to the BioLink Model.

        """
        obj = self.validate_edge(obj)
        edge_label = obj.pop('edge_label')

        properties = ', '.join('r.{0}=${0}'.format(k) for k in obj.keys())

        q = f"""
        MATCH (s {{id: $subject}}), (o {{id: $object}})
        MERGE (s)-[r:{edge_label}]->(o)
        SET {properties}
        """

        try:
            self.http_driver.query(q, params=obj)
        except CypherException as ce:
            logging.error(ce)

    def save_with_unwind(self) -> None:
        """
        Save all nodes and edges from networkx.MultiDiGraph into Neo4j using the UNWIND cypher clause.

        """
        nodes_by_category = {}

        for n in self.graph.nodes():
            node = self.graph.nodes[n]
            if 'id' not in node:
                logging.warning("Ignoring node as it does not have an 'id' property: {}".format(node))
                continue
            node = self.validate_node(node)
            category = ':'.join(node['category'])
            if category not in nodes_by_category:
                nodes_by_category[category] = [node]
            else:
                nodes_by_category[category].append(node)

        edges_by_edge_label = {}
        for n, nbrs in self.graph.adjacency():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    edge = self.validate_edge(adjitem)
                    if adjitem['edge_label'] not in edges_by_edge_label:
                        edges_by_edge_label[edge['edge_label']] = [edge]
                    else:
                        edges_by_edge_label[edge['edge_label']].append(edge)

        # create indexes
        self.create_constraints(set(nodes_by_category.keys()))
        # save all nodes
        self.save_node_unwind(nodes_by_category)
        # save all edges
        self.save_edge_unwind(edges_by_edge_label)

    def save(self) -> str:
        """
        Save all nodes and edges from networkx.MultiDiGraph into Neo4j.

        """
        categories = {self.DEFAULT_NODE_LABEL}
        for n in self.graph.nodes():
            node = self.graph.nodes[n]
            if 'category' in node:
                if isinstance(node['category'], list):
                    categories.update(node['category'])
                else:
                    categories.add(node['category'])

        self.create_constraints(categories)
        for node_id in self.graph.nodes():
            node_attributes = self.graph.node[node_id]
            if 'id' not in node_attributes:
                node_attributes['id'] = node_id
            self.save_node(node_attributes)
        for n, nbrs in self.graph.adjacency():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    self.save_edge(adjitem)
        self.neo4j_report()

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

    def create_constraints(self, categories: set) -> None:
        """
        Create a unique constraint on node 'id' for all `categories` in Neo4j.

        Parameters
        ----------
        categories: set
            Set of categories

        """
        query = "CREATE CONSTRAINT ON (n:`{}`) ASSERT n.id IS UNIQUE"
        label_set = {Transformer.DEFAULT_NODE_LABEL}

        for label in categories:
            if ':' in label:
                sub_labels = label.split(':')
                for sublabel in sub_labels:
                    label_set.add(sublabel)
            else:
                label_set.add(label)

        for label in label_set:
            try:
                self.http_driver.query(query.format(label))
            except CypherException as ce:
                logging.error(ce)

    def get_filter(self, key: str) -> str:
        """
        Get the value for filter as defined by `key`.
        This is used as a convenience method for generating cypher queries.

        Parameters
        ----------
        key: str
            Name of the filter

        Returns
        -------
        str
            Value corresponding to the given filter `key`, formatted for CQL
        """
        value = ''
        if key in self.filters and len(self.filters[key]) != 0:
            value = f":`{self.filters[key]}`"
        return value
