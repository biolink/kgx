import itertools
from typing import Any, Dict, List, Optional, Iterator, Tuple, Generator

from neo4jrestclient.client import Node, Relationship, GraphDatabase
from neo4jrestclient.query import CypherException

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    validate_node,
    validate_edge,
    sanitize_import,
    knowledge_provenance_properties,
)

log = get_logger()


class NeoSource(Source):
    """
    NeoSource is responsible for reading data as records
    from a Neo4j instance.
    """

    def __init__(self):
        super().__init__()
        self.http_driver = None
        self.node_count = 0
        self.edge_count = 0
        self.seen_nodes = set()

    def parse(
        self,
        uri: str,
        username: str,
        password: str,
        node_filters: Dict = None,
        edge_filters: Dict = None,
        start: int = 0,
        end: int = None,
        is_directed: bool = True,
        page_size: int = 50000,
        **kwargs: Any,
    ) -> Generator:
        """
        This method reads from Neo4j instance and yields records

        Parameters
        ----------
        uri: str
            The URI for the Neo4j instance.
            For example, http://localhost:7474
        username: str
            The username
        password: str
            The password
        node_filters: Dict
            Node filters
        edge_filters: Dict
            Edge filters
        start: int
            Number of records to skip before streaming
        end: int
            Total number of records to fetch
        is_directed: bool
            Whether or not the edges should be treated as directed
        page_size: int
            The size of each page/batch fetched from Neo4j (``50000``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """
        self.http_driver: GraphDatabase = GraphDatabase(
            uri, username=username, password=password
        )

        self.set_provenance_map(kwargs)

        kwargs["is_directed"] = is_directed
        self.node_filters = node_filters
        self.edge_filters = edge_filters
        for page in self.get_pages(
            self.get_nodes, start, end, page_size=page_size, **kwargs
        ):
            yield from self.load_nodes(page)
        for page in self.get_pages(
            self.get_edges, start, end, page_size=page_size, **kwargs
        ):
            yield from self.load_edges(page)

    def count(self, is_directed: bool = True) -> int:
        """
        Get the total count of records to be fetched from the Neo4j database.

        Parameters
        ----------
        is_directed: bool
            Are edges directed or undirected.
            ``True``, by default, since edges in most cases are directed.

        Returns
        -------
        int
            The total count of records

        """
        direction = "->" if is_directed else "-"
        query = f"MATCH (s)-[p]{direction}(o)"

        if self.edge_filters:
            qs = []
            if "subject_category" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'subject_category', 's', ':', 'OR')})"
                )
            if "object_category" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'object_category', 'o', ':', 'OR')})"
                )
            if "predicate" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'predicate', 'p', '.')})"
                )
            for ksf in knowledge_provenance_properties:
                if ksf in self.edge_filters:
                    qs.append(
                        f"({self.format_edge_filter(self.edge_filters, ksf, 'p', '.', 'OR')})"
                    )
            query = " WHERE "
            query += " AND ".join(qs)
        query += f" RETURN COUNT(*) AS count"
        log.debug(query)
        query_result: Any
        counts: int = 0
        try:
            query_result = self.http_driver.query(query)
            for result in query_result:
                counts = result[0]
        except CypherException as ce:
            log.error(ce)
        return counts

    def get_nodes(self, skip: int = 0, limit: int = 0, **kwargs: Any) -> List:
        """
        Get a page of nodes from the Neo4j database.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for
        kwargs: Any
            Any additional arguments

        Returns
        -------
        List
            A list of nodes

        """
        query = f"MATCH (n)"

        if self.node_filters:
            qs = []
            if "category" in self.node_filters:
                qs.append(
                    f"({self.format_node_filter(self.node_filters, 'category', 'n', ':', 'OR')})"
                )
            if "provided_by" in self.node_filters:
                qs.append(
                    f"({self.format_node_filter(self.node_filters, 'provided_by', 'n', '.', 'OR')})"
                )
            query += " WHERE "
            query += " AND ".join(qs)

        query += f" RETURN n SKIP {skip}"

        if limit:
            query += f" LIMIT {limit}"

        log.debug(query)
        nodes = []
        try:
            results = self.http_driver.query(query, returns=Node, data_contents=True)
            if results:
                nodes = [node[0] for node in results.rows]
        except CypherException as ce:
            log.error(ce)
        return nodes

    def get_edges(
        self, skip: int = 0, limit: int = 0, is_directed: bool = True, **kwargs: Any
    ) -> List:
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
        kwargs: Any
            Any additional arguments

        Returns
        -------
        List
            A list of 3-tuples

        """
        direction = "->" if is_directed else "-"
        query = f"MATCH (s)-[p]{direction}(o)"

        if self.edge_filters:
            qs = []
            if "subject_category" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'subject_category', 's', ':', 'OR')})"
                )
            if "object_category" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'object_category', 'o', ':', 'OR')})"
                )
            if "predicate" in self.edge_filters:
                qs.append(
                    f"({self.format_edge_filter(self.edge_filters, 'predicate', 'p', '.')})"
                )
            for ksf in knowledge_provenance_properties:
                if ksf in self.edge_filters:
                    qs.append(
                        f"({self.format_edge_filter(self.edge_filters, ksf, 'p', '.', 'OR')})"
                    )
            query += " WHERE "
            query += " AND ".join(qs)
        query += f" RETURN s, p, o SKIP {skip}"

        if limit:
            query += f" LIMIT {limit}"

        log.debug(query)
        edges = []
        try:
            results = self.http_driver.query(
                query, returns=(Node, Relationship, Node), data_contents=True
            )
            if results:
                edges = [x for x in results.rows]
        except CypherException as ce:
            log.error(ce)

        return edges

    def load_nodes(self, nodes: List) -> None:
        """
        Load nodes into an instance of BaseGraph

        Parameters
        ----------
        nodes: List
            A list of nodes

        """
        for node in nodes:
            if node["id"] not in self.seen_nodes:
                yield self.load_node(node)

    def load_node(self, node: Dict) -> Tuple:
        """
        Load node into an instance of BaseGraph

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        Tuple
            A tuple with node ID and node data

        """
        self.node_count += 1
        # TODO: remove the seen_nodes
        self.seen_nodes.add(node["id"])

        self.set_node_provenance(node)

        node = validate_node(node)
        node = sanitize_import(node.copy())
        self.node_properties.update(node.keys())
        return node["id"], node

    def load_edges(self, edges: List) -> None:
        """
        Load edges into an instance of BaseGraph

        Parameters
        ----------
        edges: List
            A list of edge records

        """
        for record in edges:
            self.edge_count += 1
            subject_node = record[0]
            edge = record[1]
            object_node = record[2]

            if "subject" not in edge:
                edge["subject"] = subject_node["id"]
            if "object" not in edge:
                edge["object"] = object_node["id"]

            s = self.load_node(subject_node)
            o = self.load_node(object_node)
            objs = [s, o]
            objs.append(self.load_edge([s[1], edge, o[1]]))
            for o in objs:
                yield o

    def load_edge(self, edge_record: List) -> Tuple:
        """
        Load an edge into an instance of BaseGraph

        Parameters
        ----------
        edge_record: List
            A 4-tuple edge record

        Returns
        -------
        Tuple
            A tuple with subject ID, object ID, edge key, and edge data

        """

        subject_node = edge_record[0]
        edge = edge_record[1]
        object_node = edge_record[2]

        self.set_edge_provenance(edge)

        if "id" not in edge.keys():
            edge["id"] = generate_uuid()
        key = generate_edge_key(
            subject_node["id"], edge["predicate"], object_node["id"]
        )
        edge = validate_edge(edge)
        edge = sanitize_import(edge.copy())
        self.edge_properties.update(edge.keys())
        return subject_node["id"], object_node["id"], key, edge

    def get_pages(
        self,
        query_function,
        start: int = 0,
        end: Optional[int] = None,
        page_size: int = 50000,
        **kwargs: Any,
    ) -> Iterator:
        """
        Get pages of size ``page_size`` from Neo4j.
        Returns an iterator of pages where number of pages is (``end`` - ``start``)/``page_size``

        Parameters
        ----------
        query_function: func
            The function to use to fetch records. Usually this is ``self.get_nodes`` or ``self.get_edges``
        start: int
            Start for pagination
        end: Optional[int]
            End for pagination
        page_size: int
            Size of each page (``10000``, by default)
        kwargs: Dict
            Any additional arguments that might be relevant for ``query_function``

        Returns
        -------
        Iterator
            An iterator for a list of records from Neo4j. The size of the list is ``page_size``

        """
        # TODO: use async
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

    @staticmethod
    def format_node_filter(
        node_filters: Dict,
        key: str,
        variable: Optional[str] = None,
        prefix: Optional[str] = None,
        op: Optional[str] = None,
    ) -> str:
        """
        Get the value for node filter as defined by ``key``.
        This is used as a convenience method for generating cypher queries.

        Parameters
        ----------
        node_filters: Dict
            All node filters
        key: str
            Name of the node filter
        variable: Optional[str]
            Variable binding for cypher query
        prefix: Optional[str]
            Prefix for the cypher
        op: Optional[str]
            The operator

        Returns
        -------
        str
            Value corresponding to the given node filter ``key``, formatted for CQL

        """
        value = ""
        if key in node_filters and node_filters[key]:
            if isinstance(node_filters[key], (list, set, tuple)):
                if key in {"category"}:
                    formatted = [f"{variable}{prefix}`{x}`" for x in node_filters[key]]
                    value = f" {op} ".join(formatted)
                elif key == "provided_by":
                    formatted = [
                        f"'{x}' IN {variable}{prefix}{'provided_by'}"
                        for x in node_filters["provided_by"]
                    ]
                    value = f" {op} ".join(formatted)
                else:
                    formatted = []
                    for v in node_filters[key]:
                        formatted.append(f"{variable}{prefix}{key} = '{v}'")
                    value = f" {op} ".join(formatted)
            elif isinstance(node_filters[key], str):
                value = f"{variable}{prefix}{key} = '{node_filters[key]}'"
            else:
                log.error(
                    f"Unexpected {key} node filter of type {type(node_filters[key])}"
                )
        return value

    @staticmethod
    def format_edge_filter(
        edge_filters: Dict,
        key: str,
        variable: Optional[str] = None,
        prefix: Optional[str] = None,
        op: Optional[str] = None,
    ) -> str:
        """
        Get the value for edge filter as defined by ``key``.
        This is used as a convenience method for generating cypher queries.

        Parameters
        ----------
        edge_filters: Dict
            All edge filters
        key: str
            Name of the edge filter
        variable: Optional[str]
            Variable binding for cypher query
        prefix: Optional[str]
            Prefix for the cypher
        op: Optional[str]
            The operator

        Returns
        -------
        str
            Value corresponding to the given edge filter ``key``, formatted for CQL

        """
        value = ""
        if key in edge_filters and edge_filters[key]:
            if isinstance(edge_filters[key], (list, set, tuple)):
                if key in {"subject_category", "object_category"}:
                    formatted = [f"{variable}{prefix}`{x}`" for x in edge_filters[key]]
                    value = f" {op} ".join(formatted)
                elif key == "predicate":
                    formatted = [f"'{x}'" for x in edge_filters["predicate"]]
                    value = f"type({variable}) IN [{', '.join(formatted)}]"
                elif key in knowledge_provenance_properties:
                    formatted = [
                        f"'{x}' IN {variable}{prefix}{key}" for x in edge_filters[key]
                    ]
                    value = f" {op} ".join(formatted)
                else:
                    formatted = []
                    for v in edge_filters[key]:
                        formatted.append(f"{variable}{prefix}{key} = '{v}'")
                    value = f" {op} ".join(formatted)
            elif isinstance(edge_filters[key], str):
                value = f"{variable}{prefix}{key} = '{edge_filters[key]}'"
            else:
                log.error(
                    f"Unexpected {key} edge filter of type {type(edge_filters[key])}"
                )
        return value
