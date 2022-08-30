import itertools
import typing
from typing import Any, Dict, List, Optional, Iterator, Tuple, Generator

from neo4j import GraphDatabase, Neo4jDriver
from neo4j.graph import Node, Relationship

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    sanitize_import,
    knowledge_provenance_properties,
)

log = get_logger()


class NeoSource(Source):
    """
    NeoSource is responsible for reading data as records
    from a Neo4j instance.
    """

    def __init__(self, owner):
        super().__init__(owner)        
        self.http_driver: Optional[Neo4jDriver] = None
        self.session = None
        self.node_count = 0
        self.edge_count = 0
        self.seen_nodes = set()

    def _connect_db(self, uri: str, username: str, password: str):
        self.http_driver = GraphDatabase.driver(
            uri, auth=(username, password)
        )
        self.session = self.http_driver.session()

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
    ) -> typing.Generator:
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
        self._connect_db(uri, username, password)

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
            query_result = self.session.run(query)
            for result in query_result:
                counts = result[0]
        except Exception as e:
            log.error(e)
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
            results = self.session.run(query)
            if results:
                nodes = [
                    {
                        "id": node[0].get('id', f"{node[0].id}"),
                        "name": node[0].get('name', ''),
                        "category": node[0].get('category', ['biolink:NamedThing'])
                    }
                    for node in results.values()
                ]

        except Exception as e:
            log.error(e)
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
            results = self.session.run(
                query
            )
            if results:
                edges = list()
                for entry in results.values():
                    edge = list()
                    # subject
                    edge.append(
                        {
                            "id": entry[0].get('id', f"{entry[0].id}"),
                            "name": entry[0].get('name', ''),
                            "category": entry[0].get('category', ['biolink:NamedThing'])
                        }
                    )

                    # edge
                    edge.append(
                        {
                             "subject":  entry[1].get('subject', f"{entry[0].id}"),
                             "predicate": entry[1].get('predicate', "biolink:related_to"),
                             "relation": entry[1].get('relation', "biolink:related_to"),
                             "object": entry[1].get('object', f"{entry[2].id}")
                        }
                    )

                    # object
                    edge.append(
                        {
                            "id": entry[2].get('id', f"{entry[2].id}"),
                            "name": entry[2].get('name', ''),
                            "category": entry[2].get('category', ['biolink:NamedThing'])
                        }
                    )
                    edges.append(edge)
        except Exception as e:
            log.error(e)

        return edges

    def load_nodes(self, nodes: List) -> Generator:
        """
        Load nodes into an instance of BaseGraph

        Parameters
        ----------
        nodes: List
            A list of nodes

        """
        for node_data in nodes:
            if node_data["id"] not in self.seen_nodes:

                node_data = self.load_node(node_data)
                if not node_data:
                    continue

                yield node_data


    def load_node(self, node_data: Dict) -> Optional[Tuple]:
        """
        Load node into an instance of BaseGraph

        Parameters
        ----------
        node_data: Dict
            A node

        Returns
        -------
        Tuple
            A tuple with node ID and node data

        """
        self.node_count += 1
        self.seen_nodes.add(node_data["id"])

        self.set_node_provenance(node_data)

        node_data = self.validate_node(node_data)
        if not node_data:
            return None

        node_data = sanitize_import(node_data.copy())
        self.node_properties.update(node_data.keys())
        return node_data["id"], node_data

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
            if not s:
                continue

            o = self.load_node(object_node)
            if not o:
                continue

            objs = [s, o]

            edge_data = self.load_edge([s[1], edge, o[1]])
            if not edge_data:
                continue

            objs.append(edge_data)

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
        edge_data = edge_record[1]
        object_node = edge_record[2]

        self.set_edge_provenance(edge_data)

        if "id" not in edge_data.keys():
            edge_data["id"] = generate_uuid()
        key = generate_edge_key(
            subject_node["id"], edge_data["predicate"], object_node["id"]
        )

        edge_data = self.validate_edge(edge_data)
        if not edge_data:
            return ()

        edge_data = sanitize_import(edge_data.copy())
        self.edge_properties.update(edge_data.keys())
        return subject_node["id"], object_node["id"], key, edge_data

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
