import itertools
import typing
from typing import Any, Dict, List, Optional, Iterator, Tuple, Generator

from arango import ArangoClient

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    sanitize_import,
    knowledge_provenance_properties,
)

log = get_logger()


class ArangoSource(Source):
    """
    ArangoSource is responsible for reading data as records
    from an ArangoDB instance.
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.db = None
        self.node_count = 0
        self.edge_count = 0
        self.seen_nodes = set()
        self.use_arango_id = False
        self.document_id_property = "document_id"

    def _connect_db(self, uri: str, database: str, username: str, password: str):
        """
        Connect to an ArangoDB instance.

        Parameters
        ----------
        uri: str
            The URI for the ArangoDB instance.
            For example, http://localhost:8529
        database: str
            The database name
        username: str
            The username
        password: str
            The password
        """
        client = ArangoClient(hosts=uri)
        self.db = client.db(database, username=username, password=password)

    def _discover_collections(self) -> Tuple[List[str], List[str]]:
        """
        Discover all non-system document and edge collections in the database.

        Returns
        -------
        Tuple[List[str], List[str]]
            A tuple of (document_collection_names, edge_collection_names)
        """
        doc_collections = []
        edge_collections = []
        for col in self.db.collections():
            if col["system"]:
                continue
            col_type = col["type"]
            if col_type == 2 or col_type == "document":
                doc_collections.append(col["name"])
            elif col_type == 3 or col_type == "edge":
                edge_collections.append(col["name"])
        doc_collections.sort()
        edge_collections.sort()
        log.info(
            f"Discovered {len(doc_collections)} document collections, {len(edge_collections)} edge collections"
        )
        return doc_collections, edge_collections

    def parse(
        self,
        uri: str,
        database: str,
        username: str,
        password: str,
        node_filters: Dict = None,
        edge_filters: Dict = None,
        start: int = 0,
        end: int = None,
        page_size: int = 50000,
        node_collection: str = "nodes",
        edge_collection: str = "edges",
        node_collections: List[str] = None,
        edge_collections: List[str] = None,
        all_collections: bool = False,
        use_arango_id: bool = False,
        document_id_property: str = "document_id",
        **kwargs: Any,
    ) -> typing.Generator:
        """
        Read from an ArangoDB instance and yield records.

        Parameters
        ----------
        uri: str
            The URI for the ArangoDB instance.
            For example, http://localhost:8529
        database: str
            The database name
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
        page_size: int
            The size of each page/batch fetched from ArangoDB (``50000``)
        node_collection: str
            The name of a single vertex collection (``nodes``).
            Ignored if ``node_collections`` or ``all_collections`` is set.
        edge_collection: str
            The name of a single edge collection (``edges``).
            Ignored if ``edge_collections`` or ``all_collections`` is set.
        node_collections: List[str]
            A list of vertex collection names to export
        edge_collections: List[str]
            A list of edge collection names to export
        all_collections: bool
            If True, discover and export all non-system collections
        use_arango_id: bool
            If True, derive every node ``id``, and every edge ``subject`` and
            ``object``, from the ArangoDB document handle (``_id``,
            ``_from``, ``_to``; e.g., ``CL/1000300`` becomes ``CL:1000300``),
            even when the document stores its own ``id`` or the edge its own
            ``subject``/``object``.  Use this for databases not written by
            KGX, where handles are unique by construction but an ``id``
            attribute is ordinary data that need not be unique.  A node's own
            ``id`` value is kept under ``document_id_property``.  If False
            (the default), a stored ``id`` is used as is, which is right for
            databases written by ``ArangoSink``.
        document_id_property: str
            The property that receives a node's own ``id`` value when
            ``use_arango_id`` is True (``document_id``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records
        """
        self._connect_db(uri, database, username, password)
        self.use_arango_id = use_arango_id
        self.document_id_property = document_id_property

        self.set_provenance_map(kwargs)

        if self.node_filters is not None:
            self.node_filters = node_filters
        if self.edge_filters is not None:
            self.edge_filters = edge_filters

        # Determine which collections to export
        if all_collections:
            node_cols, edge_cols = self._discover_collections()
        elif node_collections or edge_collections:
            node_cols = list(node_collections) if node_collections else []
            edge_cols = list(edge_collections) if edge_collections else []
        else:
            node_cols = [node_collection]
            edge_cols = [edge_collection]

        for nc in node_cols:
            log.info(f"Reading nodes from collection: {nc}")
            for page in self.get_pages(
                self.get_nodes,
                start,
                end,
                page_size=page_size,
                node_collection=nc,
                **kwargs,
            ):
                yield from self.load_nodes(page)

        for ec in edge_cols:
            log.info(f"Reading edges from collection: {ec}")
            for page in self.get_pages(
                self.get_edges,
                start,
                end,
                page_size=page_size,
                edge_collection=ec,
                **kwargs,
            ):
                yield from self.load_edges(page)

    def get_nodes(
        self,
        skip: int = 0,
        limit: int = 0,
        node_collection: str = "nodes",
        **kwargs: Any,
    ) -> List:
        """
        Get a page of nodes from ArangoDB.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for
        node_collection: str
            The name of the vertex collection
        kwargs: Any
            Any additional arguments

        Returns
        -------
        List
            A list of nodes
        """
        filter_clause, bind_vars = self.build_aql_node_filter(self.node_filters)

        query = f"FOR doc IN `{node_collection}` {filter_clause} LIMIT @offset, @limit RETURN UNSET(doc, '_rev')"
        bind_vars["offset"] = skip
        bind_vars["limit"] = limit if limit else page_size_default(limit)

        log.debug(query)
        nodes = []
        try:
            cursor = self.db.aql.execute(query, bind_vars=bind_vars)
            for doc in cursor:
                nodes.append(self._prepare_node(doc))
        except Exception as e:
            log.error(e)
        return nodes

    def _prepare_node(self, doc: Dict) -> Dict:
        """
        Turn an ArangoDB document into a node record.

        The CURIE is reconstructed from the document handle (per-ontology
        collection convention), e.g., ``_id`` ``CL/1000300`` becomes
        ``CL:1000300``.  It becomes the node ``id`` if the document stores no
        ``id``, or always if ``use_arango_id`` is set, in which case a stored
        ``id`` moves to ``document_id_property``.

        Parameters
        ----------
        doc: Dict
            An ArangoDB document, including ``_id`` and ``_key``

        Returns
        -------
        Dict
            The node record
        """
        arango_id = doc.pop("_id")
        doc.pop("_key", None)
        curie = _arango_ref_to_curie(arango_id)
        if self.use_arango_id:
            if "id" in doc:
                if self.document_id_property in doc:
                    log.warning(
                        f"{arango_id}: '{self.document_id_property}' is already set; "
                        f"dropping the document's own id {doc['id']!r}"
                    )
                else:
                    doc[self.document_id_property] = doc["id"]
            doc["id"] = curie
        elif "id" not in doc:
            doc["id"] = curie
        doc.setdefault("name", "")
        doc.setdefault("category", ["biolink:NamedThing"])
        return doc

    def get_edges(
        self,
        skip: int = 0,
        limit: int = 0,
        edge_collection: str = "edges",
        **kwargs: Any,
    ) -> List:
        """
        Get a page of edges from ArangoDB.

        Parameters
        ----------
        skip: int
            Records to skip
        limit: int
            Total number of records to query for
        edge_collection: str
            The name of the edge collection
        kwargs: Any
            Any additional arguments

        Returns
        -------
        List
            A list of 3-tuples (subject, edge, object)
        """
        filter_clause, bind_vars = self.build_aql_edge_filter(self.edge_filters)

        query = (
            f"FOR edge IN `{edge_collection}` "
            f"LET s = DOCUMENT(edge._from) "
            f"LET o = DOCUMENT(edge._to) "
            f"{filter_clause} "
            f"LIMIT @offset, @limit "
            f"RETURN {{"
            f"subject: UNSET(s, '_rev'), "
            f"edge: UNSET(edge, '_rev', '_key'), "
            f"object: UNSET(o, '_rev')"
            f"}}"
        )
        bind_vars["offset"] = skip
        bind_vars["limit"] = limit if limit else page_size_default(limit)

        log.debug(query)
        edges = []
        try:
            cursor = self.db.aql.execute(query, bind_vars=bind_vars)
            for record in cursor:
                subject_node = record["subject"]
                edge_data = record["edge"]
                object_node = record["object"]

                if subject_node is None or object_node is None:
                    log.warning(
                        f"Skipping edge with missing subject or object: {edge_data}"
                    )
                    continue

                # The endpoints are the _from/_to documents, so their CURIEs
                # follow the same rule as nodes read from their collections.
                edge_data.pop("_from", None)
                edge_data.pop("_to", None)
                subject_node = self._prepare_node(subject_node)
                object_node = self._prepare_node(object_node)
                if self.use_arango_id:
                    edge_data["subject"] = subject_node["id"]
                    edge_data["object"] = object_node["id"]

                edge_data.setdefault("predicate", "biolink:related_to")
                edge_data.setdefault("relation", "biolink:related_to")

                edges.append([subject_node, edge_data, object_node])
        except Exception as e:
            log.error(e)

        return edges

    def load_nodes(self, nodes: List) -> Generator:
        """
        Load nodes into an instance of BaseGraph.

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
        Load a node into an instance of BaseGraph.

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
        Load edges into an instance of BaseGraph.

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
        Load an edge into an instance of BaseGraph.

        Parameters
        ----------
        edge_record: List
            A 3-element list [subject_data, edge_data, object_data]

        Returns
        -------
        Tuple
            A tuple with subject ID, object ID, edge key, and edge data
        """
        subject_node = edge_record[0]
        edge_data = edge_record[1]
        object_node = edge_record[2]
        arango_id = edge_data.pop("_id", None)

        self.set_edge_provenance(edge_data)

        if "id" not in edge_data.keys():
            edge_data["id"] = generate_uuid()

        edge_data = self.validate_edge(edge_data)
        if not edge_data:
            return ()

        edge_data = sanitize_import(edge_data.copy())
        self.edge_properties.update(edge_data.keys())
        # Key the edge by its document handle, which is unique, rather than
        # by (subject, predicate, object): the predicate defaults to the
        # collection name, so edges between the same nodes in one collection
        # would otherwise overwrite each other in a GraphSink, which keys
        # edges by a 'key' field when there is one and does not store it.
        # It is not registered as an edge property, so it adds no column to
        # a TSV export.
        if arango_id:
            if "key" in edge_data:
                log.warning(
                    f"{arango_id}: replacing its 'key' attribute "
                    f"{edge_data['key']!r} with the document handle"
                )
            edge_data["key"] = arango_id
            key = arango_id
        else:
            key = generate_edge_key(
                subject_node["id"], edge_data["predicate"], object_node["id"]
            )
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
        Get pages of size ``page_size`` from ArangoDB.
        Returns an iterator of pages where number of pages is
        (``end`` - ``start``)/``page_size``

        Parameters
        ----------
        query_function: func
            The function to use to fetch records
        start: int
            Start for pagination
        end: Optional[int]
            End for pagination
        page_size: int
            Size of each page (``50000``, by default)
        kwargs: Dict
            Any additional arguments that might be relevant for ``query_function``

        Returns
        -------
        Iterator
            An iterator for a list of records from ArangoDB
        """
        for i in itertools.count(0):
            skip = start + (page_size * i)
            limit = page_size if end is None or skip + page_size <= end else end - skip
            if limit <= 0:
                return
            records = query_function(skip=skip, limit=limit, **kwargs)
            if records:
                yield records
            else:
                return

    @staticmethod
    def build_aql_node_filter(node_filters: Dict) -> Tuple[str, Dict]:
        """
        Build an AQL FILTER clause for node queries.

        Parameters
        ----------
        node_filters: Dict
            Node filters

        Returns
        -------
        Tuple[str, Dict]
            A tuple of (AQL filter string, bind variables dict)
        """
        if not node_filters:
            return "", {}

        clauses = []
        bind_vars = {}

        if "category" in node_filters and node_filters["category"]:
            values = node_filters["category"]
            if isinstance(values, (list, set, tuple)):
                bind_vars["cat_values"] = list(values)
            elif isinstance(values, str):
                bind_vars["cat_values"] = [values]
            clauses.append(
                "HAS(doc, 'provided_by') AND IS_LIST(doc.provided_by) AND @prov_values ANY IN doc.provided_by"
            )

        if "provided_by" in node_filters and node_filters["provided_by"]:
            values = node_filters["provided_by"]
            if isinstance(values, (list, set, tuple)):
                bind_vars["prov_values"] = list(values)
            elif isinstance(values, str):
                bind_vars["prov_values"] = [values]
            clauses.append(
                "HAS(doc, 'provided_by') AND IS_LIST(doc.provided_by) AND @prov_values ANY IN doc.provided_by"
            )

        if clauses:
            return "FILTER " + " AND ".join(clauses), bind_vars
        return "", {}

    @staticmethod
    def build_aql_edge_filter(edge_filters: Dict) -> Tuple[str, Dict]:
        """
        Build an AQL FILTER clause for edge queries.

        Parameters
        ----------
        edge_filters: Dict
            Edge filters

        Returns
        -------
        Tuple[str, Dict]
            A tuple of (AQL filter string, bind variables dict)
        """
        if not edge_filters:
            return "", {}

        clauses = []
        bind_vars = {}

        if "subject_category" in edge_filters and edge_filters["subject_category"]:
            values = edge_filters["subject_category"]
            if isinstance(values, (list, set, tuple)):
                bind_vars["subj_cat_values"] = list(values)
            else:
                bind_vars["subj_cat_values"] = [values]
            clauses.append(
                "s.category != null AND IS_LIST(s.category) AND "
                "LENGTH(INTERSECTION(s.category, @subj_cat_values)) > 0"
            )

        if "object_category" in edge_filters and edge_filters["object_category"]:
            values = edge_filters["object_category"]
            if isinstance(values, (list, set, tuple)):
                bind_vars["obj_cat_values"] = list(values)
            else:
                bind_vars["obj_cat_values"] = [values]
            clauses.append(
                "o.category != null AND IS_LIST(o.category) AND "
                "LENGTH(INTERSECTION(o.category, @obj_cat_values)) > 0"
            )

        if "predicate" in edge_filters and edge_filters["predicate"]:
            values = edge_filters["predicate"]
            if isinstance(values, (list, set, tuple)):
                bind_vars["pred_values"] = list(values)
            else:
                bind_vars["pred_values"] = [values]
            clauses.append("edge.predicate IN @pred_values")

        for ksf in knowledge_provenance_properties:
            if ksf in edge_filters and edge_filters[ksf]:
                values = edge_filters[ksf]
                var_name = f"ksf_{ksf.replace('.', '_')}"
                if isinstance(values, (list, set, tuple)):
                    bind_vars[var_name] = list(values)
                else:
                    bind_vars[var_name] = [values]
                clauses.append(
                    f"edge.{ksf} != null AND IS_LIST(edge.{ksf}) AND "
                    f"@{var_name} ANY IN edge.{ksf}"
                )

        if clauses:
            return "FILTER " + " AND ".join(clauses), bind_vars
        return "", {}


def _arango_ref_to_curie(ref: str) -> str:
    """
    Convert an ArangoDB document reference to a CURIE.

    For example, ``CL/1000302`` becomes ``CL:1000302``.

    Parameters
    ----------
    ref: str
        An ArangoDB document reference (e.g., ``collection/key``)

    Returns
    -------
    str
        A CURIE string
    """
    if "/" in ref:
        collection, key = ref.split("/", 1)
        return f"{collection}:{key}"
    return ref


def page_size_default(limit: int) -> int:
    """Return a sensible default when limit is 0 (meaning unlimited)."""
    return 50000 if limit == 0 else limit
