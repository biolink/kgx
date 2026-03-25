from typing import Any, Dict, List, Tuple

from arango import ArangoClient

from kgx.config import get_logger
from kgx.error_detection import ErrorType
from kgx.sink.sink import Sink

log = get_logger()


class ArangoSink(Sink):
    """
    ArangoSink is responsible for writing data as records
    to an ArangoDB instance.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the Sink belongs
    uri: str
        The URI for the ArangoDB instance.
        For example, http://localhost:8529
    database: str
        The database name
    username: str
        The username
    password: str
        The password
    node_collection: str
        The name of the default vertex collection (``nodes``)
    edge_collection: str
        The name of the default edge collection (``edges``)
    curie_routing: bool
        If True, route nodes and edges to per-prefix collections
        derived from CURIE IDs. For example, a node with id
        ``CL:1000300`` is stored in collection ``CL`` with
        ``_key`` ``1000300``, and an edge from ``CL:...`` to
        ``UBERON:...`` is stored in collection ``CL-UBERON``.
    kwargs: Any
        Any additional arguments
    """

    CACHE_SIZE = 100000
    BATCH_SIZE = 10000

    def __init__(
        self,
        owner,
        uri: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        node_collection: str = "nodes",
        edge_collection: str = "edges",
        curie_routing: bool = False,
        **kwargs: Any,
    ):
        if "cache_size" in kwargs:
            self.CACHE_SIZE = kwargs["cache_size"]
        client = ArangoClient(hosts=uri)
        self.db = client.db(database, username=username, password=password)
        self.node_collection_name = node_collection
        self.edge_collection_name = edge_collection
        self.curie_routing = curie_routing
        self.node_cache: Dict[str, List] = {}
        self.edge_cache: List = []
        self.node_count = 0
        self.edge_count = 0
        self._vertex_collections: Dict = {}
        self._edge_collections: Dict = {}
        if not curie_routing:
            self._ensure_collections()
        super().__init__(owner)

    def _ensure_collections(self):
        """
        Ensure the default node and edge collections exist.
        """
        self._get_or_create_vertex_collection(self.node_collection_name)
        self._get_or_create_edge_collection(self.edge_collection_name)

    def _get_or_create_vertex_collection(self, name: str):
        """
        Get or create a vertex collection.

        Parameters
        ----------
        name: str
            Collection name

        Returns
        -------
        Collection
            The ArangoDB collection
        """
        if name not in self._vertex_collections:
            if not self.db.has_collection(name):
                col = self.db.create_collection(name)
            else:
                col = self.db.collection(name)
            # Ensure a persistent index on 'id' for fast lookups
            col.add_persistent_index(fields=["id"], unique=True)
            self._vertex_collections[name] = col
        return self._vertex_collections[name]

    def _get_or_create_edge_collection(self, name: str):
        """
        Get or create an edge collection.

        Parameters
        ----------
        name: str
            Collection name

        Returns
        -------
        Collection
            The ArangoDB edge collection
        """
        if name not in self._edge_collections:
            if not self.db.has_collection(name):
                col = self.db.create_collection(name, edge=True)
            else:
                col = self.db.collection(name)
            self._edge_collections[name] = col
        return self._edge_collections[name]

    @staticmethod
    def _split_curie(curie: str) -> Tuple[str, str]:
        """
        Split a CURIE into prefix and local ID.

        For example, ``CL:1000300`` becomes ``("CL", "1000300")``.

        Parameters
        ----------
        curie: str
            A CURIE string

        Returns
        -------
        Tuple[str, str]
            A tuple of (prefix, local_id). If no colon is found,
            returns ("", curie).
        """
        if ":" in curie:
            prefix, local_id = curie.split(":", 1)
            return prefix, local_id
        return "", curie

    def write_node(self, record) -> None:
        """
        Cache a node record that is to be written to ArangoDB.
        This method writes a cache of node records when the
        total number of records exceeds ``CACHE_SIZE``.

        Parameters
        ----------
        record: Dict
            A node record
        """
        if self.node_count >= self.CACHE_SIZE:
            self._flush_node_cache()

        if self.curie_routing:
            prefix, local_id = self._split_curie(record["id"])
            target_collection = prefix if prefix else self.node_collection_name
            record["_key"] = local_id
        else:
            target_collection = record.pop("_collection", self.node_collection_name)
            record["_key"] = self._sanitize_key(record["id"])

        record["_collection"] = target_collection

        if target_collection not in self.node_cache:
            self.node_cache[target_collection] = [record]
        else:
            self.node_cache[target_collection].append(record)
        self.node_count += 1

    def write_edge(self, record) -> None:
        """
        Cache an edge record that is to be written to ArangoDB.
        This method writes a cache of edge records when the
        total number of records exceeds ``CACHE_SIZE``.

        Parameters
        ----------
        record: Dict
            An edge record
        """
        if self.edge_count >= self.CACHE_SIZE:
            self._flush_edge_cache()

        subject_id = record.get("subject", "")
        object_id = record.get("object", "")

        if self.curie_routing:
            subj_prefix, subj_local = self._split_curie(subject_id)
            obj_prefix, obj_local = self._split_curie(object_id)
            target_collection = f"{subj_prefix}-{obj_prefix}" if subj_prefix and obj_prefix else self.edge_collection_name
            subj_collection = subj_prefix if subj_prefix else self.node_collection_name
            obj_collection = obj_prefix if obj_prefix else self.node_collection_name
            predicate = record.get("predicate", "")
            record["_from"] = f"{subj_collection}/{subj_local}"
            record["_to"] = f"{obj_collection}/{obj_local}"
            record["_key"] = f"{subj_local}-{predicate}-{obj_local}"
        else:
            target_collection = record.pop("_collection", self.edge_collection_name)
            predicate = record.get("predicate", "")
            record["_from"] = f"{self.node_collection_name}/{self._sanitize_key(subject_id)}"
            record["_to"] = f"{self.node_collection_name}/{self._sanitize_key(object_id)}"
            record["_key"] = self._sanitize_key(f"{subject_id}-{predicate}-{object_id}")
        record["_collection"] = target_collection
        self.edge_cache.append((target_collection, record))
        self.edge_count += 1

    def finalize(self) -> None:
        """
        Write any remaining cached node and/or edge records.
        """
        self._write_node_cache()
        self._write_edge_cache()

    def _flush_node_cache(self):
        """Flush the node cache by writing and clearing it."""
        self._write_node_cache()
        self.node_cache.clear()
        self.node_count = 0

    def _write_node_cache(self) -> None:
        """
        Write cached node records to ArangoDB.
        """
        for collection_name, nodes in self.node_cache.items():
            col = self._get_or_create_vertex_collection(collection_name)
            for x in range(0, len(nodes), self.BATCH_SIZE):
                y = min(x + self.BATCH_SIZE, len(nodes))
                batch = nodes[x:y]
                log.debug(f"Writing node batch {x} - {y} to {collection_name}")
                try:
                    col.import_bulk(batch, on_duplicate="update")
                except Exception as e:
                    self.owner.log_error(
                        entity=f"{collection_name} Nodes batch {x}-{y}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e),
                    )

    def _flush_edge_cache(self):
        """Flush the edge cache by writing node cache first, then edges."""
        self._flush_node_cache()
        self._write_edge_cache()
        self.edge_cache.clear()
        self.edge_count = 0

    def _write_edge_cache(self) -> None:
        """
        Write cached edge records to ArangoDB.
        """
        # Group edges by collection
        grouped: Dict[str, List] = {}
        for collection_name, record in self.edge_cache:
            if collection_name not in grouped:
                grouped[collection_name] = []
            grouped[collection_name].append(record)

        for collection_name, edges in grouped.items():
            col = self._get_or_create_edge_collection(collection_name)
            for x in range(0, len(edges), self.BATCH_SIZE):
                y = min(x + self.BATCH_SIZE, len(edges))
                batch = edges[x:y]
                log.debug(f"Writing edge batch {x} - {y} to {collection_name}")
                try:
                    col.import_bulk(batch, on_duplicate="update")
                except Exception as e:
                    self.owner.log_error(
                        entity=f"{collection_name} Edges batch {x}-{y}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e),
                    )

    @staticmethod
    def _sanitize_key(node_id: str) -> str:
        """
        Sanitize a node ID for use as an ArangoDB ``_key``.
        ArangoDB ``_key`` values disallow ``/`` characters.

        Parameters
        ----------
        node_id: str
            The node ID (CURIE)

        Returns
        -------
        str
            A sanitized key safe for ArangoDB
        """
        return node_id.replace("/", "_")
