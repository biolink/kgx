"""
Classical KGX graph summary module.
"""
from typing import Dict, List, Optional, Any, Callable

import re

import yaml
from json import dump
from json.encoder import JSONEncoder

from deprecation import deprecated

from kgx.error_detection import ErrorType, MessageLevel, ErrorDetecting
from kgx.utils.kgx_utils import GraphEntityType
from kgx.graph.base_graph import BaseGraph
from kgx.prefix_manager import PrefixManager

TOTAL_NODES = "total_nodes"
NODE_CATEGORIES = "node_categories"

NODE_ID_PREFIXES_BY_CATEGORY = "node_id_prefixes_by_category"
NODE_ID_PREFIXES = "node_id_prefixes"

COUNT_BY_CATEGORY = "count_by_category"

COUNT_BY_ID_PREFIXES_BY_CATEGORY = "count_by_id_prefixes_by_category"
COUNT_BY_ID_PREFIXES = "count_by_id_prefixes"

TOTAL_EDGES = "total_edges"
EDGE_PREDICATES = "predicates"
COUNT_BY_EDGE_PREDICATES = "count_by_predicates"
COUNT_BY_SPO = "count_by_spo"


# Note: the format of the stats generated might change in the future

####################################################################################
# New "Inspector Class" design pattern for KGX stream data processing
####################################################################################


def gs_default(o):
    """
    JSONEncoder 'default' function override to
    properly serialize 'Set' objects (into 'List')
    :param o
    """
    if isinstance(o, GraphSummary.Category):
        return o.json_object()
    else:
        try:
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        # Let the base class default method raise the TypeError
        return JSONEncoder().default(o)


_category_curie_regexp = re.compile("^biolink:[A-Z][a-zA-Z]*$")
_predicate_curie_regexp = re.compile("^biolink:[a-z][a-z_]*$")


class GraphSummary(ErrorDetecting):
    """
    Class for generating a "classical" knowledge graph summary.

    The optional 'progress_monitor' for the validator should be a lightweight Callable
    which is injected into the class 'inspector' Callable, designed to intercepts
    node and edge records streaming through the Validator (inside a Transformer.process() call.
    The first (GraphEntityType) argument of the Callable tags the record as a NODE or an EDGE.
    The second argument given to the Callable is the current record itself.
    This Callable is strictly meant to be procedural and should *not* mutate the record.
    The intent of this Callable is to provide a hook to KGX applications wanting the
    namesake function of passively monitoring the graph data stream. As such, the Callable
    could simply tally up the number of times it is called with a NODE or an EDGE, then
    provide a suitable (quick!) report of that count back to the KGX application. The
    Callable (function/callable class) should not modify the record and should be of low
    complexity, so as not to introduce a large computational overhead to validation!
    """

    def __init__(
        self,
        name="",
        node_facet_properties: Optional[List] = None,
        edge_facet_properties: Optional[List] = None,
        progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = None,
        error_log: str = None,
        **kwargs,
    ):
        """
        GraphSummary constructor.

        Parameters
        ----------
        name: str
            (Graph) name assigned to the summary.
        node_facet_properties: Optional[List]
                A list of properties to facet on. For example, ``['provided_by']``
        edge_facet_properties: Optional[List]
                A list of properties to facet on. For example, ``['knowledge_source']``
        progress_monitor: Optional[Callable[[GraphEntityType, List], None]]
            Function given a peek at the current record being stream processed by the class wrapped Callable.
        error_log: str
            Where to write any graph processing error message (stderr, by default)

        """
        ErrorDetecting.__init__(self, error_log)
        
        # formal arguments
        self.name = name

        self.nodes_processed = False

        self.node_stats: Dict = {
            TOTAL_NODES: 0,
            NODE_CATEGORIES: set(),
            NODE_ID_PREFIXES: set(),
            NODE_ID_PREFIXES_BY_CATEGORY: dict(),
            COUNT_BY_CATEGORY: dict(),
            COUNT_BY_ID_PREFIXES_BY_CATEGORY: dict(),
            COUNT_BY_ID_PREFIXES: dict(),
        }

        self.edges_processed: bool = False

        self.edge_stats: Dict = {
            TOTAL_EDGES: 0,
            EDGE_PREDICATES: set(),
            COUNT_BY_EDGE_PREDICATES: {"unknown": {"count": 0}},
            COUNT_BY_SPO: {},
        }

        self.node_facet_properties: Optional[List] = node_facet_properties
        if self.node_facet_properties:
            for facet_property in self.node_facet_properties:
                self.add_node_stat(facet_property, set())

        self.edge_facet_properties: Optional[List] = edge_facet_properties
        if self.edge_facet_properties:
            for facet_property in self.edge_facet_properties:
                self.edge_stats[facet_property] = set()

        self.progress_monitor: Optional[
            Callable[[GraphEntityType, List], None]
        ] = progress_monitor

        # internal attributes
        self.node_catalog: Dict[str, List[int]] = dict()

        self.node_categories: Dict[str, GraphSummary.Category] = dict()

        # indexed internally with category index id '0'
        self.node_categories["unknown"] = GraphSummary.Category("unknown", self)

        self.graph_stats: Dict[str, Dict] = dict()
    
    def get_name(self):
        """
        Returns
        -------
        str
            Currently assigned knowledge graph name.
        """
        return self.name

    def __call__(self, entity_type: GraphEntityType, rec: List):
        """
        Transformer 'inspector' Callable, for analysing a stream of graph data.

        Parameters
        ----------
        entity_type: GraphEntityType
            indicates what kind of record being passed to the function for analysis.
        rec: Dict
            Complete data dictionary of the given record.

        """
        if self.progress_monitor:
            self.progress_monitor(entity_type, rec)
        if entity_type == GraphEntityType.EDGE:
            self.analyse_edge(*rec)
        elif entity_type == GraphEntityType.NODE:
            self.analyse_node(*rec)
        else:
            raise RuntimeError("Unexpected GraphEntityType: " + str(entity_type))

    class Category:
        """
        Internal class for compiling statistics about a distinct category.
        """

        # The 'category map' just associates a unique int catalog
        # index ('cid') value as a proxy for the full curie string,
        # to reduce storage in the main node catalog
        _category_curie_map: List[str] = list()

        def __init__(self, category_curie: str, summary):

            """
            GraphSummary.Category constructor.

            category: str
                Biolink Model category curie identifier.

            """
            if not (
                _category_curie_regexp.fullmatch(category_curie)
                or category_curie == "unknown"
            ):
                raise RuntimeError("Invalid Biolink category CURIE: " + category_curie)

            # generally, a Biolink  category class CURIE but also 'unknown'
            self.category_curie = category_curie

            # it is useful to point to the GraphSummary within
            # which this Category metadata is bring tracked...
            self.summary = summary

            # ...so that Category related entries at that
            # higher level may be properly initialized
            # for subsequent facet metadata access
            if self.category_curie != "unknown":
                self.summary.node_stats[NODE_CATEGORIES].add(self.category_curie)
            self.summary.node_stats[NODE_ID_PREFIXES_BY_CATEGORY][
                self.category_curie
            ] = list()
            self.summary.node_stats[COUNT_BY_CATEGORY][self.category_curie] = {
                "count": 0
            }

            if self.category_curie not in self._category_curie_map:
                self._category_curie_map.append(self.category_curie)
            self.category_stats: Dict[str, Any] = dict()
            self.category_stats["count"]: int = 0
            self.category_stats["count_by_source"]: Dict[str, int] = {"unknown": 0}
            self.category_stats["count_by_id_prefix"]: Dict[str, int] = dict()

        def get_name(self) -> str:
            """
            Returns
            -------
            str
                Biolink CURIE name of the category.
            """
            return self.category_curie

        def get_cid(self) -> int:
            """
            Returns
            -------
            int
                Internal GraphSummary index id for tracking a Category.
            """
            return self._category_curie_map.index(self.category_curie)

        @classmethod
        def get_category_curie_by_index(cls, cid: int) -> str:
            """
            Parameters
            ----------
            cid: int
                Internal GraphSummary index id for tracking a Category.

            Returns
            -------
            str
                Curie identifier of the Category.
            """
            return cls._category_curie_map[cid]

        def get_id_prefixes(self) -> List:
            """
            Returns
            -------
            List[str]
                List of identifier prefix (strings) used by nodes of this Category.
            """
            return list(self.category_stats["count_by_id_prefix"].keys())

        def get_count_by_id_prefixes(self):
            """
            Returns
            -------
            int
                Count of nodes by id_prefixes for nodes which have this category.
            """
            return self.category_stats["count_by_id_prefix"]

        def get_count(self):
            """
            Returns
            -------
            int
                Count of nodes which have this category.
            """
            return self.category_stats["count"]

        def _capture_prefix(self, n: str):
            prefix = PrefixManager.get_prefix(n)
            if not prefix:
                error_type = ErrorType.MISSING_NODE_CURIE_PREFIX
                self.summary.log_error(
                    entity=n,
                    error_type=error_type,
                    message="Node 'id' has no CURIE prefix",
                    message_level=MessageLevel.WARNING
                )
            else:
                if prefix in self.category_stats["count_by_id_prefix"]:
                    self.category_stats["count_by_id_prefix"][prefix] += 1
                else:
                    self.category_stats["count_by_id_prefix"][prefix] = 1

        def _capture_knowledge_source(self, data: Dict):
            if "provided_by" in data:
                for s in data["provided_by"]:
                    if s in self.category_stats["count_by_source"]:
                        self.category_stats["count_by_source"][s] += 1
                    else:
                        self.category_stats["count_by_source"][s] = 1
            else:
                self.category_stats["count_by_source"]["unknown"] += 1

        def analyse_node_category(self, summary, n, data):
            """
            Analyse metadata of a given graph node record of this category.

            Parameters
            ----------
            summary: GraphSummary
                GraphSunmmary within which the Category is being analysed.
            n: str
                Curie identifier of the node record (not used here).
            data: Dict
                Complete data dictionary of node record fields.

            """
            self.category_stats["count"] += 1

            self._capture_prefix(n)

            self._capture_knowledge_source(data)

            if summary.node_facet_properties:
                for facet_property in summary.node_facet_properties:
                    summary.node_stats = summary.get_facet_counts(
                        data,
                        summary.node_stats,
                        COUNT_BY_CATEGORY,
                        self.category_curie,
                        facet_property,
                    )

        def json_object(self):
            """
            Returns
            -------
            Dict[str, Any]
                Returns JSON friendly metadata for this category.,
            """
            return {
                "id_prefixes": list(self.category_stats["count_by_id_prefix"].keys()),
                "count": self.category_stats["count"],
                "count_by_source": self.category_stats["count_by_source"],
                "count_by_id_prefix": self.category_stats["count_by_id_prefix"],
            }

    def get_category(self, category_curie: str) -> Category:
        """
        Counts the number of distinct (Biolink) categories encountered
        in the knowledge graph (not including those of 'unknown' category)

        Parameters
        ----------
        category_curie: str
            Curie identifier for the (Biolink) category.

        Returns
        -------
        Category
            MetaKnowledgeGraph.Category object for a given Biolink category.
        """
        return self.node_stats[category_curie]

    def _process_category_field(self, category_field: str, n: str, data: Dict):

        # we note here that category_curie *may be*
        # a piped '|' set of Biolink category CURIE values
        category_list = category_field.split("|")

        # analyse them each independently...
        for category_curie in category_list:

            if category_curie not in self.node_categories:
                try:
                    self.node_categories[category_curie] = self.Category(
                        category_curie, self
                    )
                except RuntimeError:
                    error_type = ErrorType.INVALID_CATEGORY
                    self.log_error(
                        entity=n,
                        error_type=error_type,
                        message=f"Invalid node 'category' CURIE: '{category_curie}'"
                    )
                    continue

            category_record = self.node_categories[category_curie]
            category_idx: int = category_record.get_cid()
            if category_idx not in self.node_catalog[n]:
                self.node_catalog[n].append(category_idx)
            category_record.analyse_node_category(self, n, data)

        #
        # Moved this computation from the 'analyse_node_category() method above
        #
        # if self.node_facet_properties:
        #     for facet_property in self.node_facet_properties:
        #         self.node_stats = self.get_facet_counts(
        #             data, self.node_stats, COUNT_BY_CATEGORY, category_curie, facet_property
        #         )

    def analyse_node(self, n, data):
        """
        Analyse metadata of one graph node record.

        Parameters
        ----------
        n: str
            Curie identifier of the node record (not used here).
        data: Dict
            Complete data dictionary of node record fields.

        """
        if n in self.node_catalog:
            # Report duplications of node records, as discerned from node id.
            error_type = ErrorType.DUPLICATE_NODE
            self.log_error(
                entity=n,
                error_type=error_type,
                message="Node 'id' duplicated in input data",
                message_level=MessageLevel.WARNING
            )
            return
        else:
            self.node_catalog[n] = list()

        if "category" in data and data["category"]:
            categories = data["category"]

        else:
            error_type = ErrorType.MISSING_CATEGORY
            self.log_error(
                entity=n,
                error_type=error_type,
                message="Missing node 'category' tagged as 'unknown'."
            )
            categories = ["unknown"]

        # analyse them each independently...
        for category_field in categories:
            self._process_category_field(category_field, n, data)

    def _capture_predicate(self, data: Dict) -> Optional[str]:
        if "predicate" not in data:
            self.edge_stats[COUNT_BY_EDGE_PREDICATES]["unknown"]["count"] += 1
            predicate = "unknown"
        else:
            predicate = data["predicate"]

            if not _predicate_curie_regexp.fullmatch(predicate):
                error_type = ErrorType.INVALID_EDGE_PREDICATE
                self.log_error(
                    entity=predicate,
                    error_type=error_type,
                    message="Invalid 'predicate' CURIE?"
                )
                return None

            self.edge_stats[EDGE_PREDICATES].add(predicate)
            if predicate in self.edge_stats[COUNT_BY_EDGE_PREDICATES]:
                self.edge_stats[COUNT_BY_EDGE_PREDICATES][predicate]["count"] += 1
            else:
                self.edge_stats[COUNT_BY_EDGE_PREDICATES][predicate] = {"count": 1}

            if self.edge_facet_properties:
                for facet_property in self.edge_facet_properties:
                    self.edge_stats = self.get_facet_counts(
                        data,
                        self.edge_stats,
                        COUNT_BY_EDGE_PREDICATES,
                        predicate,
                        facet_property,
                    )

        return predicate

    def _process_triple(
        self, subject_category: str, predicate: str, object_category: str, data: Dict
    ):
        # Process the 'valid' S-P-O triple here...
        key = f"{subject_category}-{predicate}-{object_category}"
        if key in self.edge_stats[COUNT_BY_SPO]:
            self.edge_stats[COUNT_BY_SPO][key]["count"] += 1
        else:
            self.edge_stats[COUNT_BY_SPO][key] = {"count": 1}

        if self.edge_facet_properties:
            for facet_property in self.edge_facet_properties:
                self.edge_stats = self.get_facet_counts(
                    data, self.edge_stats, COUNT_BY_SPO, key, facet_property
                )

    def analyse_edge(self, u: str, v: str, k: str, data: Dict):
        """
        Analyse metadata of one graph edge record.

        Parameters
        ----------
        u: str
            Subject node curie identifier of the edge.
        v: str
            Subject node curie identifier of the edge.
        k: str
            Key identifier of the edge record (not used here).
        data: Dict
            Complete data dictionary of edge record fields.

        """
        # we blissfully now assume that all the nodes of a
        # graph stream were analysed first by the GraphSummary
        # before the edges are analysed, thus we can test for
        # node 'n' existence internally, by identifier.

        self.edge_stats[TOTAL_EDGES] += 1

        predicate: str = self._capture_predicate(data)

        if u not in self.node_catalog:
            error_type = ErrorType.MISSING_NODE
            self.log_error(
                entity=u,
                error_type=error_type,
                message="Subject 'id' not found in the node catalog"
            )
            
            # removing from edge count
            self.edge_stats[TOTAL_EDGES] -= 1
            self.edge_stats[COUNT_BY_EDGE_PREDICATES]["unknown"]["count"] -= 1
            return

        for subj_cat_idx in self.node_catalog[u]:

            subject_category = self.Category.get_category_curie_by_index(subj_cat_idx)

            if v not in self.node_catalog:
                error_type = ErrorType.MISSING_NODE
                self.log_error(
                    entity=v,
                    error_type=error_type,
                    message="Object 'id' not found in the node catalog"
                )
                
                self.edge_stats[TOTAL_EDGES] -= 1
                self.edge_stats[COUNT_BY_EDGE_PREDICATES]["unknown"]["count"] -= 1
                return

            for obj_cat_idx in self.node_catalog[v]:

                object_category = self.Category.get_category_curie_by_index(
                    obj_cat_idx
                )

                self._process_triple(subject_category, predicate, object_category, data)

    def _compile_prefix_stats_by_category(self, category_curie: str):
        for prefix in self.node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY][category_curie]:
            if prefix not in self.node_stats[COUNT_BY_ID_PREFIXES]:
                self.node_stats[COUNT_BY_ID_PREFIXES][prefix] = 0
            self.node_stats[COUNT_BY_ID_PREFIXES][prefix] += self.node_stats[
                COUNT_BY_ID_PREFIXES_BY_CATEGORY
            ][category_curie][prefix]

    def _compile_category_stats(self, node_category: Category):
        category_curie = node_category.get_name()

        self.node_stats[COUNT_BY_CATEGORY][category_curie][
            "count"
        ] = node_category.get_count()

        id_prefixes = node_category.get_id_prefixes()
        self.node_stats[NODE_ID_PREFIXES_BY_CATEGORY][category_curie] = id_prefixes
        self.node_stats[NODE_ID_PREFIXES].update(id_prefixes)

        self.node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY][
            category_curie
        ] = node_category.get_count_by_id_prefixes()

        self._compile_prefix_stats_by_category(category_curie)

    def get_node_stats(self) -> Dict[str, Any]:
        """
        Returns
        -------
        Dict[str, Any]
            Statistics for the nodes in the graph.
        """
        if not self.nodes_processed:

            self.nodes_processed = True

            for node_category in self.node_categories.values():
                self._compile_category_stats(node_category)

            self.node_stats[NODE_CATEGORIES] = sorted(self.node_stats[NODE_CATEGORIES])
            self.node_stats[NODE_ID_PREFIXES] = sorted(self.node_stats[NODE_ID_PREFIXES])

            if self.node_facet_properties:
                for facet_property in self.node_facet_properties:
                    self.node_stats[facet_property] = sorted(
                        list(self.node_stats[facet_property])
                    )

            if not self.node_stats[TOTAL_NODES]:
                self.node_stats[TOTAL_NODES] = len(self.node_catalog)

        return self.node_stats

    def add_node_stat(self, tag: str, value: Any):
        """
        Compile/add a nodes statistic for a given tag = value annotation of the node.

        :param tag:
        :param value:
        :return:

        Parameters
        ----------
        tag: str
            Tag label for the annotation.
        value: Any
             Value of the specific tag annotation.

        """
        self.node_stats[tag] = value

    def get_edge_stats(self) -> Dict[str, Any]:
        # Not sure if this is "safe" but assume that edge_stats may be finalized
        # and cached once after the first time the edge stats are accessed
        if not self.edges_processed:
            self.edges_processed = True

            self.edge_stats[EDGE_PREDICATES] = sorted(
                list(self.edge_stats[EDGE_PREDICATES])
            )

            if self.edge_facet_properties:
                for facet_property in self.edge_facet_properties:
                    self.edge_stats[facet_property] = sorted(
                        list(self.edge_stats[facet_property])
                    )

        return self.edge_stats

    def _wrap_graph_stats(
        self,
        graph_name: str,
        node_stats: Dict[str, Any],
        edge_stats: Dict[str, Any],
    ):
        # Utility wrapper function to support DRY code below.
        if not self.graph_stats:
            self.graph_stats = {
                "graph_name": graph_name,
                "node_stats": node_stats,
                "edge_stats": edge_stats,
            }
        return self.graph_stats

    def get_graph_summary(self, name: str = None, **kwargs) -> Dict:
        """
        Similar to summarize_graph except that the node and edge statistics are already captured
        in the GraphSummary class instance (perhaps by Transformer.process() stream inspection)
        and therefore, the data structure simply needs to be 'finalized' for saving or similar use.

        Parameters
        ----------
        name: Optional[str]
            Name for the graph (if being renamed)
        kwargs: Dict
            Any additional arguments (ignored in this method at present)

        Returns
        -------
        Dict
            A knowledge map dictionary corresponding to the graph

        """
        return self._wrap_graph_stats(
            graph_name=name if name else self.name,
            node_stats=self.get_node_stats(),
            edge_stats=self.get_edge_stats(),
        )

    def summarize_graph(self, graph: BaseGraph) -> Dict:
        """
        Summarize the entire graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The stats dictionary

        """
        return self._wrap_graph_stats(
            graph_name=self.name if self.name else graph.name,
            node_stats=self.summarize_graph_nodes(graph),
            edge_stats=self.summarize_graph_edges(graph),
        )

    def summarize_graph_nodes(self, graph: BaseGraph) -> Dict:
        """
        Summarize the nodes in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The node stats
        """
        for n, data in graph.nodes(data=True):
            self.analyse_node(n, data)

        return self.get_node_stats()

    def summarize_graph_edges(self, graph: BaseGraph) -> Dict:
        """
        Summarize the edges in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The edge stats
        """
        for u, v, k, data in graph.edges(keys=True, data=True):
            self.analyse_edge(u, v, k, data)

        return self.get_edge_stats()

    def _compile_facet_stats(
        self, stats: Dict, x: str, y: str, facet_property: str, value: str
    ):

        if facet_property not in stats[x][y]:
            stats[x][y][facet_property] = {}

        if value in stats[x][y][facet_property]:
            stats[x][y][facet_property][value]["count"] += 1
        else:
            stats[x][y][facet_property][value] = {"count": 1}
            stats[facet_property].update([value])

    def get_facet_counts(
        self, data: Dict, stats: Dict, x: str, y: str, facet_property: str
    ) -> Dict:
        """
        Facet on ``facet_property`` and record the count for ``stats[x][y][facet_property]``.

        Parameters
        ----------
        data: dict
            Node/edge data dictionary
        stats: dict
            The stats dictionary
        x: str
            first key
        y: str
            second key
        facet_property: str
            The property to facet on

        Returns
        -------
        Dict
            The stats dictionary

        """
        if facet_property in data:
            if isinstance(data[facet_property], list):
                for k in data[facet_property]:
                    self._compile_facet_stats(stats, x, y, facet_property, k)
            else:
                k = data[facet_property]
                self._compile_facet_stats(stats, x, y, facet_property, k)
        else:
            self._compile_facet_stats(stats, x, y, facet_property, "unknown")
        return stats

    def save(self, file, name: str = None, file_format: str = "yaml"):
        """
        Save the current GraphSummary to a specified (open) file (device).

        Parameters
        ----------
        file: File
            Text file handler open for writing.
        name: str
            Optional string to which to (re-)name the graph.
        file_format:  str
            Text output format ('json' or 'yaml') for the saved meta knowledge graph  (default: 'json')

        Returns
        -------
        None
        """
        stats = self.get_graph_summary(name)
        if not file_format or file_format == "yaml":
            yaml.dump(stats, file)
        else:
            dump(stats, file, indent=4, default=gs_default)


@deprecated(deprecated_in="1.5.8", details="Default is the use streaming graph_summary with inspector")
def generate_graph_stats(
    graph: BaseGraph,
    graph_name: str,
    filename: str,
    node_facet_properties: Optional[List] = None,
    edge_facet_properties: Optional[List] = None,
) -> None:
    """
    Generate stats from Graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    graph_name: str
        Name for the graph
    filename: str
        Filename to write the stats to
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['knowledge_source']``

    """
    stats = summarize_graph(
        graph, graph_name, node_facet_properties, edge_facet_properties
    )
    with open(filename, "w") as gsh:
        yaml.dump(stats, gsh)


def summarize_graph(
    graph: BaseGraph,
    name: str = None,
    node_facet_properties: Optional[List] = None,
    edge_facet_properties: Optional[List] = None,
) -> Dict:
    """
    Summarize the entire graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: str
        Name for the graph
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['knowledge_source']``

    Returns
    -------
    Dict
        The stats dictionary

    """
    gs = GraphSummary(name, node_facet_properties, edge_facet_properties)
    return gs.summarize_graph(graph)
