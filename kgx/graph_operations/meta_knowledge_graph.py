"""
Translator Reasoner API 'meta-knowledge-graph' endpoint analogous graph summary module.
"""
from typing import Dict, List, Optional, Any, Callable, Set, Tuple
import re

import yaml
from json import dump
from json.encoder import JSONEncoder

from deprecation import deprecated

from kgx.error_detection import ErrorType, MessageLevel, ErrorDetecting
from kgx.utils.kgx_utils import GraphEntityType
from kgx.prefix_manager import PrefixManager
from kgx.graph.base_graph import BaseGraph

"""
Generate a knowledge map that corresponds to TRAPI KnowledgeMap.
Specification based on TRAPI Draft PR: https://github.com/NCATSTranslator/ReasonerAPI/pull/171
"""


####################################################################
# Next Generation Implementation of Graph Summary coding which
# leverages the new "Transformer.process()" data stream "Inspector"
# design pattern, implemented here as a "Callable" inspection class.
####################################################################
def mkg_default(o):
    """
    JSONEncoder 'default' function override to
    properly serialize 'Set' objects (into 'List')
    """
    if isinstance(o, MetaKnowledgeGraph.Category):
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


class MetaKnowledgeGraph(ErrorDetecting):
    """
    Class for generating a TRAPI 1.1 style of "meta knowledge graph" summary.

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
            error_log=None,
            **kwargs,
    ):
        """
            MetaKnowledgeGraph constructor.

        Parameters
        ----------
        name: str
            (Graph) name assigned to the summary.
        node_facet_properties: Optional[List]
                A list of node properties (e.g. knowledge_source tags) to facet on. For example, ``['provided_by']``
        edge_facet_properties: Optional[List]
                A list of edge properties (e.g. knowledge_source tags) to facet on. For example,
                ``['original_knowledge_source', 'aggregator_knowledge_source']``
        progress_monitor: Optional[Callable[[GraphEntityType, List], None]]
            Function given a peek at the current record being stream processed by the class wrapped Callable.
        error_log:
            Where to write any graph processing error message (stderr, by default).
        """
        
        ErrorDetecting.__init__(self, error_log)
        
        # formal args
        self.name = name

        # these facet properties are used mainly for knowledge_source counting
        # using Biolink 2.0 'knowledge_source' slot values
        if node_facet_properties:
            self.node_facet_properties: Optional[List] = node_facet_properties
        else:
            # node counts still default to 'provided_by'
            self.node_facet_properties: Optional[List] = ["provided_by"]

        if edge_facet_properties:
            self.edge_facet_properties: Optional[List] = edge_facet_properties
        else:
            # node counts still default to 'knowledge_source'
            self.edge_facet_properties: Optional[List] = ["knowledge_source"]

        self.progress_monitor: Optional[
            Callable[[GraphEntityType, List], None]
        ] = progress_monitor

        # internal attributes
        # For Nodes...
        self.node_catalog: Dict[str, List[int]] = dict()
        self.node_stats: Dict[str, MetaKnowledgeGraph.Category] = dict()

        # We no longer track 'unknown' categories in meta-knowledge-graph
        # computations since such nodes are not TRAPI 1.1 compliant categories
        # self.node_stats['unknown'] = self.Category('unknown')

        # For Edges...
        self.edge_record_count: int = 0
        self.predicates: Dict = dict()
        self.association_map: Dict = dict()
        self.edge_stats = []

        # Overall graph statistics
        self.graph_stats: Dict[str, Dict] = dict()

    def get_name(self) -> str:
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

    @staticmethod
    def get_facet_counts(facets: Optional[List], counts_by_source: Dict, data: Dict):
        """
        Get node or edge facet counts
        """
        unknown: bool = True
        for facet in facets:
            if facet in data:
                unknown = False
                if isinstance(data[facet], str):
                    facet_values = [data[facet]]
                else:
                    # assume regular iterable
                    facet_values = list(data[facet])
                for s in facet_values:
                    if facet not in counts_by_source:
                        counts_by_source[facet] = dict()
                    if s in counts_by_source[facet]:
                        counts_by_source[facet][s] += 1
                    else:
                        counts_by_source[facet][s] = 1
        if unknown:
            if "unknown" in counts_by_source:
                counts_by_source["unknown"] += 1
            else:
                counts_by_source["unknown"] = 1

    class Category:
        """
        Internal class for compiling statistics about a distinct category.
        """

        # The 'category map' just associates a unique int catalog
        # index ('cid') value as a proxy for the full curie string,
        # to reduce storage in the main node catalog
        _category_curie_map: List[str] = list()

        def __init__(self, category_curie: str, mkg):
            """
            MetaKnowledgeGraph.Category constructor.

            category_curie: str
                Biolink Model category CURIE identifier.
            """
            if not (
                    _category_curie_regexp.fullmatch(category_curie)
                    or category_curie == "unknown"
            ):
                raise RuntimeError("Invalid Biolink category CURIE: " + category_curie)

            self.category_curie = category_curie
            self.mkg = mkg

            if self.category_curie not in self._category_curie_map:
                self._category_curie_map.append(self.category_curie)
            self.category_stats: Dict[str, Any] = dict()
            self.category_stats["id_prefixes"] = set()
            self.category_stats["count"] = 0
            self.category_stats["count_by_source"] = dict()

        def get_name(self) -> str:
            """
            Returns
            -------
            str
                CURIE name of the category.
            """
            return self.category_curie

        def get_cid(self):
            """
            Returns
            -------
            int
                Internal MetaKnowledgeGraph index id for tracking a Category.
            """
            return self._category_curie_map.index(self.category_curie)

        @classmethod
        def get_category_curie_from_index(cls, cid: int) -> str:
            """
            Parameters
            ----------
            cid: int
                Internal MetaKnowledgeGraph index id for tracking a Category.

            Returns
            -------
            str
                Curie identifier of the Category.
            """
            return cls._category_curie_map[cid]

        def get_id_prefixes(self) -> Set[str]:
            """
            Returns
            -------
            Set[str]
                Set of identifier prefix (strings) used by nodes of this Category.
            """
            return self.category_stats["id_prefixes"]

        def get_count(self) -> int:
            """
            Returns
            -------
            int
                Count of nodes which have this category.
            """
            return self.category_stats["count"]

        def get_count_by_source(
                self, facet: str = "provided_by", source: str = None
        ) -> Dict[str, Any]:
            """
            Parameters
            ----------
            facet: str
                Facet tag (default, 'provided_by') from which the count should be returned
            source: str
                Source name about which the count is desired.

            Returns
            -------
            Dict
                Count of nodes, by node 'provided_by' knowledge source, for a given category.
                Returns dictionary of all source counts, if input 'source' argument is not specified.
            """
            if source and facet in self.category_stats["count_by_source"]:
                if source in self.category_stats["count_by_source"][facet]:
                    return {
                        source: self.category_stats["count_by_source"][facet][source]
                    }
                else:
                    return {source: 0}
            return self.category_stats["count_by_source"]

        def _compile_prefix_stats(self, n: str):
            prefix = PrefixManager.get_prefix(n)
            if not prefix:
                error_type = ErrorType.MISSING_NODE_CURIE_PREFIX
                self.mkg.log_error(
                    entity=n,
                    error_type=error_type,
                    message="Node 'id' has no CURIE prefix",
                    message_level=MessageLevel.WARNING
                )
            else:
                if prefix not in self.category_stats["id_prefixes"]:
                    self.category_stats["id_prefixes"].add(prefix)

        def _compile_category_source_stats(self, data: Dict):
            self.mkg.get_facet_counts(
                self.mkg.node_facet_properties,
                self.category_stats["count_by_source"],
                data,
            )

        def analyse_node_category(self, n, data) -> None:
            """
            Analyse metadata of a given graph node record of this category.

            Parameters
            ----------
            n: str
                Curie identifier of the node record (not used here).
            data: Dict
                Complete data dictionary of node record fields.

            """
            self.category_stats["count"] += 1
            self._compile_prefix_stats(n)
            self._compile_category_source_stats(data)

        def json_object(self):
            """
            Returns
            -------
            Dict[str, Any]
                Returns JSON friendly metadata for this category.,
            """
            return {
                "id_prefixes": list(self.category_stats["id_prefixes"]),
                "count": self.category_stats["count"],
                "count_by_source": self.category_stats["count_by_source"],
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

            if category_curie not in self.node_stats:
                try:
                    self.node_stats[category_curie] = self.Category(
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

            category_record = self.node_stats[category_curie]

            category_idx: int = category_record.get_cid()
            if category_idx not in self.node_catalog[n]:
                self.node_catalog[n].append(category_idx)

            category_record.analyse_node_category(n, data)

    def analyse_node(self, n: str, data: Dict) -> None:
        """
        Analyse metadata of one graph node record.

        Parameters
        ----------
        n: str
            Curie identifier of the node record (not used here).
        data: Dict
            Complete data dictionary of node record fields.

        """
        # The TRAPI release 1.1 meta_knowledge_graph format indexes nodes by biolink:Category
        # the node 'category' field is a list of assigned categories (usually just one...).
        # However, this may perhaps sometimes result in duplicate counting and conflation of prefixes(?).
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

        if "category" not in data or not data["category"]:
            # we now simply exclude nodes with missing categories from the count, since a category
            # of 'unknown' in the  meta_knowledge_graph output  is considered invalid.
            # category = self.node_stats['unknown']
            # category.analyse_node_category(n, data)
            error_type = ErrorType.MISSING_CATEGORY
            self.log_error(
                entity=n,
                error_type=error_type,
                message="Missing node 'category'"
            )
            return

        categories = data["category"]

        # analyse them each independently...
        for category_field in categories:
            self._process_category_field(category_field, n, data)

    def _capture_predicate(self, subj, obj, data: Dict) -> Optional[str]:
        subj_obj_label = f"{str(subj)}->{str(obj)}"
        if "predicate" not in data:
            # We no longer track edges with 'unknown' predicates,
            # since those would not be TRAPI 1.1 JSON compliant...
            # self.predicates['unknown'] += 1
            # predicate = "unknown"
            error_type = ErrorType.MISSING_EDGE_PREDICATE
            self.log_error(
                entity=subj_obj_label,
                error_type=error_type,
                message="Empty predicate CURIE in edge data",
                message_level=MessageLevel.ERROR
            )
            self.edge_record_count -= 1
            return None
        else:
            predicate = data["predicate"]

            if not _predicate_curie_regexp.fullmatch(predicate):
                error_type = ErrorType.INVALID_EDGE_PREDICATE
                self.log_error(
                    entity=subj_obj_label,
                    error_type=error_type,
                    message=f"Invalid predicate CURIE: '{predicate}'",
                    message_level=MessageLevel.ERROR
                )
                self.edge_record_count -= 1
                return None

            if predicate not in self.predicates:
                # just need to track the number
                # of edge records using this predicate
                self.predicates[predicate] = 0
            self.predicates[predicate] += 1

        return predicate

    def _compile_triple_source_stats(self, triple: Tuple[str, str, str], data: Dict):
        self.get_facet_counts(
            self.edge_facet_properties,
            self.association_map[triple]["count_by_source"],
            data,
        )

    @staticmethod
    def _normalize_relation_field(field) -> Set:
        # various non-string iterables...
        if isinstance(field, List) or \
                isinstance(field, Tuple) or \
                isinstance(field, Set):
            # eliminate duplicate terms
            # and normalize to a set
            return set(field)
        elif isinstance(field, str):
            # for uniformity, we coerce
            # to a set of one element
            return {field}
        else:
            raise TypeError(f"Unexpected KGX edge 'relation' data field of type '{type(field)}'")

    def _process_triple(
            self, subject_category: str, predicate: str, object_category: str, data: Dict
    ):
        # Process the 'valid' S-P-O triple here...
        triple = (subject_category, predicate, object_category)
        if triple not in self.association_map:
            self.association_map[triple] = {
                "subject": triple[0],
                "predicate": triple[1],
                "object": triple[2],
                "relations": set(),
                "count_by_source": dict(),
                "count": 0,
            }

        # patch for observed defect in some ETL's such as the July 2021 SRI Reference graph
        # in which the relation field ends up being a list of terms, sometimes duplicated

        if "relation" in data:
            # input data["relation"] is normalized to a Set here
            data["relation"] = self._normalize_relation_field(data["relation"])
            self.association_map[triple]["relations"].update(data["relation"])

        self.association_map[triple]["count"] += 1

        self._compile_triple_source_stats(triple, data)

    def analyse_edge(self, u, v, k, data) -> None:
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
        # we blissfully assume that all the nodes of a
        # graph stream were analysed first by the MetaKnowledgeGraph
        # before the edges are analysed, thus we can test for
        # node 'n' existence internally, by identifier.
        #
        # Given the use case of multiple categories being assigned to a given node in a KGX data file,
        # either by category inheritance (ancestry all the way back up to NamedThing)
        # or by conflation (i.e. gene == protein id?), then the Cartesian product of
        # subject/object edges mappings need to be captured here.
        #
        self.edge_record_count += 1

        predicate: str = self._capture_predicate(u, v, data)
        if not predicate:
            # relationship needs a predicate to process?
            return

        if u not in self.node_catalog:
            error_type = ErrorType.MISSING_NODE
            self.log_error(
                entity=u,
                error_type=error_type,
                message="Subject 'id' not found in the node catalog"
            )
            # removing from edge count
            self.edge_record_count -= 1
            self.predicates[predicate] -= 1
            return

        for subj_cat_idx in self.node_catalog[u]:

            subject_category: str = self.Category.get_category_curie_from_index(
                subj_cat_idx
            )

            if v not in self.node_catalog:
                error_type = ErrorType.MISSING_NODE
                self.log_error(
                    entity=v,
                    error_type=error_type,
                    message="Object 'id' not found in the node catalog"
                )
                self.edge_record_count -= 1
                self.predicates[predicate] -= 1
                return

            for obj_cat_idx in self.node_catalog[v]:
                object_category: str = self.Category.get_category_curie_from_index(
                    obj_cat_idx
                )

                self._process_triple(subject_category, predicate, object_category, data)

    def get_number_of_categories(self) -> int:
        """
        Counts the number of distinct (Biolink) categories encountered
        in the knowledge graph (not including those of 'unknown' category)

        Returns
        -------
        int
            Number of distinct (Biolink) categories found in the graph (excluding nodes with 'unknown' category)
        """
        # 'unknown' not tracked anymore...
        # return len([c for c in self.node_stats.keys() if c != 'unknown'])
        return len(self.node_stats.keys())

    def get_node_stats(self) -> Dict[str, Dict]:
        """
        Returns
        -------
        Dict[str, Category]
            Statistics for the nodes in the graph.
        """
        # We no longer track 'unknown' node category counts - non TRAPI 1.1. compliant output
        # if 'unknown' in self.node_stats and not self.node_stats['unknown'].get_count():
        #     self.node_stats.pop('unknown')
        
        # Here we assume that the node_stats are complete and will now
        # be exported in a graph summary for the module, thus we aim to
        # Convert the 'MetaKnowledgeGraph.Category' object into vanilla
        # Python dictionary and lists, to facilitate output
        category_stats = dict()
        for category_curie in self.node_stats.keys():
            category_obj = self.node_stats[category_curie]
            category_stats[category_curie] = dict()
            # Convert id_prefixes Set into a sorted List
            category_stats[category_curie]["id_prefixes"] = sorted(category_obj.category_stats["id_prefixes"])
            category_stats[category_curie]["count"] = category_obj.category_stats["count"]
            category_stats[category_curie]["count_by_source"] = category_obj.category_stats["count_by_source"]

        return category_stats

    def get_edge_stats(self) -> List[Dict[str, Any]]:
        """
        Returns
        -------
        List[Dict[str, Any]]
            Knowledge map for the list of edges in the graph.
        """
        # Not sure if this is "safe" but assume
        # that edge_stats may be cached once computed?
        if not self.edge_stats:
            for k, v in self.association_map.items():
                kedge = v
                relations = list(v["relations"])
                kedge["relations"] = relations
                self.edge_stats.append(kedge)
        return self.edge_stats

    def get_total_nodes_count(self) -> int:
        """
        Counts the total number of distinct nodes in the knowledge graph
        (**not** including those ignored due to being of 'unknown' category)

        Returns
        -------
        int
            Number of distinct nodes in the knowledge.
        """
        return len(self.node_catalog)

    def get_node_count_by_category(self, category_curie: str) -> int:
        """
        Counts the number of edges in the graph
        with the specified (Biolink) category curie.

        Parameters
        ----------
        category_curie: str
            Curie identifier for the (Biolink) category.

        Returns
        -------
        int
            Number of nodes for the given category.

        Raises
        ------
        RuntimeError
            Error if category identifier is empty string or None.
        """
        if not category_curie:
            raise RuntimeError(
                "get_node_count_by_category(): null or empty category argument!?"
            )
        if category_curie in self.node_stats.keys():
            return self.node_stats[category_curie].get_count()
        else:
            return 0

    def get_total_node_counts_across_categories(self) -> int:
        """
        The aggregate count of all node to category mappings for every category.
        Note that nodes with multiple categories will have their count replicated
        under each of its categories.

        Returns
        -------
        int
            Total count of node to category mappings for the graph.
        """
        count = 0
        for category in self.node_stats.values():
            count += category.get_count()
        return count

    def get_total_edges_count(self) -> int:
        """
        Gets the total number of 'valid' edges in the data set
        (ignoring those with 'unknown' subject or predicate category mappings)

        Returns
        ----------
        int
            Total count of edges in the graph.
        """
        return self.edge_record_count

    def get_edge_mapping_count(self) -> int:
        """
        Counts the number of distinct edge
        Subject (category) - P (predicate) -> Object (category)
        mappings in the knowledge graph.

        Returns
        ----------
        int
            Count of subject(category) - predicate -> object(category) mappings in the graph.
        """
        return len(self.get_edge_stats())

    def get_predicate_count(self) -> int:
        """
        Counts the number of distinct edge predicates
        in the knowledge graph.

        Returns
        ----------
        int
            Number of distinct (Biolink) predicates in the graph.
        """
        return len(self.predicates)

    def get_edge_count_by_predicate(self, predicate_curie: str) -> int:
        """
        Counts the number of edges in the graph with the specified predicate.

        Parameters
        ----------
        predicate_curie: str
            (Biolink) curie identifier for the predicate.

        Returns
        -------
        int
            Number of edges for the given predicate.

        Raises
        ------
        RuntimeError
            Error if predicate identifier is empty string or None.
        """
        if not predicate_curie:
            raise RuntimeError(
                "get_node_count_by_category(): null or empty predicate argument!?"
            )
        if predicate_curie in self.predicates:
            return self.predicates[predicate_curie]
        return 0

    def get_total_edge_counts_across_mappings(self) -> int:
        """
        Aggregate count of the edges in the graph for every mapping. Edges
        with subject and object nodes with multiple assigned categories will
        have their count replicated under each distinct mapping of its categories.

        Returns
        -------
        int
            Number of the edges counted across all mappings.
        """
        count = 0
        for edge in self.get_edge_stats():
            count += edge["count"]
        return count

    def get_edge_count_by_source(
            self,
            subject_category: str,
            predicate: str,
            object_category: str,
            facet: str = "knowledge_source",
            source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Returns count by source for one S-P-O triple (S, O being Biolink categories; P, a Biolink predicate)
        """
        spo_label = f"Edge {str(subject_category)}-{str(predicate)}->{str(object_category)}"
        if not (subject_category and predicate and object_category):
            error_type = ErrorType.MISSING_EDGE_PROPERTY
            self.log_error(
                entity=spo_label,
                error_type=error_type,
                message="Incomplete S-P-O triple",
                message_level=MessageLevel.WARNING
            )
            return dict()
        
        triple = (subject_category, predicate, object_category)
        
        if (
                triple in self.association_map
                and "count_by_source" in self.association_map[triple]
        ):
            if facet in self.association_map[triple]["count_by_source"]:
                if source:
                    if source in self.association_map[triple]["count_by_source"][facet]:
                        return self.association_map[triple]["count_by_source"][facet][
                            source
                        ]
                    else:
                        return dict()
                else:
                    return self.association_map[triple]["count_by_source"][facet]
            else:
                return dict()
        else:
            error_type = ErrorType.INVALID_EDGE_TRIPLE
            self.log_error(
                entity=spo_label,
                error_type=error_type,
                message="Unknown S-P-O triple?",
                message_level=MessageLevel.WARNING
            )
            return dict()

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

    def summarize_graph_edges(self, graph: BaseGraph) -> List[Dict]:
        """
        Summarize the edges in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        List[Dict]
            The edge stats

        """
        for u, v, k, data in graph.edges(keys=True, data=True):
            self.analyse_edge(u, v, k, data)
        return self.get_edge_stats()

    def summarize_graph(self, graph: BaseGraph, name: str = None, **kwargs) -> Dict:
        """
        Generate a meta knowledge graph that describes the composition of the graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph
        name: Optional[str]
            Name for the graph
        kwargs: Dict
            Any additional arguments (ignored in this method at present)

        Returns
        -------
        Dict
            A TRAPI 1.1 compliant meta knowledge graph of the knowledge graph returned as a dictionary.
        """
        if not self.graph_stats:
            node_stats = self.summarize_graph_nodes(graph)
            edge_stats = self.summarize_graph_edges(graph)
            # JSON sent back as TRAPI 1.1 version,
            # without the global 'knowledge_map' object tag
            self.graph_stats = {"nodes": node_stats, "edges": edge_stats}
            if name:
                self.graph_stats["name"] = name
            else:
                self.graph_stats["name"] = self.name
        return self.graph_stats

    def get_graph_summary(self, name: str = None, **kwargs) -> Dict:
        """
        Similar to summarize_graph except that the node and edge statistics are already captured
        in the MetaKnowledgeGraph class instance (perhaps by Transformer.process() stream inspection)
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
            A TRAPI 1.1 compliant meta knowledge graph of the knowledge graph returned as a dictionary.
        """
        if not self.graph_stats:
            # JSON sent back as TRAPI 1.1 version,
            # without the global 'knowledge_map' object tag
            self.graph_stats = {
                "nodes": self.get_node_stats(),
                "edges": self.get_edge_stats(),
            }
            if name:
                self.graph_stats["name"] = name
            else:
                self.graph_stats["name"] = self.name
        return self.graph_stats

    def save(self, file, name: str = None, file_format: str = "json") -> None:
        """
        Save the current MetaKnowledgeGraph to a specified (open) file (device).

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
        if not file_format or file_format == "json":
            dump(stats, file, indent=4)
        else:
            yaml.dump(stats, file)


@deprecated(deprecated_in="1.5.8", details="Default is the use streaming graph_summary with inspector")
def generate_meta_knowledge_graph(graph: BaseGraph, name: str, filename: str, **kwargs) -> None:
    """
    Generate a knowledge map that describes
    the composition of the graph and write to ``filename``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: Optional[str]
        Name for the graph
    filename: str
        The file to write the knowledge map to

    """
    graph_stats = summarize_graph(graph, name, **kwargs)
    with open(filename, mode="w") as mkgh:
        dump(graph_stats, mkgh, indent=4, default=mkg_default)


def summarize_graph(graph: BaseGraph, name: str = None, **kwargs) -> Dict:
    """
    Generate a meta knowledge graph that describes the composition of the graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: Optional[str]
        Name for the graph
    kwargs: Dict
        Any additional arguments

    Returns
    -------
    Dict
        A TRAPI 1.1 compliant meta knowledge graph of the knowledge graph returned as a dictionary.
    """
    mkg = MetaKnowledgeGraph(name, **kwargs)
    return mkg.summarize_graph(graph)
