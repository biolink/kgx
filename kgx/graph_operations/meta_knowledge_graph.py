from typing import Dict, List, Optional, Any
from sys import stdout

import yaml
from json import dump
from json.encoder import JSONEncoder

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
        return JSONEncoder.default(o)


class MetaKnowledgeGraph:

    class Category:
        # this 'category map' just associates a unique int catalog
        # index ('cid') value as a proxy for the full curie string,
        # to reduce storage in the main node catalog
        _category_curie_map: List[str] = list()

        def __init__(self, category=''):
            self.category = category
            if category not in self._category_curie_map:
                self._category_curie_map.append(category)
            self.category_stats: Dict[str, Any] = dict()
            self.category_stats['id_prefixes'] = set()
            self.category_stats['count'] = 0
            self.category_stats['count_by_source'] = {'unknown': 0}

        def get_cid(self):
            return self._category_curie_map.index(self.category)

        @classmethod
        def get_category_curie(cls, cid: int):
            return cls._category_curie_map[cid]

        def get_id_prefixes(self):
            return self.category_stats['id_prefixes']

        def get_count(self):
            return self.category_stats['count']

        def get_count_by_source(self, source: str = None) -> Dict:
            if source:
                return {source: self.category_stats['count_by_source'][source]}
            return self.category_stats['count_by_source']

        def analyse_node_category(self, n, data):
            prefix = PrefixManager.get_prefix(n)
            self.category_stats['count'] += 1
            if prefix not in self.category_stats['id_prefixes']:
                self.category_stats['id_prefixes'].add(prefix)
            if 'provided_by' in data:
                for s in data['provided_by']:
                    if s in self.category_stats['count_by_source']:
                        self.category_stats['count_by_source'][s] += 1
                    else:
                        self.category_stats['count_by_source'][s] = 1
            else:
                self.category_stats['count_by_source']['unknown'] += 1

        def json_object(self):
            return {
                'id_prefixes': list(self.category_stats['id_prefixes']),
                'count': self.category_stats['count'],
                'count_by_source': self.category_stats['count_by_source']
            }

    def __init__(self, name='', **kwargs):
        """
         MetaKnowledgeGraph constructor
         (at this point, it doesn't expect further
          keyword args other than an optional graph 'name')
        """
        self.name = name
        self.node_catalog: Dict[str, List[int]] = dict()
        self.node_stats: Dict[str, MetaKnowledgeGraph.Category] = dict()
        self.node_stats['unknown'] = self.Category('unknown')
        self.association_map: Dict = dict()
        self.edge_stats = []
        self.graph_stats: Dict[str, Dict] = dict()

    def __call__(self, rec: List):
        if len(rec) == 4:  # infer an edge record
            self.analyse_edge(*rec)
        else:  # infer an node record
            self.analyse_node(*rec)

    def analyse_node(self, n, data):
        # The TRAPI release 1.1 meta_knowledge_graph format indexes nodes by biolink:Category
        # the node 'category' field is a list of assigned categories (usually just one...).
        # However, this may perhaps sometimes result in duplicate counting and conflation of prefixes(?).
        if n not in self.node_catalog:
            self.node_catalog[n] = list()
            
        if 'category' not in data:
            category = self.node_stats['unknown']
            category.analyse_node_category(n, data)
            return
        
        for category_curie in data['category']:
            if category_curie not in self.node_stats:
                self.node_stats[category_curie] = self.Category(category_curie)
            category = self.node_stats[category_curie]
            category_idx: int = category.get_cid()
            if category_idx not in self.node_catalog[n]:
                self.node_catalog[n].append(category_idx)
            category.analyse_node_category(n, data)

    def analyse_edge(self, u, v, k, data):
        # we blissfully assume that all the nodes of a
        # graph stream were analysed first by the MetaKnowledgeGraph
        # before the edges are analysed, thus we can test for
        # node 'n' existence internally, by identifier.
        if u in self.node_catalog:
            subject_category = \
                MetaKnowledgeGraph.Category.get_category_curie(self.node_catalog[u][0])
        else:
            subject_category = 'unknown'
        if v in self.node_catalog:
            object_category = \
                MetaKnowledgeGraph.Category.get_category_curie(self.node_catalog[v][0])
        else:
            object_category = 'unknown'

        triple = (subject_category, data['predicate'], object_category)
        if triple not in self.association_map:
            self.association_map[triple] = {
                'subject': triple[0],
                'predicate': triple[1],
                'object': triple[2],
                'relations': set(),
                'count': 0,
                'count_by_source': {'unknown': 0},
            }

        if data['relation'] not in self.association_map[triple]['relations']:
            self.association_map[triple]['relations'].add(data['relation'])

        self.association_map[triple]['count'] += 1
        if 'provided_by' in data:
            for s in data['provided_by']:
                if s not in self.association_map[triple]['count_by_source']:
                    self.association_map[triple]['count_by_source'][s] = 1
                else:
                    self.association_map[triple]['count_by_source'][s] += 1
        else:
            self.association_map[triple]['count_by_source']['unknown'] += 1

    def get_name(self):
        return self.name

    def get_category(self, category: str) -> Category:
        return self.node_stats[category]

    def get_node_stats(self) -> Dict[str, Category]:
        if 'unknown' in self.node_stats and not self.node_stats['unknown'].get_count():
            self.node_stats.pop('unknown')
        return self.node_stats

    def get_category_count(self) -> int:
        return len(self.node_stats)

    def get_edge_stats(self) -> List:
        # Not sure if this is "safe" but assume
        # that edge_stats may be cached once computed?
        if not self.edge_stats:
            for k, v in self.association_map.items():
                kedge = v
                relations = list(v['relations'])
                kedge['relations'] = relations
                self.edge_stats.append(kedge)
        return self.edge_stats

    def get_edge_map_count(self) -> int:
        return len(self.edge_stats)

    def get_total_nodes_count(self) -> int:
        count = 0
        for category in self.node_stats.values():
            count += category.get_count()
        return count

    def get_total_edges_count(self) -> int:
        count = 0
        for edge in self.get_edge_stats():
            count += edge['count']
        return count

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

    def summarize_graph(
            self,
            graph: BaseGraph,
            name: str = None,
            **kwargs
    ) -> Dict:
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
            A knowledge map dictionary corresponding to the graph
        """
        if not self.graph_stats:
            node_stats = self.summarize_graph_nodes(graph)
            edge_stats = self.summarize_graph_edges(graph)
            # JSON sent back as TRAPI 1.1 version,
            # without the global 'knowledge_map' object tag
            self.graph_stats = {
                'nodes': node_stats,
                'edges': edge_stats
            }
            if name:
                self.graph_stats['name'] = name
            else:
                self.graph_stats['name'] = self.name
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
            A knowledge map dictionary corresponding to the graph

        """
        if not self.graph_stats:
            # JSON sent back as TRAPI 1.1 version,
            # without the global 'knowledge_map' object tag
            self.graph_stats = {
                'nodes': self.get_node_stats(),
                'edges': self.get_edge_stats()
            }
            if name:
                self.graph_stats['name'] = name
            else:
                self.graph_stats['name'] = self.name
        return self.graph_stats

    def save(self, file, name: str = None, file_format: str = 'json'):
        """
        Save the current MetaKnowledgeGraph to a specified (open) file (device)
        """
        stats = self.get_graph_summary(name)
        if not file_format or file_format == 'json':
            dump(stats, file, indent=4, default=mkg_default)
        else:
            yaml.dump(stats, file)


def generate_meta_knowledge_graph(graph: BaseGraph, name: str, filename: str) -> None:
    """
    Generate a knowledge map that describes the composition of the graph
    and write to ``filename``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: Optional[str]
        Name for the graph
    filename: str
        The file to write the knowledge map to

    """
    graph_stats = summarize_graph(graph, name)
    with open(filename, 'w') as mkgh:
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
        A knowledge map dictionary corresponding to the graph

    """
    mkg = MetaKnowledgeGraph(name)
    return mkg.summarize_graph(graph)
