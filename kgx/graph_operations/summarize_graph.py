import yaml
from typing import Dict, List, Optional, Any

from json import dump
from json.encoder import JSONEncoder

from kgx.graph.base_graph import BaseGraph
from kgx.prefix_manager import PrefixManager

TOTAL_NODES = 'total_nodes'
NODE_CATEGORIES = 'node_categories'
COUNT_BY_CATEGORY = 'count_by_category'

TOTAL_EDGES = 'total_edges'
EDGE_PREDICATES = 'predicates'
COUNT_BY_EDGE_PREDICATES = 'count_by_predicates'
COUNT_BY_SPO = 'count_by_spo'


# Note: the format of the stats generated might change in the future

####################################################################################
# New "Inspector Class" design pattern for KGX stream data processing
####################################################################################
def gs_default(o):
    """
    JSONEncoder 'default' function override to
    properly serialize 'Set' objects (into 'List')
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
        return JSONEncoder.default(o)
    

class GraphSummary:

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

    def __init__(
            self,
            name='',
            node_facet_properties: Optional[List] = None,
            edge_facet_properties: Optional[List] = None
    ):
        """
        Graph Summary constructor
        
        Parameters
        ----------
        name: str
            Name for the graph
        node_facet_properties: Optional[List]
            A list of properties to facet on. For example, ``['provided_by']``
        edge_facet_properties: Optional[List]
            A list of properties to facet on. For example, ``['provided_by']``
        """
        self.name = name
        self.node_facet_properties: Optional[List] = node_facet_properties
        self.edge_facet_properties: Optional[List] = edge_facet_properties
        self.node_catalog: Dict[str, List[int]] = dict()
        self.node_stats: Dict = {
            TOTAL_NODES: 0,
            NODE_CATEGORIES: set(),
            COUNT_BY_CATEGORY: {'unknown': {'count': 0}},
        }
        self.association_map: Dict = dict()
        self.edge_stats = []
        self.graph_stats: Dict[str, Dict] = dict()

    def __call__(self, rec: List):
        if len(rec) == 4:  # infer an edge record
            self.analyse_edge(*rec)
        else:  # infer an node record
            self.analyse_node(*rec)

    def analyse_node(self, n, data):
        if n not in self.node_catalog:
            self.node_catalog[n] = list()

        if 'category' not in data:
            stats[COUNT_BY_CATEGORY]['unknown']['count'] += 1
            continue
        categories = data['category']
        stats[NODE_CATEGORIES].update(categories)
        for category in categories:
            if category in stats[COUNT_BY_CATEGORY]:
                stats[COUNT_BY_CATEGORY][category]['count'] += 1
            else:
                stats[COUNT_BY_CATEGORY][category] = {'count': 1}
    
            if facet_properties:
                for facet_property in facet_properties:
                    stats = self.get_facet_counts(
                        data, stats, COUNT_BY_CATEGORY, category, facet_property
                    )

        stats[NODE_CATEGORIES] = sorted(list(stats[NODE_CATEGORIES]))
        if facet_properties:
            for facet_property in facet_properties:
                stats[facet_property] = sorted(list(stats[facet_property]))

    def analyse_edge(self, u, v, k, data):
        # we blissfully assume that all the nodes of a
        # graph stream were analysed first by the GraphSummary
        # before the edges are analysed, thus we can test for
        # node 'n' existence internally, by identifier.
        if u in self.node_catalog:
            subject_category = \
                GraphSummary.Category.get_category_curie(self.node_catalog[u][0])
        else:
            subject_category = 'unknown'
        if v in self.node_catalog:
            object_category = \
                GraphSummary.Category.get_category_curie(self.node_catalog[v][0])
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

    def get_node_stats(self) -> Dict[str, Any]:
        return self.node_stats

    def add_node_stat(self, tag: str, value: Any):
        self.node_stats[tag] = value

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
   
    def summarize_graph(
            self,
            graph: BaseGraph
    ) -> Dict:
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
        if not self.graph_stats:
            node_stats = self.summarize_graph_nodes(graph, self.node_facet_properties)
            edge_stats = self.summarize_graph_edges(graph, self.edge_facet_properties)
            self.graph_stats = {
                'graph_name': self.name if self.name else graph.name,
                'node_stats': node_stats,
                'edge_stats': edge_stats,
            }
        return self.graph_stats

    def summarize_graph_nodes(self, graph: BaseGraph, facet_properties: Optional[List] = None) -> Dict:
        """
        Summarize the nodes in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph
        facet_properties: Optional[List]
            A list of properties to facet on

        Returns
        -------
        Dict
            The node stats
        """
        # TODO: count TOTAL_NODES somewhere else?
        self.add_node_stat(TOTAL_NODES, len(graph.nodes()))
        
        if facet_properties:
            for facet_property in facet_properties:
                self.add_node_stat(facet_property, set())
        for n, data in graph.nodes(data=True):
            self.analyse_node(n, data)
        return self.get_node_stats()

    def summarize_graph_edges(self, graph: BaseGraph, facet_properties: Optional[List] = None) -> Dict:
        """
        Summarize the edges in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph
        facet_properties: Optional[List]
            The properties to facet on

        Returns
        -------
        Dict
            The edge stats

        """
        stats: Dict = {
            TOTAL_EDGES: 0,
            EDGE_PREDICATES: set(),
            COUNT_BY_EDGE_PREDICATES: {'unknown': {'count': 0}},
            COUNT_BY_SPO: {},
        }
    
        stats[TOTAL_EDGES] = len(graph.edges())
        if facet_properties:
            for facet_property in facet_properties:
                stats[facet_property] = set()
    
        for u, v, k, data in graph.edges(keys=True, data=True):
            if 'predicate' not in data:
                stats[COUNT_BY_EDGE_PREDICATES]['unknown']['count'] += 1
                edge_predicate = "unknown"
            else:
                edge_predicate = data['predicate']
                stats[EDGE_PREDICATES].add(edge_predicate)
                if edge_predicate in stats[COUNT_BY_EDGE_PREDICATES]:
                    stats[COUNT_BY_EDGE_PREDICATES][edge_predicate]['count'] += 1
                else:
                    stats[COUNT_BY_EDGE_PREDICATES][edge_predicate] = {'count': 1}
            
                if facet_properties:
                    for facet_property in facet_properties:
                        stats = self.get_facet_counts(
                            data, stats, COUNT_BY_EDGE_PREDICATES, edge_predicate, facet_property
                        )
        
            u_data = graph.nodes()[u]
            v_data = graph.nodes()[v]
        
            if 'category' in u_data:
                u_category = u_data['category'][0]
            else:
                u_category = "unknown"
        
            if 'category' in v_data:
                v_category = v_data['category'][0]
            else:
                v_category = "unknown"
        
            key = f"{u_category}-{edge_predicate}-{v_category}"
            if key in stats[COUNT_BY_SPO]:
                stats[COUNT_BY_SPO][key]['count'] += 1
            else:
                stats[COUNT_BY_SPO][key] = {'count': 1}
        
            if facet_properties:
                for facet_property in facet_properties:
                    stats = self.get_facet_counts(data, stats, COUNT_BY_SPO, key, facet_property)
    
        stats[EDGE_PREDICATES] = sorted(list(stats[EDGE_PREDICATES]))
        if facet_properties:
            for facet_property in facet_properties:
                stats[facet_property] = sorted(list(stats[facet_property]))
    
        return stats

    def get_facet_counts(self, data: Dict, stats: Dict, x: str, y: str, facet_property: str) -> Dict:
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
                    if facet_property not in stats[x][y]:
                        stats[x][y][facet_property] = {}
                
                    if k in stats[x][y][facet_property]:
                        stats[x][y][facet_property][k]['count'] += 1
                    else:
                        stats[x][y][facet_property][k] = {'count': 1}
                        stats[facet_property].update([k])
            else:
                k = data[facet_property]
                if facet_property not in stats[x][y]:
                    stats[x][y][facet_property] = {}
            
                if k in stats[x][y][facet_property]:
                    stats[x][y][facet_property][k]['count'] += 1
                else:
                    stats[x][y][facet_property][k] = {'count': 1}
                    stats[facet_property].update([k])
        else:
            if facet_property not in stats[x][y]:
                stats[x][y][facet_property] = {}
            if 'unknown' in stats[x][y][facet_property]:
                stats[x][y][facet_property]['unknown']['count'] += 1
            else:
                stats[x][y][facet_property]['unknown'] = {'count': 1}
                stats[facet_property].update(['unknown'])
        return stats

    def save(self, filename: str, name: str = None):
        """
        Save the current GraphSummary to a specified file
        """
        stats = self.get_graph_summary(name)
        with open(filename, 'w') as gsh:
            yaml.dump(stats, gsh)
            

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
        A list of properties to facet on. For example, ``['provided_by']``

    """
    stats = summarize_graph(graph, graph_name, node_facet_properties, edge_facet_properties)
    with open(filename, 'w') as gsh:
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
        A list of properties to facet on. For example, ``['provided_by']``

    Returns
    -------
    Dict
        The stats dictionary

    """
    gs = GraphSummary(name, node_facet_properties, edge_facet_properties)
    return gs.summarize_graph(graph)
