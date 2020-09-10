import gzip
import json

import networkx
import networkx as nx
import stringcase

from kgx.config import get_logger
from kgx.prefix_manager import PrefixManager
from kgx.transformers.pandas_transformer import PandasTransformer
from typing import List, Dict, Any, Optional

from kgx.utils.kgx_utils import get_toolkit, get_biolink_element, format_biolink_slots

log = get_logger()


class JsonTransformer(PandasTransformer):
    """
    Transformer that parses a JSON, and loads nodes and edges into a networkx.MultiDiGraph

    Parameters
    ----------
    source_graph: Optional[networkx.MultiDiGraph]
        The source graph

    """

    def __init__(self, source_graph: Optional[networkx.MultiDiGraph] = None):
        super().__init__(source_graph)

    def parse(self, filename: str, input_format: str = 'json', compression: Optional[str] = None, provided_by: Optional[str] = None, **kwargs) -> None:
        """
        Parse a JSON file of the format,

        {
            "nodes" : [...],
            "edges" : [...],
        }

        Parameters
        ----------
        filename: str
            JSON file to read from
        input_format: str
            The input file format (``json``, by default)
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by: Optional[str]
            Define the source providing the input file
        kwargs: dict
            Any additional arguments

        """
        log.info("Parsing {}".format(filename))
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if compression == 'gz':
            with gzip.open(filename, 'rb') as FH:
                obj = json.load(FH)
                self.load(obj)
        else:
            with open(filename, 'r') as FH:
                obj = json.load(FH)
                self.load(obj)

    def load(self, obj: Dict[str, Any]) -> None:
        """
        Load a JSON object, containing nodes and edges, into a networkx.MultiDiGraph

        Parameters
        ----------
        obj: Dict[str, Any]
            JSON Object with all nodes and edges

        """
        if 'nodes' in obj:
            self.load_nodes(obj['nodes'])
        if 'edges' in obj:
            self.load_edges(obj['edges'])

    def load_nodes(self, nodes: List[Dict]) -> None:
        """
        Load a list of nodes into a networkx.MultiDiGraph

        Parameters
        ----------
        nodes: list
            List of nodes

        """
        log.info("Loading {} nodes into networkx.MultiDiGraph".format(len(nodes)))
        for node in nodes:
            self.load_node(node)

    def load_edges(self, edges: List[Dict]) -> None:
        """
        Load a list of edges into a networkx.MultiDiGraph

        Parameters
        ----------
        edges: list
            List of edges

        """
        log.info("Loading {} edges into networkx.MultiDiGraph".format(len(edges)))
        for edge in edges:
            self.load_edge(edge)

    def export(self) -> Dict:
        """
        Export networkx.MultiDiGraph as a dictionary.

        Returns
        -------
        dict
            A dictionary with a list nodes and a list of edges

        """
        nodes = []
        edges = []
        for id, data in self.graph.nodes(data=True):
            nodes.append(data)
        for s, o, data in self.graph.edges(data=True):
            edges.append(data)

        return {
            'nodes': nodes,
            'edges': edges
        }

    def save(self, filename: str, output_format: str = 'json', compression: Optional[str] = None, **kwargs) -> str:
        """
        Write networkx.MultiDiGraph to a file as JSON.

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output file format (``json``, by default)
        compression: Optional[str]
            The compression type. For example, ``gz``
        kwargs: dict
            Any additional arguments

        Returns
        -------
        str
            The filename

        """
        obj = self.export()
        if compression == 'gz':
            with gzip.open(filename, 'wb') as WH:
                WH.write(json.dumps(obj, indent=4, sort_keys=True))
        else:
            with open(filename, 'w') as WH:
                WH.write(json.dumps(obj, indent=4, sort_keys=True))
        return filename


class ObographJsonTransformer(JsonTransformer):
    """
    Transformer that parses an Obograph JSON, and loads nodes and edges into a networkx.MultiDiGraph
    """

    HAS_OBO_NAMESPACE = 'http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'
    SKOS_EXACT_MATCH = 'http://www.w3.org/2004/02/skos/core#exactMatch'

    def __init__(self, source_graph: nx.MultiDiGraph = None):
        super().__init__(source_graph)
        self.toolkit = get_toolkit()
        self.prefix_manager = PrefixManager()
        self.ecache = {}

    def parse(self, filename: str, input_format: str = 'json', compression: Optional[str] = None, provided_by: Optional[str] = None, **kwargs) -> None:
        """
        Parse Obograph JSON file of the format,

        {
          "graphs": [
            {
              "nodes" : [
                {
                  "id" : "UBERON:0002102",
                  "lbl" : "forelimb"
                }, {
                  "id" : "UBERON:0002101",
                  "lbl" : "limb"
                }
              ],
              "edges" : [
                {
                  "subj" : "UBERON:0002102",
                  "pred" : "is_a",
                  "obj" : "UBERON:0002101"
                }
              ]
            }
          ]
        }

        Parameters
        ----------
        filename: str
            JSON file to read from
        input_format: str
            The input file format (``json``, by default)
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by: Optional[str]
            Define the source providing the input file
        kwargs: dict
            Any additional arguments

        """
        log.info("Parsing {}".format(filename))
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if compression == 'gz':
            with gzip.open(filename, 'rb') as FH:
                obj = json.load(FH)
                self.load(obj['graphs'][0])
        else:
            with open(filename, 'r') as FH:
                obj = json.load(FH)
                self.load(obj['graphs'][0])

    def load_node(self, node: Dict) -> None:
        """
        Load a node into a networkx.MultiDiGraph

        Parameters
        ----------
        node : dict
            A node

        """
        curie = self.prefix_manager.contract(node['id'])
        node_properties = {}
        if 'meta' in node:
            node_properties = self.parse_meta(node['id'], node['meta'])

        fixed_node = dict()
        fixed_node['id'] = curie
        if 'lbl' in node:
            fixed_node['name'] = node['lbl']
        fixed_node['iri'] = node['id']

        if 'description' in node_properties:
            fixed_node['description'] = node_properties['description']
        if 'synonym' in node_properties:
            fixed_node['synonym'] = node_properties['synonym']
        if 'xrefs' in node_properties:
            fixed_node['xrefs'] = node_properties['xrefs']
        if 'subsets' in node_properties:
            fixed_node['subsets'] = node_properties['subsets']

        if 'category' not in node:
            category = self.get_category(curie, node)
            if category:
                fixed_node['category'] = [category]
            else:
                fixed_node['category'] = ['biolink:OntologyClass']
        if 'equivalent_nodes' in node_properties:
            equivalent_nodes = node_properties['equivalent_nodes']
            fixed_node['same_as'] = equivalent_nodes
            # for n in node_properties['equivalent_nodes']:
            #     data = {'subject': fixed_node['id'], 'edge_label': 'biolink:same_as', 'object': n, 'relation': 'owl:sameAs'}
            #     super().load_node({'id': n, 'category': ['biolink:OntologyClass']})
            #     self.graph.add_edge(fixed_node['id'], n, **data)
        super().load_node(fixed_node)

    def load_edge(self, edge: dict) -> None:
        """
        Load an edge from Obograph JSON into a networkx.MultiDiGraph

        Parameters
        ----------
        edge : dict
            An edge

        """
        fixed_edge = dict()
        fixed_edge['subject'] = self.prefix_manager.contract(edge['sub'])
        if PrefixManager.is_iri(edge['pred']):
            curie = self.prefix_manager.contract(edge['pred'])
            if curie in self.ecache:
                edge_label = self.ecache[curie]
            else:
                element = get_biolink_element(curie)
                if not element:
                    try:
                        mapping = self.toolkit.get_by_mapping(edge['pred'])
                        element = self.toolkit.get_element(mapping)
                    except ValueError as e:
                        log.error(e)

                if element:
                    edge_label = format_biolink_slots(element.name.replace(',', ''))
                    fixed_edge['edge_label'] = edge_label
                else:
                    edge_label = 'biolink:related_to'
                self.ecache[curie] = edge_label
            fixed_edge['edge_label'] = edge_label
            fixed_edge['relation'] = curie
        else:
            if edge['pred'] == 'is_a':
                fixed_edge['edge_label'] = 'biolink:subclass_of'
                fixed_edge['relation'] = 'rdfs:subClassOf'
            elif edge['pred'] == 'has_part':
                fixed_edge['edge_label'] = 'biolink:has_part'
                fixed_edge['relation'] = "BFO:0000051"
            elif edge['pred'] == 'part_of':
                fixed_edge['edge_label'] = 'biolink:part_of'
                fixed_edge['relation'] = "BFO:0000050"
            else:
                fixed_edge['edge_label'] = f"biolink:{edge['pred'].replace(' ', '_')}"
                fixed_edge['relation'] = edge['pred']

        fixed_edge['object'] = self.prefix_manager.contract(edge['obj'])
        for x in edge.keys():
            if x not in {'sub', 'pred', 'obj'}:
                fixed_edge[x] = edge[x]
        super().load_edge(fixed_edge)

    def get_category(self, curie: str, node: dict) -> Optional[str]:
        """
        Get category for a given CURIE.

        Parameters
        ----------
        curie: str
            Curie for node
        node: dict
            Node data

        Returns
        -------
        Optional[str]
            Category for the given node CURIE.

        """
        category = None
        # use meta.basicPropertyValues
        if 'meta' in node and 'basicPropertyValues' in node['meta']:
            for p in node['meta']['basicPropertyValues']:
                if p['pred'] == self.HAS_OBO_NAMESPACE:
                    category = p['val']
                    element = self.toolkit.get_element(category)
                    if element:
                        category = f"biolink:{stringcase.pascalcase(stringcase.snakecase(element.name))}"
                    else:
                        element = self.toolkit.get_by_mapping(category)
                        if element:
                            category = f"biolink:{stringcase.pascalcase(stringcase.snakecase(element.name))}"
                        else:
                            category = 'biolink:OntologyClass'

        if not category or category == 'biolink:OntologyClass':
            prefix = PrefixManager.get_prefix(curie)
            # TODO: the mapping should be via biolink-model lookups
            if prefix == 'HP':
                category = "biolink:PhenotypicFeature"
            elif prefix == 'CHEBI':
                category = "biolink:ChemicalSubstance"
            elif prefix == 'MONDO':
                category = "biolink:Disease"
            elif prefix == 'UBERON':
                category = "biolink:AnatomicalEntity"
            elif prefix == 'SO':
                category = "biolink:SequenceFeature"
            elif prefix == 'CL':
                category = "biolink:Cell"
            elif prefix == 'PR':
                category = "biolink:Protein"
            elif prefix == 'NCBITaxon':
                category = "organism taxon"
            else:
                log.debug(f"{curie} Could not find a category mapping for '{category}'; Defaulting to 'biolink:OntologyClass'")
        return category

    def parse_meta(self, node: str, meta: dict):
        """
        Parse 'meta' field of a node.

        Parameters
        ----------
        node: str
            Node identifier
        meta: dict
            meta dictionary for the node

        Returns
        -------
        dict
            A dictionary that contains 'description', 'synonyms',
            'xrefs', and 'equivalent_nodes'.

        """
        # cross species links are in meta; this needs to be parsed properly too
        # do not put assumptions in code; import as much as possible

        properties = {}
        if 'definition' in meta:
            # parse 'definition' as 'description'
            description = meta['definition']['val']
            properties['description'] = description

        if 'subsets' in meta:
            # parse 'subsets'
            subsets = meta['subsets']
            properties['subsets'] = [x.split('#')[1] if '#' in x else x for x in subsets]

        if 'synonyms' in meta:
            # parse 'synonyms' as 'synonym'
            synonyms = [s['val'] for s in meta['synonyms']]
            properties['synonym'] = synonyms

        if 'xrefs' in meta:
            # parse 'xrefs' as 'xrefs'
            xrefs = [x['val'] for x in meta['xrefs']]
            properties['xrefs'] = xrefs

        if 'deprecated' in meta:
            # parse 'deprecated' flag
            properties['deprecated'] = meta['deprecated']

        equivalent_nodes = []
        if 'basicPropertyValues' in meta:
            # parse SKOS_EXACT_MATCH entries as 'equivalent_nodes'
            for p in meta['basicPropertyValues']:
                if p['pred'] in {self.SKOS_EXACT_MATCH}:
                    n = self.prefix_manager.contract(p['val'])
                    if not n:
                        n = p['val']
                    equivalent_nodes.append(n)
        properties['equivalent_nodes'] = equivalent_nodes
        return properties
