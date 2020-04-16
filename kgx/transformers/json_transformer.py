import json, logging

import networkx as nx
import stringcase
from prefixcommons import contract_uri

from kgx.prefix_manager import PrefixManager
from kgx.mapper import get_prefix
from kgx.transformers.pandas_transformer import PandasTransformer
from typing import List, Dict

from kgx.utils.graph_utils import get_category_via_superclass
from kgx.utils.kgx_utils import get_curie_lookup_service, get_toolkit
from kgx.utils.rdf_utils import infer_category


class JsonTransformer(PandasTransformer):
    """
    Transformer that parses a JSON, and loads nodes and edges into a networkx.MultiDiGraph
    """

    def parse(self, filename: str, input_format: str = 'json', provided_by: str = None, **kwargs) -> None:
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
        provided_by: str
            Define the source providing the input file
        kwargs: dict
            Any additional arguments

        """
        logging.info("Parsing {}".format(filename))
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        with open(filename, 'r') as FH:
            obj = json.load(FH)
            self.load(obj)

    def load(self, obj: Dict[str, List]) -> None:
        """
        Load a JSON object, containing nodes and edges, into a networkx.MultiDiGraph

        Parameters
        ----------
        obj: dict
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
        logging.info("Loading {} nodes into networkx.MultiDiGraph".format(len(nodes)))
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
        logging.info("Loading {} edges into networkx.MultiDiGraph".format(len(edges)))
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
            node = data.copy()
            node['id'] = id
            nodes.append(node)
        for s, o, data in self.graph.edges(data=True):
            edge = data.copy()
            edge['subject'] = s
            edge['object'] = o
            edges.append(edge)

        return {
            'nodes': nodes,
            'edges': edges
        }

    def save(self, filename: str, **kwargs) -> None:
        """
        Write networkx.MultiDiGraph to a file as JSON.

        Parameters
        ----------
        filename: str
            Filename to write to
        kwargs: dict
            Any additional arguments

        """
        obj = self.export()
        with open(filename, 'w') as WH:
            WH.write(json.dumps(obj, indent=4, sort_keys=True))


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

    def parse(self, filename: str, input_format: str = 'json', provided_by: str = None, **kwargs) -> None:
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
        provided_by: str
            Define the source providing the input file
        kwargs: dict
            Any additional arguments

        """
        logging.info("Parsing {}".format(filename))
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
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
        curie = contract_uri(node['id'])[0]
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
        if 'synonyms' in node_properties:
            fixed_node['synonyms'] = node_properties['synonyms']

        if 'category' not in node:
            category = self.get_category(curie, node)
            if category:
                fixed_node['category'] = [category]
            else:
                fixed_node['category'] = ['biolink:OntologyClass']

        super().load_node(fixed_node)
        if 'equivalent_nodes' in node_properties:
            for n in node_properties['equivalent_nodes']:
                data = {'subject': fixed_node['id'], 'edge_label': 'biolink:same_as', 'object': n, 'relation': 'owl:sameAs'}
                self.graph.add_edge(fixed_node['id'], n, **data)

    def load_edge(self, edge: dict) -> None:
        """
        Load an edge from Obograph JSON into a networkx.MultiDiGraph

        Parameters
        ----------
        edge : dict
            An edge

        """
        fixed_edge = dict()
        fixed_edge['subject'] = edge['sub']
        if PrefixManager.is_iri(edge['pred']):
            curie = self.prefix_manager.contract(edge['pred'])
            fixed_edge['relation'] = curie
            if self.graph.has_node(curie):
                fixed_edge['edge_label'] = f"biolink:{self.graph.nodes[curie]['name'].replace(' ', '_')}"
                # TODO: validate edge_label to biolink model
            else:
                fixed_edge['edge_label'] = 'related_to'
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

        fixed_edge['object'] = edge['obj']
        for x in edge.keys():
            if x not in {'sub', 'pred', 'obj'}:
                fixed_edge[x] = edge[x]
        super().load_edge(fixed_edge)

    def get_category(self, curie: str, node: dict) -> str:
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
        str
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
            prefix = get_prefix(curie)
            if prefix == 'CHEBI':
                category = "biolink:ChemicalSubstance"
            elif prefix == 'MONDO':
                category = "biolink:Disease"
            elif prefix == 'UBERON':
                category = "biolink:AnatomicalEntity"

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
        properties = {}
        if 'definition' in meta:
            # parse 'definition' as 'description'
            description = meta['definition']['val']
            properties['description'] = description

        if 'synonyms' in meta:
            # parse 'synonyms' as 'synonyms'
            synonyms = [s['val'] for s in meta['synonyms']]
            properties['synonyms'] = synonyms

        if 'xrefs' in meta:
            # parse 'xrefs' as 'xrefs'
            xrefs = [x['val'] for x in meta['xrefs']]
            properties['xrefs'] = xrefs

        equivalent_nodes = []
        if 'basicPropertyValues' in meta:
            # parse SKOS_EXACT_MATCH entries as 'equivalent_nodes'
            for p in meta['basicPropertyValues']:
                if p['pred'] in {self.SKOS_EXACT_MATCH}:
                    n = contract_uri(p['val'])
                    if not n:
                        n = [p['val']]
                    equivalent_nodes.append(n[0])
        properties['equivalent_nodes'] = equivalent_nodes
        return properties
