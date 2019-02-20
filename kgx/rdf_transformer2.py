import click, rdflib, logging, os
import networkx as nx

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL

from .transformer import Transformer
from .utils.rdf_utils import find_category, category_mapping, equals_predicates, property_mapping, predicate_mapping, process_iri, make_curie

from collections import defaultdict

from abc import ABCMeta, abstractmethod

class RdfTransformer(Transformer, metaclass=ABCMeta):
    def __init__(self, t:Transformer=None):
        super().__init__(t)
        self.ontologies = []

    def parse(self, filename:str=None, provided_by:str=None):
        rdfgraph = rdflib.Graph()
        rdfgraph.parse(filename, format=rdflib.util.guess_format(filename))

        logging.info("Parsed : {}".format(filename))

        if provided_by is None:
            provided_by = os.path.basename(filename)

        self.load_networkx_graph(rdfgraph, self.graph, provided_by=provided_by)
        self.load_node_attributes(rdfgraph, provided_by=provided_by)

    def add_ontology(self, owlfile:str):
        ont = rdflib.Graph()
        ont.parse(owlfile, format=rdflib.util.guess_format(owlfile))
        self.ontologies.append(ont)
        logging.info("Parsed : {}".format(owlfile))

    def add_node_attribute(self, node:str, key:str, value:str) -> None:
        """
        All node attributes except for their iri and id are treated as lists.
        This is beneficial to the clique merging process, which will merge list
        attributes of nodes when merging those nodes.
        """
        if node in self.graph:
            attr_dict = self.graph.node[node]
            if key in attr_dict:
                attr_dict[key].append(value)
            else:
                attr_dict[key] = [value]
        else:
            if key is not None:
                self.graph.add_node(node, **{key : [value]})
            else:
                self.graph.add_node(node)

    @abstractmethod
    def load_networkx_graph(self, rdfgraph:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        """
        This method should be overridden and implemented by the derived class,
        and should load all desired nodes and edges from rdfgraph into nxgraph.

        Note: All nodes must have their iri property set, otherwise they will be
        ignored when node attributes are loaded into the NetworkX graph.
        """
        pass

    def load_node_attributes(self, rdfgraph:rdflib.Graph, provided_by:str=None):
        """
        This method loads the properties of nodes in the NetworkX graph. As there
        can be many values for a single key, all properties are lists by default.

        This method assumes that load_edges has been called, and that all nodes
        have had their IRI saved as an attribute.
        """
        with click.progressbar(self.graph.nodes(), label='loading node attributes') as bar:
            for node_id in bar:
                if 'iri' in self.graph.node[node_id]:
                    iri = self.graph.node[node_id]['iri']
                else:
                    logging.warning("Expected IRI for {} provided by {}".format(node_id, provided_by))
                    continue

                node_attr = defaultdict(list)

                for s, p, o in rdfgraph.triples((URIRef(iri), None, None)):
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        p = property_mapping.get(p, make_curie(process_iri(p)))
                        o = process_iri(str(o))
                        node_attr[p].append(o)

                category = find_category(iri, [rdfgraph] + self.ontologies)

                if category is not None:
                    node_attr['category'].append(category)

                if provided_by is not None:
                    node_attr['provided_by'].append(provided_by)

                for k, values in node_attr.items():
                    if isinstance(values, (list, set, tuple)):
                        node_attr[k] = [make_curie(v) for v in values]

                for key, value in node_attr.items():
                    self.graph.node[node_id][key] = value

class ObanRdfTransformer2(RdfTransformer):
    OBAN = Namespace('http://purl.org/oban/')

    def load_networkx_graph(self, rdfgraph:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        associations = list(rdfgraph.subjects(RDF.type, self.OBAN.association))
        with click.progressbar(associations, label='loading graph') as bar:
            for association in bar:
                edge_attr = defaultdict(list)

                edge_attr['iri'] = str(association)
                edge_attr['id'] = make_curie(association)

                for s, p, o in rdfgraph.triples((association, None, None)):
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        p = property_mapping.get(p, make_curie(process_iri(p)))
                        o = process_iri(o)
                        edge_attr[p].append(o)

                if 'predicate' not in edge_attr:
                    edge_attr['predicate'].append('related to')

                if provided_by is not None:
                    edge_attr['provided_by'].append(provided_by)

                subjects = edge_attr['subject']
                objects = edge_attr['object']

                for k, values in edge_attr.items():
                    if isinstance(values, (list, set, tuple)):
                        edge_attr[k] = [make_curie(v) for v in values]

                for subject_iri in subjects:
                    for object_iri in objects:
                        sid = make_curie(subject_iri)
                        oid = make_curie(object_iri)

                        nxgraph.add_edge(sid, oid, **edge_attr)

                        nxgraph.node[sid]['iri'] = subject_iri
                        nxgraph.node[sid]['id'] = sid

                        nxgraph.node[oid]['iri'] = object_iri
                        nxgraph.node[oid]['id'] = oid

class HgncRdfTransformer(RdfTransformer):
    def load_networkx_graph(self, rdfgraph:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        triples = list(rdfgraph.triples((None, None, None)))
        with click.progressbar(triples, label='loading graph') as bar:
            for s, p, o in bar:
                s_iri, p, o_iri = str(s), str(p), str(o)

                s = make_curie(s_iri)
                o = make_curie(o_iri)

                if p == 'http://purl.obolibrary.org/obo/IAO_0000136':
                    self.add_node_attribute(o, 'publications', s_iri)
                    nxgraph.node[o]['iri'] = o_iri
                elif p == 'http://purl.obolibrary.org/obo/RO_0002524':
                    nxgraph.add_edge(s, o, predicate='has_subsequence', provided_by=provided_by)
                    nxgraph.node[s]['iri'] = s_iri
                    nxgraph.node[o]['iri'] = o_iri
                elif p == 'http://purl.obolibrary.org/obo/RO_0002525':
                    nxgraph.add_edge(o, s, predicate='has_subsequence', provided_by=provided_by)
                    nxgraph.node[s]['iri'] = s_iri
                    nxgraph.node[o]['iri'] = o_iri
                elif any(p.lower() == predicate.lower() for predicate in equals_predicates):
                    self.graph.add_node(s)
                    self.graph.node[s]['iri'] = s_iri
