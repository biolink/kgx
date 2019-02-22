import click, rdflib, logging, os
import networkx as nx

from typing import Tuple

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL

from .transformer import Transformer
from .utils.rdf_utils import find_category, category_mapping, equals_predicates, property_mapping, predicate_mapping, process_iri, make_curie, is_property_multivalued

from collections import defaultdict

from abc import ABCMeta, abstractmethod

OBAN = Namespace('http://purl.org/oban/')

class RdfTransformer(Transformer, metaclass=ABCMeta):
    def __init__(self, t:Transformer=None):
        super().__init__(t)
        self.ontologies = []

    def parse(self, filename:str=None, provided_by:str=None):
        rdfgraph = rdflib.Graph()

        fmt = rdflib.util.guess_format(filename)
        logging.info("Parsing {} with {} format".format(filename, fmt))
        rdfgraph.parse(filename, format=fmt)

        logging.info("Parsed : {}".format(filename))

        if provided_by is None:
            provided_by = os.path.basename(filename)

        self.load_networkx_graph(rdfgraph, self.graph, provided_by=provided_by)
        self.load_node_attributes(rdfgraph, provided_by=provided_by)

    def add_ontology(self, owlfile:str):
        ont = rdflib.Graph()
        ont.parse(owlfile, format=rdflib.util.guess_format(owlfile))
        self.ontologies.append(ont)
        logging.info("Parsed  {}".format(owlfile))

    def add_node(self, iri:URIRef) -> str:
        """
        This method should be used by all derived classes when adding an edge to
        the graph. This ensures that the node's identifier is a CURIE, and that
        its IRI property is set.

        Returns the CURIE identifier for the node in the NetworkX graph.
        """
        n = make_curie(iri)
        if n not in self.graph:
            self.graph.add_node(n, iri=str(iri))
        return n

    def add_edge(self, subject_iri:URIRef, object_iri:URIRef, predicate_iri:URIRef) -> Tuple[str, str, str]:
        """
        This method should be used by all derived classes when adding an edge to
        the graph. This ensures that the nodes identifiers are CURIEs, and that
        their IRI properties are set.

        Returns the CURIE identifiers used for the subject and object in the
        NetworkX graph, and the processed edge_label.
        """
        s = self.add_node(subject_iri)
        o = self.add_node(object_iri)

        relation = make_curie(predicate_iri)
        edge_label = process_iri(predicate_iri).replace(' ', '_')

        if not self.graph.has_edge(s, o, key=edge_label):
            self.graph.add_edge(s, o, key=edge_label, relation=relation, edge_label=edge_label)

        return s, o, edge_label

    def add_node_attribute(self, iri:URIRef, key:str, value:str) -> None:
        """
        Adds an attribute to a node, respecting whether or not that property
        should be multi-valued. Multi-valued properties will not contain
        duplicates.

        The key may be a URIRef or a URI string that maps onto a property name
        in `property_mapping`.

        If the node does not exist then it is created.

        Parameters
        ----------
        iri : The identifier of a node in the NetworkX graph
        key : The name of the attribute, may be a URIRef or URI string
        value : The value of the attribute
        """
        if key.lower() in is_property_multivalued:
            key = key.lower()
        else:
            if not isinstance(key, URIRef):
                key = URIRef(key)
            key = property_mapping.get(key)

        if key is not None:
            n = self.add_node(iri)
            attr_dict = self.graph.node[n]
            self.__add_attribute(attr_dict, key, value)

    def add_edge_attribute(self, subject_iri:URIRef, object_iri:URIRef, predicate_iri:URIRef, key:str, value:str) -> None:
        """
        Adds an attribute to an edge, respecting whether or not that property
        should be multi-valued. Multi-valued properties will not contain
        duplicates.

        The key may be a URIRef or a URI string that maps onto a property name
        in `property_mapping`.

        If the nodes or their edge does not yet exist then they will be created.

        Parameters
        ----------
        iri : The identifier of a node in the NetworkX graph
        key : The name of the attribute, may be a URIRef or URI string
        value : The value of the attribute
        """
        if key.lower() in is_property_multivalued:
            key = key.lower()
        else:
            if not isinstance(key, URIRef):
                key = URIRef(key)
            key = property_mapping.get(key)

        if key is not None:
            s, o, edge_label = self.add_edge(subject_iri, object_iri, predicate_iri)
            attr_dict = self.graph.get_edge_data(s, o, key=edge_label)
            self.__add_attribute(attr_dict, key, value)

    def __add_attribute(self, attr_dict:dict, key:str, value:str):
        """
        Adds an attribute to the attribute dictionary, respecting whether or not
        that attribute should be multi-valued. Multi-valued attributes will not
        contain duplicates.
        Some attributes are singular forms of others, e.g. name -> synonym. In
        such cases overflowing values will be placed into the correlating
        multi-valued attribute.

        Parameters
        ----------
        attr_dict : The dictionary representing the attribute set of a NetworkX
                    node or edge. It can be aquired with G.node[n] or G.edge[u][v].
        key : The name of the attribute
        value : The value of the attribute
        """
        if key is None or key not in is_property_multivalued:
            return

        value = make_curie(process_iri(value))

        if is_property_multivalued[key]:
            if key not in attr_dict:
                attr_dict[key] = [value]
            elif value not in attr_dict[key]:
                attr_dict[key].append(value)
        else:
            if key not in attr_dict:
                attr_dict[key] = value
            elif key == 'name':
                self.__add_attribute(attr_dict, 'synonym', value)

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
            for node in bar:
                if 'iri' in self.graph.node[node]:
                    iri = self.graph.node[node]['iri']
                else:
                    logging.warning("Expected IRI for {} provided by {}".format(node, provided_by))
                    continue

                for s, p, o in rdfgraph.triples((URIRef(iri), None, None)):
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        self.add_node_attribute(node, p, o)

                category = find_category(iri, [rdfgraph] + self.ontologies)

                if category is not None:
                    self.add_node_attribute(iri, 'category', category)

                if provided_by is not None:
                    self.add_node_attribute(iri, 'provided_by', provided_by)

class ObanRdfTransformer2(RdfTransformer):
    ontological_predicates = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    def load_networkx_graph(self, rdfgraph:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        for rel in self.ontological_predicates:
            triples = list(rdfgraph.triples((None, rel, None)))
            with click.progressbar(triples, label='loading {}'.format(rel)) as bar:
                for s, p, o in bar:
                    self.add_edge(s, o, p)

        associations = list(rdfgraph.subjects(RDF.type, OBAN.association))
        with click.progressbar(associations, label='loading graph') as bar:
            for association in bar:
                edge_attr = defaultdict(list)

                subjects = []
                objects = []
                edge_labels = []

                for s, p, o in rdfgraph.triples((association, None, None)):
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        p = property_mapping.get(p)
                        if p == 'subject':
                            subjects.append(o)
                        elif p == 'object':
                            objects.append(o)
                        elif p == 'predicate':
                            edge_labels.append(o)
                        else:
                            edge_attr[p].append(o)

                if len(edge_labels) == 0:
                    edge_labels.append('related_to')

                if provided_by is not None:
                    edge_attr['provided_by'].append(provided_by)

                for subject_iri in subjects:
                    for object_iri in objects:
                        for edge_label in edge_labels:
                            for key, values in edge_attr.items():
                                for value in values:
                                    self.add_edge_attribute(
                                        subject_iri,
                                        object_iri,
                                        edge_label,
                                        key=key,
                                        value=value
                                    )

class HgncRdfTransformer(RdfTransformer):
    is_about = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
    has_subsequence = URIRef('http://purl.obolibrary.org/obo/RO_0002524')
    is_subsequence_of = URIRef('http://purl.obolibrary.org/obo/RO_0002525')
    ontological_predicates = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    def load_networkx_graph(self, rdfgraph:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        triples = list(rdfgraph.triples((None, None, None)))

        with click.progressbar(triples, label='loading graph') as bar:
            for s, p, o in bar:
                if p == self.is_about:
                    self.add_node_attribute(o, s, 'publications')
                elif p == self.has_subsequence:
                    self.add_edge(o, s, self.is_subsequence_of)
                elif p == self.is_subsequence_of:
                    self.add_edge(s, o, self.is_subsequence_of)
                elif any(p.lower() == x.lower() for x in self.ontological_predicates):
                    self.add_edge(s, o, p)

class RdfOwlTransformer2(RdfTransformer):
    """
    Transforms from an OWL ontology in RDF, retaining class-class
    relationships
    """
    def load_networkx_graph(self, rg:rdflib.Graph, nxgraph:nx.Graph, provided_by:str=None):
        for s, p, o in rg.triples((None,RDFS.subClassOf,None)):
            if isinstance(s, rdflib.term.BNode):
                continue
            pred = None
            parent = None
            attr_dict = {}
            if isinstance(o, rdflib.term.BNode):
                # C SubClassOf R some D
                parent = None
                for x in rg.objects(o, OWL.onProperty):
                    pred = x
                for x in rg.objects(o, OWL.someValuesFrom):
                    parent = x
                if pred is None or parent is None:
                    logging.warning("Do not know how to handle: {}".format(o))
            else:
                # C SubClassOf D (C and D are named classes)
                pred = p
                parent = o
            self.add_edge_attribute(s, parent, pred, key='provided_by', value=provided_by)
