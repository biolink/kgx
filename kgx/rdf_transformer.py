from collections import defaultdict
from typing import Union, Set, List

import click
import logging
import networkx as nx
import os
import rdflib
import uuid
from prefixcommons.curie_util import read_remote_jsonld_context
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL

from kgx.prefix_manager import PrefixManager
from kgx.rdf_graph_mixin import RdfGraphMixin
from kgx.transformer import Transformer
from kgx.utils.kgx_utils import get_toolkit
from kgx.utils.rdf_utils import find_category, property_mapping

biolink_prefix_map = read_remote_jsonld_context('https://biolink.github.io/biolink-model/context.jsonld')

# TODO: use OBO IRI from biolink model context once https://github.com/biolink/biolink-model/issues/211 is resolved
OBO = Namespace('http://purl.obolibrary.org/obo/')
OBAN = Namespace(biolink_prefix_map['OBAN'])
PMID = Namespace(biolink_prefix_map['PMID'])
BIOLINK = Namespace(biolink_prefix_map['@vocab'])
DEFAULT_EDGE_LABEL = 'related_to'

class RdfTransformer(RdfGraphMixin, Transformer):
    """
    Transformer that parses RDF and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This is the base class which is used to implement other RDF-based transformers.
    """

    OWL_PREDICATES = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    is_about = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
    has_subsequence = URIRef('http://purl.obolibrary.org/obo/RO_0002524')
    is_subsequence_of = URIRef('http://purl.obolibrary.org/obo/RO_0002525')

    def __init__(self, source_graph: nx.MultiDiGraph = None):
        super().__init__(source_graph)
        self.ontologies = []
        self.prefix_manager = PrefixManager()
        self.toolkit = get_toolkit()

    def parse(self, filename: str = None, input_format: str = None, provided_by: str = None) -> None:
        """
        Parse a file, containing triples, into a rdflib.Graph

        The file can be either a 'turtle' file or any other format supported by rdflib.

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : str
            The input file format. If None is provided then the format is guessed using rdflib.util.guess_format()
        provided_by : str
            Define the source providing the input file.

        """
        rdfgraph = rdflib.Graph()

        if input_format is None:
            input_format = rdflib.util.guess_format(filename)

        logging.info("Parsing {} with '{}' format".format(filename, input_format))
        rdfgraph.parse(filename, format=input_format)
        logging.info("{} parsed with {} triples".format(filename, len(rdfgraph)))

        # TODO: use source from RDF
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        else:
            if isinstance(filename, str):
                self.graph_metadata['provided_by'] = [os.path.basename(filename)]
            elif hasattr(filename, 'name'):
                self.graph_metadata['provided_by'] = [filename.name]

        self.load_networkx_graph(rdfgraph)
        self.load_node_attributes(rdfgraph)
        self.report()

    def add_ontology(self, file: str) -> None:
        """
        Load an ontology OWL into a Rdflib.Graph
        # TODO: is there better way of pre-loading required ontologies?
        """
        ont = rdflib.Graph()
        logging.info("Parsing {}".format(file))
        ont.parse(file, format=rdflib.util.guess_format(file))
        self.ontologies.append(ont)
        logging.info("{} parsed with {} triples".format(file, len(ont)))

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, predicates: Set[URIRef] = None, **kwargs) -> None:
        """
        Walk through the rdflib.Graph and load all required triples into networkx.MultiDiGraph

        By default this method loads the following predicates,
            - RDFS.subClassOf
            - OWL.sameAs
            - OWL.equivalentClass
            - is_about (IAO:0000136)
            - has_subsequence (RO:0002524)
            - is_subsequence_of (RO:0002525)

        This behavior can be overridden by providing a list of rdflib.URIRef that ought to be loaded
        via the 'predicates' parameter.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: list
            A list of rdflib.URIRef representing predicates to be loaded
        kwargs: dict
            Any additional arguments

        """
        if predicates is None:
            predicates = set()
            predicates = predicates.union(self.OWL_PREDICATES, [self.is_about, self.is_subsequence_of, self.has_subsequence])

        triples = rdfgraph.triples((None, None, None))
        logging.info("Loading from rdflib.Graph to networkx.MultiDiGraph")
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                if (p == self.is_about) and (p in predicates):
                    logging.info("Loading is_about predicate")
                    # if predicate is 'is_about' then treat object as publication
                    self.add_node_attribute(o, key=s, value='publications')
                elif (p == self.is_subsequence_of) and (p in predicates):
                    logging.info("Loading is_subsequence_of predicate")
                    # if predicate is 'is_subsequence_of'
                    self.add_edge(s, o, self.is_subsequence_of)
                elif (p == self.has_subsequence) and (p in predicates):
                    logging.info("Loading has_subsequence predicate")
                    # if predicate is 'has_subsequence', interpret the inverse relation 'is_subsequence_of'
                    self.add_edge(o, s, self.is_subsequence_of)
                elif any(p.lower() == x.lower() for x in predicates):
                    logging.info("Loading {} predicate, additional predicate".format(p))
                    self.add_edge(s, o, p)

    def load_node_attributes(self, rdfgraph: rdflib.Graph) -> None:
        """
        This method loads the properties of nodes into networkx.MultiDiGraph
        As there can be many values for a single key, all properties are lists by default.

        This method assumes that RdfTransformer.load_edges() has been called, and that all nodes
        have had their IRI as an attribute.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges

        """
        logging.info("Loading node attributes from rdflib.Graph into networkx.MultiDiGraph")
        with click.progressbar(self.graph.nodes(data=True), label='Progress') as bar:
            for n, data in bar:
                if 'iri' in data:
                    uriref = URIRef(data['iri'])
                else:
                    provided_by = self.graph_metadata.get('provided_by')
                    logging.warning("No 'iri' property for {} provided by {}".format(n, provided_by))
                    continue

                for s, p, o in rdfgraph.triples((uriref, None, None)):
                    if p in property_mapping:
                        # predicate corresponds to a property on subject
                        if not (isinstance(s, rdflib.term.BNode) and isinstance(o, rdflib.term.BNode)):
                            # neither subject nor object is a BNode
                            self.add_node_attribute(uriref, key=p, value=o)
                    elif isinstance(o, rdflib.term.Literal):
                        # object is a Literal
                        # i.e. predicate corresponds to a property on subject
                        self.add_node_attribute(uriref, key=p, value=o)

                category = find_category(uriref, [rdfgraph] + self.ontologies)
                logging.debug("Inferred '{}' as category for node '{}'".format(category, uriref))
                if category is not None:
                    self.add_node_attribute(uriref, key='category', value=category)


class ObanRdfTransformer(RdfTransformer):
    """
    Transformer that parses a 'turtle' file and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This Transformer supports OBAN style of modeling where,
    - it dereifies OBAN.association triples into a property graph form
    - it reifies property graph into OBAN.association triples

    """

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, predicates: Set[URIRef] = None, **kwargs) -> None:
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: list
            A list of rdflib.URIRef representing predicates to be loaded
        kwargs: dict
            Any additional arguments

        """
        if not predicates:
            predicates = set()
            predicates = predicates.union(self.OWL_PREDICATES)

        for rel in predicates:
            triples = rdfgraph.triples((None, rel, None))
            with click.progressbar(list(triples), label="Loading relation '{}'".format(rel)) as bar:
                for s, p, o in bar:
                    if not (isinstance(s, rdflib.term.BNode) and isinstance(o, rdflib.term.BNode)):
                        self.add_edge(s, o, p)

        # get all OBAN.associations
        associations = rdfgraph.subjects(RDF.type, OBAN.association)
        logging.info("Loading from rdflib.Graph into networkx.MultiDiGraph")
        with click.progressbar(list(associations), label='Progress') as bar:
            for association in bar:
                edge_attr = defaultdict(list)
                edge_attr['id'].append(str(association))

                # dereify OBAN.association
                subject = None
                object = None
                predicate = None

                # get all triples for association
                for s, p, o in rdfgraph.triples((association, None, None)):
                    if o.startswith(PMID):
                        edge_attr['publications'].append(o)
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        p = property_mapping.get(p, p)
                        if p == 'subject':
                            subject = o
                        elif p == 'object':
                            object = o
                        elif p == 'predicate':
                            predicate = o
                        else:
                            edge_attr[p].append(o)

                if predicate is None:
                    logging.warning("No 'predicate' for OBAN.association {}; defaulting to '{}'".format(association, self.DEFAULT_EDGE_LABEL))
                    predicate = DEFAULT_EDGE_LABEL

                if subject and object:
                    self.add_edge(subject, object, predicate)
                    for key, values in edge_attr.items():
                        for value in values:
                            self.add_edge_attribute(subject, object, predicate, key=key, value=value)

    def uriref(self, identifier: str) -> URIRef:
        """
        Generate a rdflib.URIRef for a given string.

        Parameters
        ----------
        identifier: str
            Identifier as string.

        Returns
        -------
        rdflib.URIRef
            URIRef form of the input `identifier`

        """
        if identifier in property_mapping:
            uri = property_mapping[identifier]
        else:
            uri = self.prefix_manager.expand(identifier)
        return URIRef(uri)

    def save_attribute(self, rdfgraph: rdflib.Graph, object_iri: URIRef, key: str, value: Union[List[str], str]) -> None:
        """
        Saves a node or edge attributes from networkx.MultiDiGraph into rdflib.Graph

        Intended to be used within `ObanRdfTransformer.save()`.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        object_iri: rdflib.URIRef
            IRI of an object in the graph
        key: str
            The name of the attribute
        value: Union[List[str], str]
            The value of the attribute; Can be either a List or just a string

        """
        element = self.toolkit.get_element(key)
        if element is None:
            return
        if element.is_a == 'association slot' or element.is_a == 'node property':
            if key in property_mapping:
                key = property_mapping[key]
            else:
                key = URIRef('{}{}'.format(BIOLINK, element.name.replace(' ', '_')))
            if not isinstance(value, (list, tuple, set)):
                value = [value]
            for value in value:
                if element.range == 'iri type':
                    value = URIRef('{}{}'.format(BIOLINK, ''.join(value.title().split(' '))))
                rdfgraph.add((object_iri, key, rdflib.term.Literal(value)))

    def save(self, filename: str = None, output_format: str = "turtle", **kwargs) -> None:
        """
        Transform networkx.MultiDiGraph into rdflib.Graph that follow OBAN-style reification and export
        this graph as a file (TTL, by default).

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format; default: 'turtle'
        kwargs: dict
            Any additional arguments

        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()

        # Register OBAN URL prefix (http://purl.org/oban/) as `OBAN` in the namespace.
        rdfgraph.bind('OBAN', str(OBAN))

        # <http://purl.obolibrary.org/obo/RO_0002558> is currently stored as OBO:RO_0002558 rather than RO:0002558
        # because of the bug in rdflib. See https://github.com/RDFLib/rdflib/issues/632
        rdfgraph.bind('OBO', str(OBO))
        rdfgraph.bind('biolink', str(BIOLINK))

        # saving all nodes
        for n, data in self.graph.nodes(data=True):
            if 'iri' not in n:
                uriRef = self.uriref(n)
            else:
                uriRef = URIRef(data['iri'])

            for key, value in data.items():
                if key not in ['id', 'iri']:
                    self.save_attribute(rdfgraph, uriRef, key=key, value=value)

        # saving all edges
        for u, v, data in self.graph.edges(data=True):
            if 'relation' not in data:
                raise Exception('Relation is a required edge property in the biolink model, edge {} --> {}'.format(u, v))

            if 'id' in data and data['id'] is not None:
                assoc_id = URIRef(data['id'])
            else:
                # generating a UUID for association
                assoc_id = URIRef('urn:uuid:{}'.format(uuid.uuid4()))

            rdfgraph.add((assoc_id, RDF.type, OBAN.association))
            rdfgraph.add((assoc_id, OBAN.association_has_subject, self.uriref(u)))
            rdfgraph.add((assoc_id, OBAN.association_has_predicate, self.uriref(data['relation'])))
            rdfgraph.add((assoc_id, OBAN.association_has_object, self.uriref(v)))

            for key, value in data.items():
                if key not in ['subject', 'relation', 'object']:
                    self.save_attribute(rdfgraph, assoc_id, key=key, value=value)

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)


class RdfOwlTransformer(RdfTransformer):
    """
    Transformer that parses an OWL ontology in RDF, while retaining class-class relationships.
    """

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, predicates: Set[URIRef] = None, **kwargs) -> None:
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: list
            A list of rdflib.URIRef representing predicates to be loaded
        kwargs: dict
            Any additional arguments
        """
        triples = rdfgraph.triples((None, RDFS.subClassOf, None))
        logging.info("Loading from rdflib.Graph to networkx.MultiDiGraph")
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                # ignoring blank nodes
                if isinstance(s, rdflib.term.BNode):
                    continue
                pred = None
                parent = None
                # TODO: does this block load all relevant bits from an OWL?
                if isinstance(o, rdflib.term.BNode):
                    # C SubClassOf R some D
                    for x in rdfgraph.objects(o, OWL.onProperty):
                        pred = x
                    for x in rdfgraph.objects(o, OWL.someValuesFrom):
                        parent = x
                    if pred is None or parent is None:
                        logging.warning("Do not know how to handle BNode: {}".format(o))
                        continue
                else:
                    # C SubClassOf D (C and D are named classes)
                    pred = p
                    parent = o
                self.add_edge(s, parent, pred)

        relations = rdfgraph.subjects(RDF.type, OWL.ObjectProperty)
        logging.info("Loading relations")
        with click.progressbar(relations, label='Progress') as bar:
            for relation in bar:
                for _, p, o in rdfgraph.triples((relation, None, None)):
                    if o.startswith('http://purl.obolibrary.org/obo/RO_'):
                        self.add_edge(relation, o, p)
                    else:
                        self.add_node_attribute(relation, key=p, value=o)
                self.add_node_attribute(relation, key='category', value='relation')
