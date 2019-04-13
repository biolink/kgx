import click, rdflib, logging, os, uuid, bmt
import networkx as nx

from typing import Tuple, Union, List, Dict

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL

from kgx.transformer import Transformer
from kgx.utils.rdf_utils import find_category, category_mapping, equals_predicates, property_mapping, predicate_mapping, process_iri, make_curie, is_property_multivalued

from collections import defaultdict

from abc import ABCMeta, abstractmethod

from prefixcommons.curie_util import read_remote_jsonld_context

biolink_prefix_map = read_remote_jsonld_context('https://raw.githubusercontent.com/biolink/biolink-model/master/context.jsonld')

# TODO: use OBO IRI from biolink model context once https://github.com/biolink/biolink-model/issues/211 is resolved
OBO = Namespace('http://purl.obolibrary.org/obo/')
OBAN = Namespace(biolink_prefix_map['OBAN'])
PMID = Namespace(biolink_prefix_map['PMID'])
BIOLINK = Namespace(biolink_prefix_map['biolink'])
DEFAULT_EDGE_LABEL = 'related_to'
OWL_PREDICATES = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

class RdfTransformer(Transformer):
    """
    Transformer that parses RDF and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This is the base class which is used to implement other RDF-based transformers.

    TODO: we will have some of the same logic if we go from a triplestore. How to share this?
    """

    def __init__(self, source_graph: nx.MultiDiGraph = None):
        super().__init__(source_graph)
        self.ontologies = []

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

    def add_node(self, iri: URIRef) -> str:
        """
        This method should be used by all derived classes when adding a node to
        the networkx.MultiDiGraph. This ensures that a node's identifier is a CURIE,
        and that it's 'iri' property is set.

        Returns the CURIE identifier for the node in the networkx.MultiDiGraph

        Parameters
        ----------
        iri : rdflib.URIRef
            IRI of a node

        Returns
        -------
        str
            The CURIE identifier of a node

        """
        kwargs = {
            'iri': str(iri),
        }
        if 'provided_by' in self.graph_metadata:
            kwargs['provided_by'] = self.graph_metadata['provided_by']

        n = make_curie(iri)

        if n not in self.graph:
            self.graph.add_node(n, **kwargs)

        return n

    def add_edge(self, subject_iri: URIRef, object_iri: URIRef, predicate_iri: URIRef) -> Tuple[str, str, str]:
        """
        This method should be used by all derived classes when adding an edge to the networkx.MultiDiGraph.
        This ensures that the subject and object identifiers are CURIEs, and that edge_label is in the correct form.

        Returns the CURIE identifiers used for the subject and object in the
        networkx.MultiDiGraph, and the processed edge_label.

        Parameters
        ----------
        subject_iri: rdflib.URIRef
            Subject IRI for the subject in a triple
        object_iri: rdflib.URIRef
            Object IRI for the object in a triple
        predicate_iri: rdflib.URIRef
            Predicate IRI for the predicate in a triple

        Returns
        -------
        Tuple[str, str, str]
            A 3-nary tuple (of the form subject, object, predicate) that represents the edge

        """
        s = self.add_node(subject_iri)
        o = self.add_node(object_iri)

        relation = make_curie(predicate_iri)
        edge_label = process_iri(predicate_iri)
        if ' ' in edge_label:
            logging.debug("predicate IRI '{}' yields edge_label '{}' that not in snake_case form; replacing ' ' with '_'".format(predicate_iri, edge_label))
        # TODO: shouldn't this move to the utilities function process_uri()
        if edge_label.startswith(BIOLINK):
            logging.debug("predicate IRI '{}' yields edge_label '{}' that starts with '{}'; removing IRI prefix".format(predicate_iri, edge_label,BIOLINK))
            edge_label = edge_label.replace(BIOLINK, '')

        # TODO: is there no way to get label of a CURIE?
        # TODO: this should also move to the utilities function
        # Any service? or preload required ontologies by prefix?
        if ':' in edge_label:
            logging.debug("edge label '{}' is a CURIE; defaulting back to 'related_to'".format(edge_label))
            logging.debug("predicate IRI '{}' yields edge_label '{}' that is actually a CURIE; defaulting back to {}".format(predicate_iri, edge_label, DEFAULT_EDGE_LABEL))
            edge_label = DEFAULT_EDGE_LABEL

        kwargs = {
            'relation': relation,
            'edge_label': edge_label
        }
        if 'provided_by' in self.graph_metadata:
            kwargs['provided_by'] = self.graph_metadata['provided_by']

        if self.graph.has_edge(s, o, key=edge_label):
            logging.debug("{} -- {} --> {} edge already exists".format(s, edge_label, o))
        else:
            self.graph.add_edge(s, o, key=edge_label, **kwargs)

        return s, o, edge_label

    def add_node_attribute(self, iri: URIRef, key: str, value: str) -> None:
        """
        Add an attribute to a node, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The key may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in `rdf_utils.property_mapping`.

        If the node does not exist then it is created using the given iri.

        Parameters
        ----------
        iri: rdflib.URIRef
            The IRI of a node in the rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: str
            The value of the attribute

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

    def add_edge_attribute(self, subject_iri: URIRef, object_iri: URIRef, predicate_iri: URIRef, key: str, value: str) -> None:
        """
        Adds an attribute to an edge, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The key may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in `rdf_utils.property_mapping`.

        If the nodes in the edge does not exist then they will be created
        using subject_iri and object_iri.

        If the edge itself does not exist then it will be created using
        subject_iri, object_iri and predicate_iri.

        Parameters
        ----------
        subject_iri: rdflib.URIRef
            The IRI of the subject node of an edge in rdflib.Graph
        object_iri: rdflib.URIRef
            The IRI of the object node of an edge in rdflib.Graph
        predicate_iri: rdflib.URIRef
            The IRI of the predicate representing an edge in rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: str
            The value of the attribute

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

    def __add_attribute(self, attr_dict: Dict, key: str, value: str) -> None:
        """
        Adds an attribute to the attribute dictionary, respecting whether or not
        that attribute should be multi-valued.
        Multi-valued attributes will not contain duplicates.

        Some attributes are singular form of others. In such cases overflowing values
        will be placed into the correlating multi-valued attribute.
        For example, 'name' attribute will hold only one value while any additional
        value will be stored as 'synonym' attribute.

        Parameters
        ----------
        attr_dict: dict
            Dictionary representing the attribute set of a node or an edge in a networkx graph
        key: str
            The name of the attribute
        value: str
            The value of the attribute

        """
        if key is None or key not in is_property_multivalued:
            logging.warning("Discarding key {} as it is not a valid property.".format(key))
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
    def load_networkx_graph(self, rdfgraph: rdflib.Graph) -> None:
        """
        This method should be overridden and implemented by the derived class,
        and should load all desired nodes and edges from rdflib.Graph into networkx.MultiDiGraph

        Its preferred that this method does not use the networkx API directly
        when adding nodes, edges, and their attributes.

        Instead, Using the following methods,

        - RdfTransformer.add_node()
        - RdfTransformer.add_edge()
        - RdfTransformer.add_node_attribute()
        - RdfTransformer.add_edge_attribute()

        to ensure that nodes, edges, and their attributes
        are added in conformance with the biolink model, and that URIRef's are
        translated into CURIEs or biolink model elements whenever appropriate.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges

        """
        pass

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

    def load_networkx_graph(self, rdfgraph: rdflib.Graph) -> None:
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges

        """
        for rel in OWL_PREDICATES:
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
                    logging.warning("No 'predicate' for OBAN.association {}; defaulting to '{}'".format(association, DEFAULT_EDGE_LABEL))
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
        element = bmt.get_element(key)
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

class HgncRdfTransformer(RdfTransformer):
    """
    Custom transformer for loading: https://data.monarchinitiative.org/ttl/hgnc.ttl
    TODO: merge with base RdfTransformer class
    """
    is_about = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
    has_subsequence = URIRef('http://purl.obolibrary.org/obo/RO_0002524')
    is_subsequence_of = URIRef('http://purl.obolibrary.org/obo/RO_0002525')

    def load_networkx_graph(self, rdfgraph:rdflib.Graph):
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges

        """
        triples = rdfgraph.triples((None, None, None))
        logging.info("Loading from rdflib.Graph to networkx.MultiDiGraph")
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                if p == self.is_about:
                    # if predicate is 'is_about' then treat object as a publication
                    self.add_node_attribute(o, key=s, value='publications')
                elif p == self.is_subsequence_of:
                    # if predicate is 'is_subsequence_of'
                    self.add_edge(s, o, self.is_subsequence_of)
                elif p == self.has_subsequence:
                    # if predicate is 'has_subsequence', interpret the inverse relation 'is_subsequence_of'
                    self.add_edge(o, s, self.is_subsequence_of)
                elif any(p.lower() == x.lower() for x in OWL_PREDICATES):
                    # if predicate is one of the OWL predicates
                    self.add_edge(s, o, p)

class RdfOwlTransformer(RdfTransformer):
    """
    Transformer that parses an OWL ontology in RDF, while retaining class-class relationships.
    """

    def load_networkx_graph(self, rg: rdflib.Graph) -> None:
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges

        """
        triples = rg.triples((None, RDFS.subClassOf, None))
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
                    for x in rg.objects(o, OWL.onProperty):
                        pred = x
                    for x in rg.objects(o, OWL.someValuesFrom):
                        parent = x
                    if pred is None or parent is None:
                        logging.warning("Do not know how to handle BNode: {}".format(o))
                        continue
                else:
                    # C SubClassOf D (C and D are named classes)
                    pred = p
                    parent = o
                self.add_edge(s, parent, pred)

        relations = rg.subjects(RDF.type, OWL.ObjectProperty)
        logging.info("Loading relations")
        with click.progressbar(relations, label='Progress') as bar:
            for relation in bar:
                for _, p, o in rg.triples((relation, None, None)):
                    if o.startswith('http://purl.obolibrary.org/obo/RO_'):
                        self.add_edge(relation, o, p)
                    else:
                        self.add_node_attribute(relation, key=p, value=o)
                self.add_node_attribute(relation, key='category', value='relation')
