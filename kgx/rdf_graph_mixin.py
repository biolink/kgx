import logging
from typing import Set, Dict, Tuple

import networkx as nx
import rdflib
from prefixcommons.curie_util import read_remote_jsonld_context
from rdflib import URIRef, Namespace

from kgx.utils.rdf_utils import property_mapping, process_iri, make_curie, is_property_multivalued

biolink_prefix_map = read_remote_jsonld_context('https://biolink.github.io/biolink-model/context.jsonld')


class RdfGraphMixin(object):
    """
    A mixin that defines the following methods,
        - load_networkx_graph(): template method that all deriving classes should implement
        - add_node(): method to add a node from a RDF form to property graph form
        - add_node_attribute(): method to add a node attribute from a RDF form to property graph form
        - add_edge(): method to add an edge from a RDF form to property graph form
        - add_edge_attribute(): method to add an edge attribute from an RDF form to property graph form

    """

    # TODO: use OBO IRI from biolink model context once https://github.com/biolink/biolink-model/issues/211 is resolved
    OBO = Namespace('http://purl.obolibrary.org/obo/')
    OBAN = Namespace(biolink_prefix_map['OBAN'])
    PMID = Namespace(biolink_prefix_map['PMID'])
    # TODO: double check: is this the correct prefix change? (biolink --> biolinkml)
    BIOLINK = Namespace(biolink_prefix_map['biolinkml'])
    DEFAULT_EDGE_LABEL = 'related_to'

    def __init__(self, source_graph: nx.MultiDiGraph = None):
        if source_graph:
            self.graph = source_graph
        else:
            self.graph = nx.MultiDiGraph()

        self.graph_metadata = {}

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, predicates: Set[URIRef] = None, **kwargs) -> None:
        """
        This method should be overridden and be implemented by the derived class,
        and should load all desired nodes and edges from rdflib.Graph into networkx.MultiDiGraph

        Its preferred that this method does not use the networkx API directly
        when adding nodes, edges, and their attributes.

        Instead, Using the following methods,
            - add_node()
            - add_node_attribute()
            - add_edge()
            - add_edge_attribute()

        to ensure that nodes, edges, and their attributes
        are added in conformance with the biolink model, and that URIRef's are
        translated into CURIEs or biolink model elements whenever appropriate.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: list
            A list of rdflib.URIRef representing predicates to be loaded
        kwargs: dict
            Any additional arguments
        """
        raise NotImplementedError("Method not implemented.")

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
        if edge_label.startswith(self.BIOLINK):
            logging.debug("predicate IRI '{}' yields edge_label '{}' that starts with '{}'; removing IRI prefix".format(predicate_iri, edge_label, self.BIOLINK))
            edge_label = edge_label.replace(self.BIOLINK, '')

        # TODO: is there no way to get label of a CURIE?
        # TODO: this should also move to the utilities function
        # Any service? or preload required ontologies by prefix?
        if ':' in edge_label:
            logging.debug("edge label '{}' is a CURIE; defaulting back to 'related_to'".format(edge_label))
            logging.debug("predicate IRI '{}' yields edge_label '{}' that is actually a CURIE; defaulting back to {}".format(predicate_iri, edge_label, self.DEFAULT_EDGE_LABEL))
            edge_label = self.DEFAULT_EDGE_LABEL

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
            self._add_attribute(attr_dict, key, value)


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
            self._add_attribute(attr_dict, key, value)

    def _add_attribute(self, attr_dict: Dict, key: str, value: str) -> None:
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
                self._add_attribute(attr_dict, 'synonym', value)