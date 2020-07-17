import logging
import networkx as nx
from typing import List, Set, Dict, Tuple, Union
import rdflib
from rdflib import URIRef, Namespace

from kgx.utils.graph_utils import curie_lookup
from kgx.utils.rdf_utils import property_mapping, process_iri, is_property_multivalued
from kgx.utils.kgx_utils import generate_edge_key
from kgx.prefix_manager import PrefixManager

class RdfGraphMixin(object):
    """
    A mixin that defines the following methods,
        - load_networkx_graph(): template method that all deriving classes should implement
        - add_node(): method to add a node from a RDF form to property graph form
        - add_node_attribute(): method to add a node attribute from a RDF form to property graph form
        - add_edge(): method to add an edge from a RDF form to property graph form
        - add_edge_attribute(): method to add an edge attribute from an RDF form to property graph form

    """

    DEFAULT_EDGE_LABEL = 'related_to'

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        if source_graph:
            self.graph = source_graph
        else:
            self.graph = nx.MultiDiGraph()

        self.graph_metadata = {}
        self.prefix_manager = PrefixManager()
        self.DEFAULT = Namespace(self.prefix_manager.prefix_map[':'])
        if curie_map:
            self.prefix_manager.update_prefix_map(curie_map)
        # TODO: use OBO IRI from biolink model context once https://github.com/biolink/biolink-model/issues/211 is resolved
        self.OBO = Namespace('http://purl.obolibrary.org/obo/')
        self.OBAN = Namespace(self.prefix_manager.prefix_map['OBAN'])
        self.PMID = Namespace(self.prefix_manager.prefix_map['PMID'])
        self.BIOLINK = Namespace(self.prefix_manager.prefix_map['biolink'])

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, predicates: Set[URIRef] = None, **kwargs) -> None:
        """
        This method should be overridden and be implemented by the derived class,
        and should load all desired nodes and edges from rdflib.Graph into networkx.MultiDiGraph

        Its preferred that this method does not use the networkx API directly
        when adding nodes, edges, and their attributes.

        Instead, Using the following methods,
            - ``add_node()``
            - ``add_node_attribute()``
            - ``add_edge()``
            - ``add_edge_attribute()``

        to ensure that nodes, edges, and their attributes
        are added in conformance with the BioLink Model, and that URIRef's are
        translated into CURIEs or BioLink Model elements whenever appropriate.

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

    def add_node(self, iri: URIRef, **kwargs: Dict) -> Dict:
        """
        This method should be used by all derived classes when adding a node to
        the networkx.MultiDiGraph. This ensures that a node's identifier is a CURIE,
        and that it's `iri` property is set.

        Returns the CURIE identifier for the node in the networkx.MultiDiGraph

        Parameters
        ----------
        iri: rdflib.URIRef
            IRI of a node
        kwargs: dict
            Additional node properties

        Returns
        -------
        Dict
            The node data

        """
        n = self.prefix_manager.contract(str(iri))
        if n == iri:
            if self.prefix_manager.has_urlfragment(iri):
                n = rdflib.namespace.urldefrag(iri).fragment

        if not n:
            n = iri
        if self.graph.has_node(n):
            node_data = self.graph.nodes[n]
            if kwargs:
                node_data.update(kwargs)
        else:
            print(f"Adding node {n}")
            node_data = kwargs
            node_data['id'] = n
            if 'category' not in node_data:
                node_data['category'] = ["biolink:NamedThing"]
            if 'provided_by' in self.graph_metadata and 'provided_by' not in node_data:
                node_data['provided_by'] = self.graph_metadata['provided_by']
            self.graph.add_node(n, **node_data)
        return node_data

    def add_edge(self, subject_iri: URIRef, object_iri: URIRef, predicate_iri: URIRef, **kwargs) -> Dict:
        """
        This method should be used by all derived classes when adding an edge to the networkx.MultiDiGraph.
        This ensures that the `subject` and `object` identifiers are CURIEs, and that `edge_label` is in the correct form.

        Returns the CURIE identifiers used for the `subject` and `object` in the
        networkx.MultiDiGraph, and the processed `edge_label`.

        Parameters
        ----------
        subject_iri: rdflib.URIRef
            Subject IRI for the subject in a triple
        object_iri: rdflib.URIRef
            Object IRI for the object in a triple
        predicate_iri: rdflib.URIRef
            Predicate IRI for the predicate in a triple
        kwargs:
            Additional edge properties

        Returns
        -------
        Dict
            The edge data

        """
        subject_node = self.add_node(subject_iri)
        object_node = self.add_node(object_iri)
        relation = self.prefix_manager.contract(predicate_iri)
        edge_label = process_iri(predicate_iri)
        if ' ' in edge_label:
            logging.debug("predicate IRI '{}' yields edge_label '{}' that not in snake_case form; replacing ' ' with '_'".format(predicate_iri, edge_label))
        if edge_label.startswith(self.BIOLINK):
            logging.debug("predicate IRI '{}' yields edge_label '{}' that starts with '{}'; removing IRI prefix".format(predicate_iri, edge_label, self.BIOLINK))
            edge_label = edge_label.replace(self.BIOLINK, '')

        if PrefixManager.is_curie(edge_label):
            name = curie_lookup(edge_label)
            if name:
                logging.debug("predicate IRI '{}' yields edge_label '{}' that is actually a CURIE; Using its mapping instead: {}".format(predicate_iri, edge_label, name))
                edge_label = name
            else:
                logging.debug("predicate IRI '{}' yields edge_label '{}' that is actually a CURIE; defaulting back to {}".format(predicate_iri, edge_label, self.DEFAULT_EDGE_LABEL))
                edge_label = self.DEFAULT_EDGE_LABEL

        kwargs.update({
            'subject': subject_node['id'],
            'predicate': str(predicate_iri),
            'object': object_node['id'],
            'relation': relation,
            'edge_label': f"biolink:{edge_label}"
        })

        if 'provided_by' in self.graph_metadata:
            kwargs['provided_by'] = self.graph_metadata['provided_by']

        edge_key = generate_edge_key(subject_node['id'], edge_label, object_node['id'])
        kwargs['edge_key'] = edge_key
        if not self.graph.has_edge(subject_node['id'], object_node['id'], key=edge_key):
            self.graph.add_edge(subject_node['id'], object_node['id'], key=edge_key, **kwargs)
        # TODO: support append
        return kwargs

    def add_node_attribute(self, iri: Union[URIRef, str], key: str, value: str) -> Dict:
        """
        Add an attribute to a node, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The ``key`` may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in ``rdf_utils.property_mapping``.

        If the node does not exist then it is created using the given ``iri``.

        Parameters
        ----------
        iri: Union[rdflib.URIRef, str]
            The IRI of a node in the rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: str
            The value of the attribute

        Returns
        -------
        Dict
            The node data

        """
        if not isinstance(key, URIRef):
            key = URIRef(key)
        mapped_key = property_mapping.get(key)
        if not mapped_key:
            mapped_key = self.prefix_manager.contract(key)
            if self.prefix_manager.is_curie(mapped_key):
                mapped_key = self.prefix_manager.get_reference(mapped_key)
        if not mapped_key:
            logging.debug(f"{key} could not be mapped; using {key}")
            mapped_key = key
        print(f"{key} mapped to {mapped_key}")
        node_data = self.add_node(iri)
        node_data = self._add_node_attribute(node_data, mapped_key, str(value))
        return node_data

    def _add_node_attribute(self, node: Dict, key: str, value: str) -> Dict:
        """
        Adds an attribute to a node.

        Parameters
        ----------
        node: Dict
            Node data
        key: str
            The key of an attribute
        value: str
            The value of the attribute

        Returns
        -------
        Dict
            The node data

        """
        if PrefixManager.is_iri(value):
            value = process_iri(value)
        if key in is_property_multivalued and is_property_multivalued[key]:
            if key in node:
                node[key].append(value)
            else:
                node[key] = [value]
        else:
            if key == 'name' and 'name' in node:
                if 'synonym' in node:
                    node['synonym'].append(value)
                else:
                    node['synonym'] = [value]
            else:
                node[key] = value
        return node

    def add_edge_attribute(self, subject_iri: Union[URIRef, str], object_iri: URIRef, predicate_iri: URIRef, key: str, value: str) -> Dict:
        """
        Adds an attribute to an edge, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The ``key`` may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in ``rdf_utils.property_mapping``.

        If the nodes in the edge does not exist then they will be created
        using ``subject_iri`` and ``object_iri``.

        If the edge itself does not exist then it will be created using
        ``subject_iri``, ``object_iri`` and ``predicate_iri``.

        Parameters
        ----------
        subject_iri: [rdflib.URIRef, str]
            The IRI of the subject node of an edge in rdflib.Graph
        object_iri: rdflib.URIRef
            The IRI of the object node of an edge in rdflib.Graph
        predicate_iri: rdflib.URIRef
            The IRI of the predicate representing an edge in rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: str
            The value of the attribute

        Returns
        -------
        Dict
            The edge data

        """
        edge_data = None
        if key.lower() in is_property_multivalued:
            key = key.lower()
        else:
            if not isinstance(key, URIRef):
                key = URIRef(key)
            key = property_mapping.get(key)

        if key is not None and value is not None:
            subject_curie = self.prefix_manager.contract(subject_iri)
            object_curie = self.prefix_manager.contract(object_iri)
            edge_label = process_iri(predicate_iri)
            if PrefixManager.is_curie(edge_label):
                edge_label = curie_lookup(edge_label)
            edge_key = generate_edge_key(subject_curie, edge_label, object_curie)
            if self.graph.has_edge(subject_curie, object_curie, edge_key):
                edge_data = self.graph.get_edge_data(subject_iri, object_iri, edge_key)
            else:
                edge_data = self.add_edge(subject_iri, object_iri, predicate_iri)
            edge_data = self._add_edge_attribute(edge_data, key, value)
        return edge_data


    def _add_edge_attribute(self, edge_data, key: str, value: str):
        """
        Adds an attribute to an edge.

        Parameters
        ----------
        subject_iri: Union[URIRef, str]
            Subject of an edge
        object_iri: Union[URIRef, str]
            Object of an edge
        predicate_iri: Union[URIRef, str]
            Predicate of an edge
        key: str
            The key of an attribute
        value: str
            The value of the attribute

        Returns
        -------
        dict
            The edge data

        """
        if PrefixManager.is_iri(value):
            value = process_iri(value)
        if key in is_property_multivalued and is_property_multivalued[key]:
            if key in edge_data:
                edge_data[key].append(value)
            else:
                edge_data[key] = [value]
        else:
            edge_data[key] = value
        return edge_data

