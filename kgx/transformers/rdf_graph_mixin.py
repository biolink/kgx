from typing import List, Set, Dict, Tuple, Union, Optional, Any
import rdflib
from biolinkml.meta import SlotDefinition, ClassDefinition, Element
from rdflib import URIRef, Namespace

from kgx.config import get_logger, get_graph_store_class
from kgx.graph.base_graph import BaseGraph
from kgx.graph.nx_graph import NxGraph
from kgx.utils.graph_utils import curie_lookup
from kgx.utils.rdf_utils import property_mapping, is_property_multivalued, reverse_property_mapping, process_predicate
from kgx.utils.kgx_utils import generate_edge_key, get_toolkit, sentencecase_to_camelcase, sentencecase_to_snakecase, \
    get_biolink_ancestors
from kgx.prefix_manager import PrefixManager

log = get_logger()


class RdfGraphMixin(object):
    """
    A mixin that defines the following methods,
        - load_graph(): template method that all deriving classes should implement
        - add_node(): method to add a node from a RDF form to property graph form
        - add_node_attribute(): method to add a node attribute from a RDF form to property graph form
        - add_edge(): method to add an edge from a RDF form to property graph form
        - add_edge_attribute(): method to add an edge attribute from an RDF form to property graph form

    """

    DEFAULT_EDGE_PREDICATE = 'biolink:related_to'
    CORE_NODE_PROPERTIES = {'id'}
    CORE_EDGE_PROPERTIES = {'id', 'subject', 'predicate', 'object', 'type'}

    def __init__(self, source_graph: Optional[BaseGraph] = None, curie_map: Optional[Dict] = None):
        if source_graph:
            self.graph = source_graph
        else:
            self.graph = get_graph_store_class()()

        self.graph_metadata: Dict = {}
        self.prefix_manager = PrefixManager()
        self.DEFAULT = Namespace(self.prefix_manager.prefix_map[''])
        if curie_map:
            self.prefix_manager.update_prefix_map(curie_map)
        # TODO: use OBO IRI from biolink model context once https://github.com/biolink/biolink-model/issues/211 is resolved
        self.OBO = Namespace('http://purl.obolibrary.org/obo/')
        self.OBAN = Namespace(self.prefix_manager.prefix_map['OBAN'])
        self.PMID = Namespace(self.prefix_manager.prefix_map['PMID'])
        self.BIOLINK = Namespace(self.prefix_manager.prefix_map['biolink'])
        self.predicate_mapping = property_mapping.copy()
        self.reverse_predicate_mapping = reverse_property_mapping.copy()
        self.cache: Dict = {}

    def load_graph(self, rdfgraph: rdflib.Graph, predicates: Optional[Set[URIRef]] = None, **kwargs: Dict) -> None:
        """
        This method should be overridden and be implemented by the derived class,
        and should load all desired nodes and edges from rdflib.Graph into
        an instance of BaseGraph.

        Its preferred that this method does not use the BaseGraph API directly
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
        predicates: Optional[Set[URIRef]]
            A set containing predicates in rdflib.URIRef form
        kwargs: Dict
            Any additional arguments

        """
        raise NotImplementedError("Method not implemented.")

    def add_node(self, iri: URIRef, data: Optional[Dict] = None) -> Dict:
        """
        This method should be used by all derived classes when adding a node to
        the kgx.graph.base_graph.BaseGraph. This ensures that a node's identifier is a CURIE,
        and that it's `iri` property is set.

        Returns the CURIE identifier for the node in the kgx.graph.base_graph.BaseGraph

        Parameters
        ----------
        iri: rdflib.URIRef
            IRI of a node
        data: Optional[Dict]
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
            node_data = self.update_node(n, data)
        else:
            if data:
                node_data = data
            else:
                node_data = {}
            node_data['id'] = n

            if 'category' in node_data:
                if 'biolink:NamedThing' not in set(node_data['category']):
                    node_data['category'].append('biolink:NamedThing')
            else:
                node_data['category'] = ["biolink:NamedThing"]

            if 'provided_by' in self.graph_metadata and 'provided_by' not in node_data:
                node_data['provided_by'] = self.graph_metadata['provided_by']
            self.graph.add_node(n, **node_data)
        return node_data

    def update_node(self, n: Union[URIRef, str], data: Optional[Dict] = None) -> Dict:
        """
        Update a node with properties.

        Parameters
        ----------
        n: Union[URIRef, str]
            Node identifier
        data: Optional[Dict]
            Node properties

        Returns
        -------
        Dict
            The node data

        """
        node_data = self.graph.nodes()[str(n)]
        if data:
            new_data = self._prepare_data_dict(node_data, data)
            node_data.update(new_data)
        return node_data

    def add_edge(self, subject_iri: URIRef, object_iri: URIRef, predicate_iri: URIRef, data: Optional[Dict[Any, Any]] = None) -> Dict:
        """
        This method should be used by all derived classes when adding an edge to the kgx.graph.base_graph.BaseGraph.
        This method ensures that the `subject` and `object` identifiers are CURIEs, and that `predicate`
        is in the correct form.

        Parameters
        ----------
        subject_iri: rdflib.URIRef
            Subject IRI for the subject in a triple
        object_iri: rdflib.URIRef
            Object IRI for the object in a triple
        predicate_iri: rdflib.URIRef
            Predicate IRI for the predicate in a triple
        data: Optional[Dict[Any, Any]]
            Additional edge properties

        Returns
        -------
        Dict
            The edge data

        """
        (element_uri, canonical_uri, predicate, property_name) = process_predicate(self.prefix_manager, predicate_iri)
        subject_node = self.add_node(subject_iri)
        object_node = self.add_node(object_iri)
        edge_predicate = element_uri if element_uri else predicate
        if not edge_predicate:
            edge_predicate = property_name

        if ' ' in edge_predicate:
            log.debug(f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that not in snake_case form; replacing ' ' with '_'")
        edge_predicate_prefix = self.prefix_manager.get_prefix(edge_predicate)
        if edge_predicate_prefix not in {'biolink', 'rdf', 'rdfs', 'skos', 'owl'}:
            if PrefixManager.is_curie(edge_predicate):
                # name = curie_lookup(edge_predicate)
                # if name:
                #     log.debug(f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that is actually a CURIE; Using its mapping instead: {name}")
                #     edge_predicate = f"{edge_predicate_prefix}:{name}"
                # else:
                #     log.debug(f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that is actually a CURIE; defaulting back to {self.DEFAULT_EDGE_PREDICATE}")
                edge_predicate = self.DEFAULT_EDGE_PREDICATE

        edge_key = generate_edge_key(subject_node['id'], edge_predicate, object_node['id'])
        if self.graph.has_edge(subject_node['id'], object_node['id'], edge_key=edge_key):
            # edge already exists; process kwargs and update the edge
            edge_data = self.update_edge(subject_node['id'], object_node['id'], edge_key, data)
        else:
            # add a new edge
            edge_data = data if data else {}
            edge_data.update({
                'subject': subject_node['id'],
                'predicate': f"{edge_predicate}",
                'object': object_node['id']
            })
            if 'relation' not in edge_data:
                edge_data['relation'] = predicate

            if 'provided_by' in self.graph_metadata and 'provided_by' not in edge_data:
                edge_data['provided_by'] = self.graph_metadata['provided_by']
            self.graph.add_edge(subject_node['id'], object_node['id'], edge_key=edge_key, **edge_data)

        return edge_data

    def update_edge(self, subject_curie: str, object_curie: str, edge_key: str, data: Optional[Dict[Any, Any]]) -> Dict:
        """
        Update an edge with properties.

        Parameters
        ----------
        subject_curie: str
            Subject CURIE
        object_curie: str
            Object CURIE
        edge_key: str
            Edge key
        data: Optional[Dict[Any, Any]]
            Edge properties

        Returns
        -------
        Dict
            The edge data

        """
        edge_data = self.graph.get_edge(subject_curie, object_curie, edge_key=edge_key)
        if data:
            new_data = self._prepare_data_dict(edge_data, data)
            edge_data.update(new_data)
        return edge_data

    def add_node_attribute(self, iri: Union[URIRef, str], key: str, value: Union[str, List]) -> Dict:
        """
        Add an attribute to a node, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The ``key`` may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in ``rdf_utils.property_mapping``.

        If the node does not exist then it is created.

        Parameters
        ----------
        iri: Union[rdflib.URIRef, str]
            The IRI of a node in the rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: Union[str, List]
            The value of the attribute

        Returns
        -------
        Dict
            The node data

        """
        if self.prefix_manager.is_iri(key):
            key_curie = self.prefix_manager.contract(key)
        else:
            key_curie = key
        c = curie_lookup(key_curie)
        if c:
            key_curie = c

        if self.prefix_manager.is_curie(key_curie):
            # property names will always be just the reference
            mapped_key = self.prefix_manager.get_reference(key_curie)
        else:
            mapped_key = key_curie

        if isinstance(value, rdflib.term.Identifier):
            if isinstance(value, rdflib.term.URIRef):
                value_curie = self.prefix_manager.contract(value)
                # if self.prefix_manager.get_prefix(value_curie) not in {'biolink'} \
                #         and mapped_key not in {'type', 'category', 'predicate', 'relation', 'predicate'}:
                #     d = self.add_node(value)
                #     value = d['id']
                # else:
                #     value = value_curie
                value = value_curie
            else:
                value = value.toPython()
        if mapped_key in is_property_multivalued and is_property_multivalued[mapped_key]:
            value = [value]
        node_data = self.add_node(iri, {mapped_key: value})
        return node_data

    def add_edge_attribute(self, subject_iri: Union[URIRef, str], object_iri: URIRef, predicate_iri: URIRef, key: str, value: str) -> Dict:
        """
        Adds an attribute to an edge, while taking into account whether the attribute
        should be multi-valued.
        Multi-valued properties will not contain duplicates.

        The ``key`` may be a rdflib.URIRef or a URI string that maps onto a property name
        as defined in ``rdf_utils.property_mapping``.

        If the nodes in the edge does not exist then they will be created.

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
        pass

    def _prepare_data_dict(self, d1: Dict, d2: Dict) -> Dict:
        """
        Given two dict objects, make a new dict object that is the intersection of the two.

        If a key is known to be multivalued then it's value is converted to a list.
        If a key is already multivalued then it is updated with new values.
        If a key is single valued, and a new unique value is found then the existing value is
        converted to a list and the new value is appended to this list.

        Parameters
        ----------
        d1: Dict
            Dict object
        d2: Dict
            Dict object

        Returns
        -------
        Dict
            The intersection of d1 and d2

        """
        new_data = {}
        for key, value in d2.items():
            if isinstance(value, (list, set, tuple)):
                new_value = [self.prefix_manager.contract(x) if self.prefix_manager.is_iri(x) else x for x in value]
            else:
                new_value = self.prefix_manager.contract(value) if self.prefix_manager.is_iri(value) else value

            if key in is_property_multivalued:
                if is_property_multivalued[key]:
                    # key is supposed to be multivalued
                    if key in d1:
                        # key is in data
                        if isinstance(d1[key], list):
                            # existing key has value type list
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value if x not in new_data[key]]
                            else:
                                if new_value not in new_data[key]:
                                    new_data[key].append(new_value)
                        else:
                            if key in self.CORE_NODE_PROPERTIES or key in self.CORE_EDGE_PROPERTIES:
                                log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                            else:
                                # existing key does not have value type list; converting to list
                                new_data[key] = [d1[key]]
                                if isinstance(new_value, (list, set, tuple)):
                                    new_data[key] += [x for x in new_value if x not in new_data[key]]
                                else:
                                    if new_value not in new_data[key]:
                                        new_data[key].append(new_value)
                    else:
                        # key is not in data; adding
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] = [x for x in new_value]
                        else:
                            new_data[key] = [new_value]
                else:
                    # key is not multivalued; adding/replacing as-is
                    if key in d1:
                        if isinstance(d1[key], list):
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value]
                            else:
                                new_data[key].append(new_value)
                        else:
                            if key in self.CORE_NODE_PROPERTIES or key in self.CORE_EDGE_PROPERTIES:
                                log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                            else:
                                new_data[key] = new_value
                    else:
                        new_data[key] = new_value
            else:
                # treating key as multivalued
                if key in d1:
                    # key is in data
                    if key in self.CORE_NODE_PROPERTIES or key in self.CORE_EDGE_PROPERTIES:
                        log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                    else:
                        if isinstance(d1[key], list):
                            # existing key has value type list
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value if x not in new_data[key]]
                            else:
                                new_data[key].append(new_value)
                        else:
                            # existing key does not have value type list; converting to list
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value if x not in new_data[key]]
                            else:
                                new_data[key].append(new_value)
                else:
                    new_data[key] = new_value
        return new_data

    def get_biolink_element(self, predicate: Any) -> Optional[Element]:
        """
        Returns a Biolink Model element for a given predicate.

        Parameters
        ----------
        predicate: Any
            The CURIE of a predicate

        Returns
        -------
        Optional[Element]
            The corresponding Biolink Model element

        """
        toolkit = get_toolkit()
        if self.prefix_manager.is_iri(predicate):
            predicate_curie = self.prefix_manager.contract(predicate)
        else:
            predicate_curie = predicate
        if self.prefix_manager.is_curie(predicate_curie):
            reference = self.prefix_manager.get_reference(predicate_curie)
        else:
            reference = predicate_curie
        element = toolkit.get_element(reference)
        if not element:
            try:
                mapping = toolkit.get_element_by_mapping(predicate)
                if mapping:
                    element = toolkit.get_element(mapping)
            except ValueError as e:
                log.error(e)
        return element
