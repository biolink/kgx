import itertools

import click, rdflib, os, uuid
import networkx as nx
from typing import Tuple, Union, Set, List, Dict, Any, Iterator, Optional
from rdflib import Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL

from kgx.config import get_logger
from kgx.prefix_manager import PrefixManager
from kgx.transformers.transformer import Transformer
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin
from kgx.utils.rdf_utils import property_mapping, reverse_property_mapping, generate_uuid
from kgx.utils.kgx_utils import get_toolkit, get_biolink_node_properties, get_biolink_edge_properties, \
    current_time_in_millis, get_biolink_association_types, get_biolink_property_types, apply_filters


log = get_logger()


class RdfTransformer(RdfGraphMixin, Transformer):
    """
    Transformer that parses RDF and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This is the base class which is used to implement other RDF-based transformers.

    Parameters
    ----------
    source_graph: Optional[networkx.MultiDiGraph]
        The source graph
    curie_map: Optional[Dict]
        A curie map that maps non-canonical CURIEs to IRIs

    """

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Optional[Dict] = None):
        super().__init__(source_graph, curie_map)
        self.toolkit = get_toolkit()
        self.node_properties = set([URIRef(self.prefix_manager.expand(x)) for x in get_biolink_node_properties()])
        self.node_properties.update(get_biolink_node_properties())
        self.node_properties.update(get_biolink_edge_properties())
        self.node_properties.add(URIRef(self.prefix_manager.expand('biolink:provided_by')))
        self.reification_types = {RDF.Statement, self.BIOLINK.Association, self.OBAN.association}
        self.reification_predicates = {
            self.BIOLINK.subject, self.BIOLINK.predicate, self.BIOLINK.object,
            RDF.subject, RDF.object, RDF.predicate,
            self.OBAN.association_has_subject, self.OBAN.association_has_predicate, self.OBAN.association_has_object
        }
        self.reified_nodes: Set = set()
        self.start: int = 0
        self.count: int = 0
        self.property_types: Dict = get_biolink_property_types()
        self.node_filters: Dict[str, Union[str, Set]] = {}
        self.edge_filters: Dict[str, Union[str, Set]] = {}

    def set_predicate_mapping(self, m: Dict) -> None:
        """
        Set predicate mappings.

        Use this method to update predicate mappings for predicates that are
        not in Biolink Model.

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are IRIs and values are their corresponding property names

        """
        for k, v in m.items():
            self.predicate_mapping[URIRef(k)] = v
            self.reverse_predicate_mapping[v] = URIRef(k)

    def set_property_types(self, m: Dict) -> None:
        """
        Set property types.

        Use this method to populate type information for properties that are
        not in Biolink Model.

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are property URI and values are the type

        """
        for k, v in m.items():
            (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(k)
            if element_uri:
                key = element_uri
            elif predicate:
                key = predicate
            else:
                key = property_name

            self.property_types[key] = v

    def parse(self, filename: str, input_format: Optional[str] = None, compression: Optional[str] = None, provided_by: Optional[str] = None, node_property_predicates: Optional[Set[str]] = None) -> None:
        """
        Parse a file, containing triples, into a rdflib.Graph

        The file can be either a 'turtle' file or any other format supported by rdflib.

        Parameters
        ----------
        filename : Optional[str]
            File to read from.
        input_format : Optional[str]
            The input file format.
            If ``None`` is provided then the format is guessed using ``rdflib.util.guess_format()``
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by : Optional[str]
            Define the source providing the input file.
        node_property_predicates: Optional[Set[str]]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties

        """
        rdfgraph = rdflib.Graph()
        if node_property_predicates:
            self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in node_property_predicates])

        if compression:
            log.warning(f"compression mode '{compression}' not supported by RdfTransformer")
        if input_format is None:
            input_format = rdflib.util.guess_format(filename)

        log.info("Parsing {} with '{}' format".format(filename, input_format))
        rdfgraph.parse(filename, format=input_format)
        log.info("{} parsed with {} triples".format(filename, len(rdfgraph)))

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]

        self.start = current_time_in_millis()
        self.load_networkx_graph(rdfgraph)
        log.info(f"Done parsing {filename}")
        apply_filters(self.graph, self.node_filters, self.edge_filters)
        self.report()

    def load_networkx_graph(self, rdfgraph: rdflib.Graph, predicates: Optional[Set[URIRef]] = None, **kwargs: Dict) -> None:
        """
        Walk through the rdflib.Graph and load all required triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: Optional[Set[URIRef]]
            A set containing predicates in rdflib.URIRef form
        kwargs: Dict
            Any additional arguments

        """
        self.reified_nodes.clear()
        for s, p, o in rdfgraph.triples((None, None, None)):
            self.triple(s, p, o)
        self.dereify(self.reified_nodes)

    def triple(self, s: URIRef, p: URIRef, o: URIRef) -> None:
        """
        Parse a triple.

        Parameters
        ----------
        s: URIRef
            Subject
        p: URIRef
            Predicate
        o: URIRef
            Object

        """
        self.count += 1
        (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(p)
        if element_uri:
            prop_uri = element_uri
        elif predicate:
            prop_uri = predicate
        else:
            prop_uri = property_name

        if s in self.reified_nodes:
            # subject is a reified node
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif p in self.reification_predicates:
            # subject is a reified node
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif property_name in {'subject', 'edge_label', 'object', 'predicate', 'relation'}:
            # subject is a reified node
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif o in self.reification_types:
            # subject is a reified node
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif element_uri and element_uri in self.node_properties:
            # treating predicate as a node property
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif p in self.node_properties \
                or predicate in self.node_properties \
                or property_name in self.node_properties:
            # treating predicate as a node property
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif isinstance(o, rdflib.term.Literal):
            self.add_node_attribute(s, key=prop_uri, value=o)
        else:
            # treating predicate as an edge
            self.add_edge(s, o, p)

        if self.count % 1000 == 0:
            log.info(f"Parsed {self.count} triples; time taken: {current_time_in_millis() - self.start} ms")
            self.start = current_time_in_millis()

    def dereify(self, nodes: Set[str]) -> None:
        """
        Dereify a set of nodes where each node has all the properties
        necessary to create an edge.

        Parameters
        ----------
        nodes: Set[str]
            A set of nodes

        """
        log.info(f"Dereifying {len(nodes)} nodes")
        while nodes:
            n = nodes.pop()
            n_curie = self.prefix_manager.contract(str(n))
            node = self.graph.nodes[n_curie]
            if 'edge_label' not in node:
                node['edge_label'] = "biolink:related_to"
            if 'relation' not in node:
                node['relation'] = node['edge_label']
            if 'category' in node:
                del node['category']
            self.add_edge(node['subject'], node['object'], node['edge_label'], node)
            self.graph.remove_node(n_curie)

    def reify(self, u: str, v: str, k: str, data: Dict) -> Dict:
        """
        Create a node representation of an edge.

        Parameters
        ----------
        u: str
            Subject
        v: str
            Object
        k: str
            Edge key
        data: Dict
            Edge data

        Returns
        -------
        Dict
            The reified node

        """
        s = self.uriref(u)
        p = self.uriref(data['edge_label'])
        o = self.uriref(v)

        if 'id' in data:
            node_id = self.uriref(data['id'])
        else:
            # generate a UUID for the reified node
            node_id = self.uriref(generate_uuid())
        reified_node = data.copy()
        if 'category' in reified_node:
            del reified_node['category']
        reified_node['id'] = node_id
        reified_node['type'] = 'biolink:Association'
        reified_node['subject'] = s
        reified_node['edge_label'] = p
        reified_node['object'] = o
        return reified_node

    def save(self, filename: str, output_format: str = "turtle", compression: Optional[str] = None, reify_all_edges: bool = False, **kwargs) -> None:
        """
        Transform networkx.MultiDiGraph into rdflib.Graph and export
        this graph as a file (``turtle``, by default).

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format; default: ``turtle``
        compression: Optional[str]
            The compression type. Not yet supported.
        reify_all_edges: bool
            Whether to reify all edges in the graph
        kwargs: Dict
            Any additional arguments

        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()
        rdfgraph.bind('', str(self.DEFAULT))
        rdfgraph.bind('OBO', str(self.OBO))
        rdfgraph.bind('biolink', str(self.BIOLINK))

        nodes_generator = self.export_nodes()
        edges_generator = self.export_edges(reify_all_edges)
        generator = itertools.chain(nodes_generator, edges_generator)
        for t in generator:
            rdfgraph.add(t)
        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)

    def export_nodes(self) -> Iterator:
        """
        Export nodes and its attributes as triples.
        This methods yields a 3-tuple of (subject, predicate, object).

        Returns
        -------
        Iterator
            An iterator

        """
        for n, data in self.graph.nodes(data=True):
            s = self.uriref(n)
            for k, v in data.items():
                if k in {'id', 'iri'}:
                    continue
                (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(k)
                if element_uri is None:
                    # not a biolink predicate
                    if k in self.reverse_predicate_mapping:
                        prop_uri = self.reverse_predicate_mapping[k]
                        #prop_uri = self.prefix_manager.contract(prop_uri)
                    else:
                        prop_uri = k
                else:
                    prop_uri = canonical_uri if canonical_uri else element_uri
                prop_type = self._get_property_type(prop_uri)
                prop_uri = self.uriref(prop_uri)
                if isinstance(v, (list, set, tuple)):
                    for x in v:
                        value_uri = self._prepare_object(k, prop_type, x)
                        yield (s, prop_uri, value_uri)
                else:
                    value_uri = self._prepare_object(k, prop_type, v)
                    yield (s, prop_uri, value_uri)

    def export_edges(self, reify_all_edges: bool = False) -> Iterator:
        """
        Export edges and its attributes as triples.
        This methods yields a 3-tuple of (subject, predicate, object).

        Parameters
        ----------
        reify_all_edges: bool
            Whether to reify all edges in the graph

        Returns
        -------
        Iterator
            An iterator

        """
        ecache = []
        associations = set([self.prefix_manager.contract(x) for x in self.reification_types])
        associations.update([str(x) for x in get_biolink_association_types()])
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            if reify_all_edges:
                reified_node = self.reify(u, v, k, data)
                s = reified_node['subject']
                p = reified_node['edge_label']
                o = reified_node['object']
                ecache.append((s, p, o))
                n = reified_node['id']
                for prop, value in reified_node.items():
                    if prop in {'id', 'association_id', 'edge_key'}:
                        continue
                    (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(prop)
                    if element_uri:
                        prop_uri = canonical_uri if canonical_uri else element_uri
                    else:
                        if prop in self.reverse_predicate_mapping:
                            prop_uri = self.reverse_predicate_mapping[prop]
                            #prop_uri = self.prefix_manager.contract(prop_uri)
                        else:
                            prop_uri = predicate
                    prop_type = self._get_property_type(prop_uri)
                    prop_uri = self.uriref(prop_uri)
                    if isinstance(value, list):
                        for x in value:
                            value_uri = self._prepare_object(prop, prop_type, x)
                            yield (n, prop_uri, value_uri)
                    else:
                        value_uri = self._prepare_object(prop, prop_type, value)
                        yield (n, prop_uri, value_uri)
            else:
                if 'type' in data and data['type'] in associations:
                    reified_node = self.reify(u, v, k, data)
                    s = reified_node['subject']
                    p = reified_node['edge_label']
                    o = reified_node['object']
                    ecache.append((s, p, o))
                    n = reified_node['id']
                    for prop, value in reified_node.items():
                        if prop in {'id', 'association_id', 'edge_key'}:
                            continue
                        (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(prop)
                        if element_uri:
                            prop_uri = canonical_uri if canonical_uri else element_uri
                        else:
                            if prop in self.reverse_predicate_mapping:
                                prop_uri = self.reverse_predicate_mapping[prop]
                                #prop_uri = self.prefix_manager.contract(prop_uri)
                            else:
                                prop_uri = predicate
                        prop_type = self._get_property_type(prop_uri)
                        prop_uri = self.uriref(prop_uri)
                        if isinstance(value, list):
                            for x in value:
                                value_uri = self._prepare_object(prop, prop_type, x)
                                yield (n, prop_uri, value_uri)
                        else:
                            value_uri = self._prepare_object(prop, prop_type, value)
                            yield (n, prop_uri, value_uri)
                else:
                    s = self.uriref(u)
                    p = self.uriref(data['edge_label'])
                    o = self.uriref(v)
                    yield (s, p, o)
        for t in ecache:
            yield (t[0], t[1], t[2])

    def _prepare_object(self, prop: str, prop_type: str, value: Any) -> rdflib.term.Identifier:
        """
        Prepare the object of a triple.

        Parameters
        ----------
        prop: str
            property name
        prop_type: str
            property type
        value: Any
            property value

        Returns
        -------
        rdflib.term.Identifier
            An instance of rdflib.term.Identifier

        """
        if prop_type == 'uriorcurie' or prop_type == 'xsd:anyURI':
            if isinstance(value, str) and PrefixManager.is_curie(value):
                o = self.uriref(value)
            elif isinstance(value, str) and PrefixManager.is_iri(value):
                o = URIRef(value)
            else:
                o = Literal(value)
        elif prop_type.startswith('xsd'):
            o = Literal(value, datatype=self.prefix_manager.expand(prop_type))
        else:
            o = Literal(value, datatype=self.prefix_manager.expand("xsd:string"))
        return o

    def _get_property_type(self, p: str) -> str:
        """
        Get type for a given property name.

        Parameters
        ----------
        p: str
            property name

        Returns
        -------
        str
            The type for property name

        """
        # TODO: this should be properly defined in the model
        default_uri_types = {
            'biolink:type', 'biolink:category', 'biolink:subject',
            'biolink:object', 'biolink:relation', 'biolink:edge_label',
            'rdf:type', 'rdf:subject', 'rdf:predicate', 'rdf:object'
        }

        if p in default_uri_types:
            t = 'uriorcurie'
        else:
            if p in self.property_types:
                t = self.property_types[p]
            elif f'biolink:{p}' in self.property_types:
                t = self.property_types[f'biolink:{p}']
            else:
                t = 'xsd:string'
            # if value:
            #     if isinstance(value, (list, set, tuple)):
            #         x = value[0]
            #         if self.graph.has_node(x):
            #             t = 'uriorcurie'
            #         else:
            #             t = 'xsd:string'
            #     else:
            #         if self.graph.has_node(value):
            #             t = 'uriorcurie'
            #         else:
            #             t = 'xsd:string'
        return t

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
            URIRef form of the input ``identifier``

        """
        if identifier.startswith('urn:uuid:'):
            uri = identifier
        elif identifier in reverse_property_mapping:
            # identifier is a property
            uri = reverse_property_mapping[identifier]
        else:
            # identifier is an entity
            if identifier.startswith(':'):
                # TODO: this should be handled upstream by prefixcommons-py
                uri = self.DEFAULT.term(identifier.replace(':', '', 1))
            else:
                uri = self.prefix_manager.expand(identifier)
            # if identifier == uri:
            #     if PrefixManager.is_curie(identifier):
            #         identifier = identifier.replace(':', '_')
            #     if ' ' in identifier:
            #         identifier = identifier.replace(' ', '_')
            #     uri = self.DEFAULT.term(identifier)

        return URIRef(uri)


class ObanRdfTransformer(RdfTransformer):
    """
    Transformer that parses a 'turtle' file and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This Transformer supports OBAN style of modeling where,
    - it dereifies OBAN.association triples into a property graph form
    - it reifies property graph into OBAN.association triples

    Parameters
    ----------
    source_graph: Optional[networkx.MultiDiGraph]
        The source graph
    curie_map: Optional[Dict]
        A curie map that maps non-canonical CURIEs to IRIs

    """

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        super().__init__(source_graph, curie_map)
        self.reification_types.update({self.OBAN.association})
        self.reification_predicates.update({
            self.OBAN.association_has_subject, self.OBAN.association_has_predicate, self.OBAN.association_has_object
        })


class RdfOwlTransformer(RdfTransformer):
    """
    Transformer that parses an OWL ontology.

    .. note::
        This is a simple parser that loads direct class-class relationships.

    Parameters
    ----------
    source_graph: Optional[networkx.MultiDiGraph]
        The source graph
    curie_map: Optional[Dict]
        A curie map that maps non-canonical CURIEs to IRIs

    """

    def __init__(self, source_graph: Optional[nx.MultiDiGraph] = None, curie_map: Optional[Dict] = None):
        self.imported: Set = set()
        super().__init__(source_graph, curie_map)

    def parse(self, filename: str, input_format: Optional[str] = None, compression: Optional[str] = None, provided_by: Optional[str]  = None, node_property_predicates: Optional[Set[str]] = None) -> None:
        """
        Parse an OWL, and load into a rdflib.Graph

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : Optional[str]
            The input file format.
            If ``None`` is provided then the format is guessed using ``rdflib.util.guess_format()``
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by : Optional[str]
            Define the source providing the input file.
        node_property_predicates: Optional[Set[str]]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties

        """
        rdfgraph = rdflib.Graph()
        if node_property_predicates:
            self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in node_property_predicates])

        if compression:
            log.warning(f"compression mode '{compression}' not supported by RdfTransformer")
        if input_format is None:
            input_format = rdflib.util.guess_format(filename)

        log.info("Parsing {} with '{}' format".format(filename, input_format))
        rdfgraph.parse(filename, format=input_format)
        log.info("{} parsed with {} triples".format(filename, len(rdfgraph)))

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]

        self.start = current_time_in_millis()
        log.info(f"Done parsing {filename}")
        self.report()
        triples = rdfgraph.triples((None, OWL.imports, None))
        for s, p, o in triples:
            # Load all imports first
            if p == OWL.imports:
                if o not in self.imported:
                    input_format = rdflib.util.guess_format(o)
                    imported_rdfgraph = rdflib.Graph()
                    log.info(f"Parsing OWL import: {o}")
                    self.imported.add(o)
                    imported_rdfgraph.parse(o, format=input_format)
                    self.load_networkx_graph(imported_rdfgraph)
                else:
                    log.warning(f"Trying to import {o} but its already done")
        self.load_networkx_graph(rdfgraph)

    def load_networkx_graph(self, rdfgraph: rdflib.Graph, predicates: Optional[Set[URIRef]] = None, **kwargs: Dict) -> None:
        """
        Walk through the rdflib.Graph and load all triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        predicates: Optional[Set[URIRef]]
            A list of rdflib.URIRef representing predicates to be loaded
        kwargs: Dict
            Any additional arguments

        """
        seen = set()
        seen.add(RDFS.subClassOf)
        for s, p, o in rdfgraph.triples((None, RDFS.subClassOf, None)):
            # ignoring blank nodes
            if isinstance(s, rdflib.term.BNode):
                continue
            pred = None
            parent = None
            if isinstance(o, rdflib.term.BNode):
                # C SubClassOf R some D
                for x in rdfgraph.objects(o, OWL.onProperty):
                    pred = x
                for x in rdfgraph.objects(o, OWL.someValuesFrom):
                    parent = x
                if pred is None or parent is None:
                    log.warning(f"{s} {p} {o} has OWL.onProperty {pred} and OWL.someValuesFrom {parent}")
                    log.warning("Do not know how to handle BNode: {}".format(o))
                    continue
            else:
                # C SubClassOf D (C and D are named classes)
                pred = p
                parent = o
            self.triple(s, pred, parent)

        seen.add(OWL.equivalentClass)
        for s, p, o in rdfgraph.triples((None, OWL.equivalentClass, None)):
            if not isinstance(o, rdflib.term.BNode):
                self.triple(s, p, o)

        for relation in rdfgraph.subjects(RDF.type, OWL.ObjectProperty):
            seen.add(relation)
            for s, p, o in rdfgraph.triples((relation, None, None)):
                if o.startswith('http://purl.obolibrary.org/obo/RO_'):
                    self.triple(s, p, o)
                elif not isinstance(o, rdflib.term.BNode):
                    self.triple(s, p, o)

        for s, p, o in rdfgraph.triples((None, None, None)):
            if isinstance(s, rdflib.term.BNode) or isinstance(o, rdflib.term.BNode):
                continue
            if p in seen:
                continue
            self.triple(s, p, o)
