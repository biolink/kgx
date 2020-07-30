import click, rdflib, os, uuid
import networkx as nx
import logging
from typing import Tuple, Union, Set, List, Dict

from biolinkml.meta import Element
from rdflib import Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict
from prefixcommons.curie_util import read_remote_jsonld_context

from kgx.prefix_manager import PrefixManager
from kgx.transformers.transformer import Transformer
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin
from kgx.utils.rdf_utils import property_mapping, infer_category, reverse_property_mapping, generate_uuid
from kgx.utils.kgx_utils import get_toolkit, get_biolink_node_properties, get_biolink_edge_properties, \
    current_time_in_millis, get_biolink_association_types, get_biolink_property_types


class RdfTransformer(RdfGraphMixin, Transformer):
    """
    Transformer that parses RDF and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This is the base class which is used to implement other RDF-based transformers.
    """
    RDF_PREDICATES = {RDF.type}
    OWL_PREDICATES = {RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass}
    BASIC_PREDICATES = {'subclass_of', 'part_of', 'has_part', 'same_as', 'equivalent_class', 'type'}
    BASIC_BIOLINK_PREDICATES = set([f"biolink:{x}" for x in BASIC_PREDICATES])

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        super().__init__(source_graph, curie_map)
        self.toolkit = get_toolkit()
        self.node_properties = set([URIRef(self.prefix_manager.expand(x)) for x in get_biolink_node_properties()])
        additional_predicates = ['biolink:same_as', 'OBAN:association_has_object', 'OBAN:association_has_subject',
             'OBAN:association_has_predicate', 'OBAN:association_has_object']
        self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in additional_predicates])
        self.node_properties.add(URIRef(self.prefix_manager.expand('biolink:provided_by')))
        self.reified_nodes = set()
        self.start = 0
        self.count = 0
        self.cache = {}
        self.property_types = get_biolink_property_types()

    def set_property_types(self, m):
        for k, v in m.items():
            self.property_types[k] = v

    def parse(self, filename: str = None, input_format: str = None, provided_by: str = None, node_property_predicates: Set[str] = None) -> None:
        """
        Parse a file, containing triples, into a rdflib.Graph

        The file can be either a 'turtle' file or any other format supported by rdflib.

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : str
            The input file format.
            If ``None`` is provided then the format is guessed using ``rdflib.util.guess_format()``
        provided_by : str
            Define the source providing the input file.
        node_property_predicates: Set[str]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties

        """
        rdfgraph = rdflib.Graph()
        if node_property_predicates:
            self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in node_property_predicates])

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
        self.report()

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, **kwargs) -> None:
        """
        Walk through the rdflib.Graph and load all required triples into networkx.MultiDiGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        node_property_predicates: Set[str]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties
        kwargs: Dict
            Any additional arguments

        """
        triples = rdfgraph.triples((None, None, None))
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                self.triple(s, p, o)
                self.count += 1
                if self.count % 1000 == 0:
                    logging.info(f"Parsed {self.count} triples; time taken: {current_time_in_millis() - self.start} ms")
                    self.start = current_time_in_millis()
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
        if p in self.cache:
            # already processed this predicate before; pull from cache
            element = self.cache[p]['element']
            predicate = self.cache[p]['predicate']
            property_name = self.cache[p]['property_name']
        else:
            # haven't seen this predicate before; map to element
            predicate = self.prefix_manager.contract(str(p))
            print(f"Mapping {p} to {predicate}")
            property_name = self.prefix_manager.get_reference(predicate)
            element = self.get_biolink_element(predicate, property_name)
            self.cache[p] = {'element': element, 'predicate': predicate, 'property_name': property_name}

        if s in self.reified_nodes:
            # subject is a reified node
            self.add_node_attribute(s, key=p, value=o)
        elif p in {RDF.subject, RDF.object, RDF.predicate}:
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=p, value=o)
        elif property_name in {'subject', 'edge_label', 'object', 'predicate', 'relation'}:
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=p, value=o)
        elif o in {RDF.Statement, self.BIOLINK.Association}:
            self.reified_nodes.add(s)
            self.add_node_attribute(s, key=p, value=o)
        elif p in self.node_properties or property_name in self.node_properties:
            # treating predicate as a node property
            self.add_node_attribute(s, key=p, value=o)
        else:
            # treating predicate as an edge
            print(f"Adding edge {s} {p} {o}")
            self.add_edge(s, o, p)

    def get_biolink_element(self, predicate: str, property_name: str) -> Element:
        """
        Returns a Biolink Model element for a given predicate and property name.

        If property_name could not be mapped to a biolink model element
        then will return ``None``.

        Parameters
        ----------
        predicate: str
            The CURIE of a predicate
        property_name: str
            The property name (usually the reference of a CURIE)

        Returns
        -------
        Optional[Element]
            The corresponding Biolink Model element

        """
        element = self.toolkit.get_element(property_name)
        if not element:
            # using original predicate to get mapping
            try:
                mapping = self.toolkit.get_by_mapping(predicate)
                element = self.toolkit.get_element(mapping)
            except ValueError as e:
                logging.error(e)
        return element

    def dereify(self, nodes: Set[str]) -> None:
        """
        Dereify a set of nodes where each node has all the properties
        necessary to create an edge.

        Parameters
        ----------
        nodes: Set[str]
            A set of nodes

        """
        for n in nodes:
            n_curie = self.prefix_manager.contract(str(n))
            node = self.graph.nodes[n_curie]
            if 'relation' not in node:
                node['relation'] = node['predicate']
            if 'edge_label' not in node:
                node['edge_label'] = "biolink:related_to"
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
        reified_node['id'] = node_id
        reified_node['type'] = 'biolink:Association'
        reified_node['subject'] = s
        reified_node['edge_label'] = p
        reified_node['object'] = o
        return reified_node

    def save(self, filename: str = None, output_format: str = "turtle", reify_all_edges = False, **kwargs) -> None:
        """
        Transform networkx.MultiDiGraph into rdflib.Graph and export
        this graph as a file (``turtle``, by default).

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format; default: ``turtle``
        kwargs: Dict
            Any additional arguments

        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()
        rdfgraph.bind('', str(self.DEFAULT))
        rdfgraph.bind('OBO', str(self.OBO))
        rdfgraph.bind('biolink', str(self.BIOLINK))

        self.save_nodes(rdfgraph)
        self.save_edges(rdfgraph, reify_all_edges)

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)

    # def save_node(self, rdfgraph: rdflib.graph, node: str, data: Dict) -> None:
    #     """
    #     Save a node and its attributes to rdflib.Graph
    #
    #     Parameters
    #     ----------
    #     rdfgraph: rdflib.Graph
    #         rdflib.Graph containing nodes and edges
    #     node: str
    #         Node identifier, as a CURIE
    #     data: Dict
    #         Node properties
    #
    #     """
    #     node_uri = URIRef(data['id'])
    #
    #     # Defaulting to biolink:NamedThing for all nodes
    #     rdfgraph.add((node_uri, RDF.type, URIRef(self.BIOLINK.NamedThing)))
    #     for key, value in data.items():
    #         if key not in {'id', 'iri'}:
    #             self.save_attribute(rdfgraph, node_uri, key=key, value=value)

    def save_nodes(self, rdfgraph):
        for n, data in self.graph.nodes(data=True):
            s = self.uriref(n)
            for k, v in data.items():
                if k in {'id', 'iri'}:
                    continue
                prop_type = self._get_property_type(k)
                p = self.uriref(k)
                logging.info(f"[n] Treating {p} as {prop_type}")
                if isinstance(v, (list, set, tuple)):
                    for x in v:
                        o = self._prepare_object(k, prop_type, x)
                        rdfgraph.add((s, p, o))
                else:
                    o = self._prepare_object(k, prop_type, v)
                    rdfgraph.add((s, p, o))

    # def save_edge(self, rdfgraph: rdflib.Graph, subject: str, object: str, data: Dict) -> None:
    #     """
    #     Save an edge to rdflib.Graph, reifying where applicable.
    #
    #     Parameters
    #     ----------
    #     rdfgraph: rdflib.Graph
    #         rdflib.Graph containing nodes and edges
    #     subject: str
    #         Subject node identifier, as a CURIE
    #     object: str
    #         Object node identifier, as a CURIE
    #     data: Dict
    #         Edge properties
    #
    #     """
    #     edge_label = data['edge_label']
    #
    #     if edge_label in self.BASIC_BIOLINK_PREDICATES \
    #                 or edge_label in self.BASIC_PREDICATES \
    #                 or edge_label in self.OWL_PREDICATES:
    #         # Note: This drops all edge properties since we do not reify these edges
    #         subject_term = self.uriref(subject)
    #         object_term = self.uriref(object)
    #         if edge_label in self.BASIC_PREDICATES:
    #             edge_label = f"biolink:{edge_label}"
    #         predicate_term = self.uriref(edge_label)
    #         rdfgraph.add((subject_term, predicate_term, object_term))
    #     else:
    #         element = self.toolkit.get_element(edge_label)
    #         if element:
    #             # edge is a Biolink association
    #             if 'relation' not in data:
    #                 raise Exception(
    #                     'Relation is a required edge property in the Biolink Model, edge {} --> {}'.format(subject, object))
    #
    #             if 'id' in data and data['id'] is not None:
    #                 assoc_id = URIRef(data['id'])
    #             else:
    #                 # generating a UUID for association
    #                 assoc_id = URIRef('urn:uuid:{}'.format(uuid.uuid4()))
    #
    #             # Defaulting to biolink:Association for all reified edges
    #             if 'type' in data:
    #                 assoc_type = data['type']
    #             else:
    #                 assoc_type = self.BIOLINK.association
    #             rdfgraph.add((assoc_id, RDF.type, URIRef(assoc_type)))
    #             s = self.uriref(subject)
    #             p = self.uriref(data['edge_label'])
    #             o = self.uriref(object)
    #             relation = self.uriref(data['relation'])
    #             rdfgraph.add((assoc_id, self.OBAN.association_has_subject, s))
    #             rdfgraph.add((assoc_id, self.OBAN.association_has_predicate, p))
    #             rdfgraph.add((assoc_id, self.OBAN.association_has_object, o))
    #             rdfgraph.add((assoc_id, self.BIOLINK.relation, relation))
    #
    #             for key, value in data.items():
    #                 if key not in {'subject', 'relation', 'object', 'edge_label'}:
    #                     self.save_attribute(rdfgraph, assoc_id, key=key, value=value)
    #         else:
    #             s = self.uriref(subject)
    #             p = self.uriref(edge_label)
    #             o = self.uriref(object)
    #             rdfgraph.add((s, p, o))

    def save_edges(self, rdfgraph, reify_all_edges = False):
        associations = get_biolink_association_types()
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            if reify_all_edges:
                reified_node = self.reify(u, v, k, data)
                s = reified_node['subject']
                p = reified_node['edge_label']
                o = reified_node['object']
                rdfgraph.add((s, p, o))
                n = reified_node['id']
                for prop, value in reified_node.items():
                    if prop in {'id', 'association_id', 'edge_key'}:
                        continue
                    prop_type = self._get_property_type(prop)
                    p = self.uriref(prop)
                    logging.info(f"[e] Treating {p} as {prop_type}")
                    if isinstance(value, list):
                        for x in value:
                            o = self._prepare_object(prop, prop_type, x)
                            rdfgraph.add((n, p, o))
                    else:
                        o = self._prepare_object(prop, prop_type, value)
                        rdfgraph.add((n, p, o))
            else:
                if 'type' in data and data['type'] in associations:
                    reified_node = self.reify(u, v, k, data)
                    s = reified_node['subject']
                    p = reified_node['edge_label']
                    o = reified_node['object']
                    rdfgraph.add((s, p, o))
                    n = reified_node['id']
                    for prop, value in reified_node.items():
                        if prop in {'id', 'association_id', 'edge_key'}:
                            continue
                        prop_type = self._get_property_type(prop)
                        p = self.uriref(prop)
                        if isinstance(value, list):
                            for x in value:
                                o = self._prepare_object(prop, prop_type, x)
                                rdfgraph.add((n, p, o))
                        else:
                            o = self._prepare_object(prop, prop_type, value)
                            rdfgraph.add((n, p, o))
                    rdfgraph.add((s, p, o))
                else:
                    s = self.uriref(u)
                    p = self.uriref(data['edge_label'])
                    o = self.uriref(v)
                    rdfgraph.add((s, p, o))


    def _prepare_object(self, prop, prop_type, value):
        print(prop, prop_type, value)
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

    def _get_property_type(self, p):
        if p in {'type'}:
            t = 'uriorcurie'
        elif p in self.property_types:
            t = self.property_types[p]
        else:
            t = 'xsd:string'
        print(f"# {p} => {t}")
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
            if identifier == uri:
                if PrefixManager.is_curie(identifier):
                    identifier = identifier.replace(':', '_')
                if ' ' in identifier:
                    identifier = identifier.replace(' ', '_')
                uri = self.DEFAULT.term(identifier)

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
        if element:
            if element.is_a == 'association slot' or element.is_a == 'node property':
                if key in property_mapping:
                    key = property_mapping[key]
                else:
                    key = self.BIOLINK.term(element.name.replace(' ', '_'))
                if not isinstance(value, (list, tuple, set)):
                    value = [value]
                for v in value:
                    if element.range == 'iri type':
                        v = self.uriref(v)
                    else:
                        v = rdflib.term.Literal(v)
                    rdfgraph.add((object_iri, key, v))
        else:
            # key is not a biolink property
            # treat value as a literal
            key = self.DEFAULT.term(key)
            if not isinstance(value, (list, tuple, set)):
                value = [value]
            for v in value:
                v = rdflib.term.Literal(v)
                rdfgraph.add((object_iri, key, v))


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
        associations = rdfgraph.subjects(RDF.type, self.OBAN.association)
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
                    if o.startswith(self.PMID):
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
                    predicate = self.DEFAULT_EDGE_LABEL

                if subject and object:
                    self.add_edge(subject, object, predicate)
                    for key, values in edge_attr.items():
                        for value in values:
                            self.add_edge_attribute(subject, object, predicate, key=key, value=value)

    def save(self, filename: str = None, output_format: str = "turtle", **kwargs) -> None:
        """
        Transform networkx.MultiDiGraph into rdflib.Graph that follow OBAN-style reification and export
        this graph as a file (``turtle``, by default).

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format; default: ``turtle``
        kwargs: dict
            Any additional arguments

        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()

        # <http://purl.obolibrary.org/obo/RO_0002558> is currently stored as OBO:RO_0002558 rather than RO:0002558
        # because of the bug in rdflib. See https://github.com/RDFLib/rdflib/issues/632
        rdfgraph.bind('', str(self.DEFAULT))
        rdfgraph.bind('OBO', str(self.OBO))
        rdfgraph.bind('OBAN', str(self.OBAN))
        rdfgraph.bind('PMID', str(self.PMID))
        rdfgraph.bind('biolink', str(self.BIOLINK))

        # saving all nodes
        for n, data in self.graph.nodes(data=True):
            if 'iri' not in data:
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

            rdfgraph.add((assoc_id, RDF.type, self.OBAN.association))
            rdfgraph.add((assoc_id, self.OBAN.association_has_subject, self.uriref(u)))
            rdfgraph.add((assoc_id, self.OBAN.association_has_predicate, self.uriref(data['relation'])))
            rdfgraph.add((assoc_id, self.OBAN.association_has_object, self.uriref(v)))

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
        logging.info("Loading RDFS:subClassOf triples from rdflib.Graph to networkx.MultiDiGraph")
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
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
                        logging.warning("Do not know how to handle BNode: {}".format(o))
                        continue
                else:
                    # C SubClassOf D (C and D are named classes)
                    pred = p
                    parent = o
                self.add_edge(s, parent, pred)

        triples = rdfgraph.triples((None, OWL.equivalentClass, None))
        logging.info("Loading OWL:equivalentClass triples from rdflib.Graph to networkx.MultiDiGraph")
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                self.add_edge(s, o, p)
        relations = rdfgraph.subjects(RDF.type, OWL.ObjectProperty)
        logging.debug("Loading relations")
        with click.progressbar(relations, label='Progress') as bar:
            for relation in bar:
                for _, p, o in rdfgraph.triples((relation, None, None)):
                    if o.startswith('http://purl.obolibrary.org/obo/RO_'):
                        self.add_edge(relation, o, p)
                    else:
                        self.add_node_attribute(relation, key=p, value=o)
                self.add_node_attribute(relation, key='category', value='relation')
        triples = rdfgraph.triples((None, None, None))
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                if p in property_mapping.keys():
                    self.add_node_attribute(s, key=p, value=o)
