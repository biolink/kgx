import click, rdflib, logging, os, uuid
import networkx as nx
from typing import Tuple, Union, Set, List, Dict
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL
from collections import defaultdict
from prefixcommons.curie_util import read_remote_jsonld_context

from kgx.prefix_manager import PrefixManager
from kgx.transformers.transformer import Transformer
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin
from kgx.utils.rdf_utils import property_mapping, infer_category, reverse_property_mapping
from kgx.utils.kgx_utils import get_toolkit, sentencecase_to_snakecase, get_biolink_node_properties, \
    get_biolink_edge_properties


class RdfTransformer(RdfGraphMixin, Transformer):
    """
    Transformer that parses RDF and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    This is the base class which is used to implement other RDF-based transformers.
    """

    OWL_PREDICATES = {RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass}
    BASIC_PREDICATES = {'subclass_of', 'part_of', 'has_part', 'same_as'}
    BASIC_BIOLINK_PREDICATES = set([f"biolink:{x}" for x in BASIC_PREDICATES])

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        super().__init__(source_graph, curie_map)
        self.toolkit = get_toolkit()

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

        self.load_networkx_graph(rdfgraph, node_property_predicates)
        self.report()

    def load_networkx_graph(self, rdfgraph: rdflib.Graph = None, node_property_predicates: Set[str] = None, **kwargs) -> None:
        """
        Walk through the rdflib.Graph and load all required triples into networkx.MultiDiGraph

        Optionally, you can define which predicates are to be treated as node properties.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        node_property_predicates: Set[str]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties
        kwargs: dict
            Any additional arguments

        """
        reified_nodes = set()
        node_properties = get_biolink_node_properties()
        node_properties += node_property_predicates
        np = [URIRef(self.prefix_manager.expand(x)) for x in node_properties]
        triples = rdfgraph.triples((None, None, None))
        with click.progressbar(list(triples), label='Progress') as bar:
            for s, p, o in bar:
                if s in reified_nodes:
                    # subject is a reified node
                    self.add_node_attribute(s, key=p, value=o)
                elif p in {RDF.subject, RDF.object, RDF.predicate}:
                    # subject is a reified node
                    reified_nodes.add(s)
                    self.add_node_attribute(s, key=p, value=o)
                elif o in {RDF.Statement}:
                    # subject is a reified node
                    reified_nodes.add(s)
                    self.add_node_attribute(s, key=p, value=o)
                elif p in np:
                    # treating predicate as a node property
                    self.add_node_attribute(s, key=p, value=o)
                else:
                    # treating predicate as an edge
                    self.add_edge(s, o, p)
        self.dereify(reified_nodes)

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
            node = self.graph.nodes[str(n)]
            if 'relation' not in node:
                node['relation'] = node['predicate']
            if 'edge_label' not in node:
                node['edge_label'] = "biolink:related_to"
            self.add_edge(node['subject'], node['object'], node['edge_label'], **node)
            self.graph.remove_node(str(n))

    def save(self, filename: str = None, output_format: str = "turtle", **kwargs) -> None:
        """
        Transform networkx.MultiDiGraph into rdflib.Graph and export
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
        rdfgraph.bind('', str(self.DEFAULT))
        rdfgraph.bind('OBO', str(self.OBO))
        rdfgraph.bind('OBAN', str(self.OBAN))
        rdfgraph.bind('PMID', str(self.PMID))
        rdfgraph.bind('biolink', str(self.BIOLINK))

        # saving all nodes
        for n, data in self.graph.nodes(data=True):
            self.save_node(rdfgraph, n, data)

        # saving all edges
        for u, v, data in self.graph.edges(data=True):
            self.save_edge(rdfgraph, u, v, data)

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)
        pass

    def save_node(self, rdfgraph: rdflib.graph, node: str, data: dict) -> None:
        """
        Save a node and its attributes to rdflib.Graph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            rdflib.Graph containing nodes and edges
        node: str
            Node identifier, as a CURIE
        data: dict
            Node properties

        """
        if 'iri' not in data:
            uriRef = self.uriref(node)
        else:
            uriRef = URIRef(data['iri'])

        # Defaulting to biolink:NamedThing for all nodes
        rdfgraph.add((uriRef, RDF.type, URIRef(self.BIOLINK.NamedThing)))
        for key, value in data.items():
            if key not in {'id', 'iri'}:
                self.save_attribute(rdfgraph, uriRef, key=key, value=value)

    def save_edge(self, rdfgraph: rdflib.Graph, subject: str, object: str, data: dict) -> None:
        """
        Save an edge to rdflib.Graph, reifying where applicable.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            rdflib.Graph containing nodes and edges
        subject: str
            Subject node identifier, as a CURIE
        object: str
            Object node identifier, as a CURIE
        data: dict
            Edge properties

        """
        edge_label = data['edge_label']

        if edge_label in self.BASIC_BIOLINK_PREDICATES \
                    or edge_label in self.BASIC_PREDICATES \
                    or edge_label in self.OWL_PREDICATES:
            # Note: This drops all edge properties since we do not reify the edge
            subject_term = self.uriref(subject)
            object_term = self.uriref(object)
            if edge_label in self.BASIC_PREDICATES:
                edge_label = f"biolink:{edge_label}"
            predicate_term = self.uriref(edge_label)
            rdfgraph.add((subject_term, predicate_term, object_term))
        else:
            element = self.toolkit.get_element(edge_label)
            if element:
                # edge is a Biolink association
                if 'relation' not in data:
                    raise Exception(
                        'Relation is a required edge property in the Biolink Model, edge {} --> {}'.format(subject, object))

                if 'id' in data and data['id'] is not None:
                    assoc_id = URIRef(data['id'])
                else:
                    # generating a UUID for association
                    assoc_id = URIRef('urn:uuid:{}'.format(uuid.uuid4()))

                # Defaulting to biolink:Association for all reified edges
                rdfgraph.add((assoc_id, RDF.type, self.BIOLINK.association))
                subject_term = self.uriref(subject)
                object_term = self.uriref(object)
                predicate_term = self.uriref(data['edge_label'])
                relation = self.uriref(data['relation'])
                rdfgraph.add((assoc_id, self.OBAN.association_has_subject, subject_term))
                rdfgraph.add((assoc_id, self.OBAN.association_has_predicate, predicate_term))
                rdfgraph.add((assoc_id, self.OBAN.association_has_object, object_term))
                rdfgraph.add((assoc_id, self.BIOLINK.relation, relation))

                for key, value in data.items():
                    if key not in ['subject', 'relation', 'object', 'edge_label']:
                        self.save_attribute(rdfgraph, assoc_id, key=key, value=value)
            else:
                subject_term = self.uriref(subject)
                predicate_term = self.uriref(edge_label)
                object_term = self.uriref(object)
                rdfgraph.add((subject_term, predicate_term, object_term))

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
