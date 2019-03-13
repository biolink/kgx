import click, rdflib, logging, os, uuid, bmt
import networkx as nx

from typing import Tuple, Union, List

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL

from .transformer import Transformer
from .utils.rdf_utils import find_category, category_mapping, equals_predicates, property_mapping, predicate_mapping, process_iri, make_curie, is_property_multivalued

from collections import defaultdict

from abc import ABCMeta, abstractmethod

OBAN = Namespace('http://purl.org/oban/')
PMID = Namespace('http://www.ncbi.nlm.nih.gov/pubmed/')
BIOLINK = Namespace('http://w3id.org/biolink/vocab/')

mapping = {value : key for key, value in property_mapping.items()}

class RdfTransformer(Transformer, metaclass=ABCMeta):
    """
    Transforms to and from RDF

    We support different RDF metamodels, including:

     - OBAN reification (as used in Monarch)
     - RDF reification

    TODO: we will have some of the same logic if we go from a triplestore. How to share this?
    """

    def __init__(self, source:Union[Transformer, nx.MultiDiGraph]=None):
        super().__init__(source)
        self.ontologies = []

    def parse(self, filename:str=None, provided_by:str=None, *, input_format=None):
        """
        Parse a file into an graph, using rdflib
        """
        rdfgraph = rdflib.Graph()

        if input_format is None:
            input_format = rdflib.util.guess_format(filename)

        logging.info("Parsing {} with {} format".format(filename, input_format))
        rdfgraph.parse(filename, format=input_format)
        logging.info("Parsed : {}".format(filename))

        # TODO: use source from RDF
        if provided_by is None:
            if isinstance(filename, str):
                self.graph_metadata['provided_by'] = os.path.basename(filename)
            elif hasattr(filename, 'name'):
                self.graph_metadata['provided_by'] = filename.name


        self.load_networkx_graph(rdfgraph)
        self.load_node_attributes(rdfgraph)
        self.report()

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
        kwargs = {
            'iri' : str(iri)
        }

        if 'provided_by' in self.graph_metadata:
            provided_by = self.graph_metadata['provided_by']
            if isinstance(provided_by, list):
                kwargs['provided_by'] = provided_by
            elif isinstance(provided_by, str):
                kwargs['provided_by'] = [provided_by]
            else:
                raise Exception('provided_by must be a string or list, instead it was {}'.format(type(provided_by)))

        n = make_curie(iri)

        if n not in self.graph:
            self.graph.add_node(n, **kwargs)

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

        if edge_label.startswith(BIOLINK):
            edge_label = edge_label.replace(BIOLINK, '')

        if ':' in edge_label:
            edge_label = 'related_to'

        kwargs = {
            'relation' : relation,
            'edge_label' : edge_label,
        }

        if 'provided_by' in self.graph_metadata:
            provided_by = self.graph_metadata['provided_by']
            if isinstance(provided_by, list):
                kwargs['provided_by'] = provided_by
            elif isinstance(provided_by, str):
                kwargs['provided_by'] = [provided_by]
            else:
                raise Exception('provided_by must be a string or list, instead it was {}'.format(type(provided_by)))

        if not self.graph.has_edge(s, o, key=edge_label):
            self.graph.add_edge(s, o, key=edge_label, **kwargs)

        return s, o, edge_label

    def add_node_attribute(self, iri:URIRef, *, key:str, value:str) -> None:
        """
        Adds an attribute to a node, respecting whether or not that property
        should be multi-valued. Multi-valued properties will not contain
        duplicates.

        The key may be a URIRef or a URI string that maps onto a property name
        in `property_mapping`.

        If the node does not exist then it is created.

        Parameters
        ----------
        iri : The iri of a node in the RDF graph
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

    def add_edge_attribute(self, subject_iri:URIRef, object_iri:URIRef, predicate_iri:URIRef, *, key:str, value:str) -> None:
        """
        Adds an attribute to an edge, respecting whether or not that property
        should be multi-valued. Multi-valued properties will not contain
        duplicates.

        The key may be a URIRef or a URI string that maps onto a property name
        in `property_mapping`.

        If the nodes or their edge does not yet exist then they will be created.

        Parameters
        ----------
        subject_iri : The iri of the subject node in the RDF graph
        object_iri : The iri of the object node in the RDF graph
        predicate_iri : The iri of the predicate in the RDF graph
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
    def load_networkx_graph(self, rdfgraph:rdflib.Graph):
        """
        This method should be overridden and implemented by the derived class,
        and should load all desired nodes and edges from rdfgraph into nxgraph.

        It's preferred that this method doesn't use the NetworkX graph directly
        when adding edges or nodes or their attributes. Using the following
        methods instead will ensure that nodes, edges, and their attributes
        are added in conformance with the biolink model, and that URIRef's are
        translated into CURIEs or biolink model elements whenever appropriate:

        add_edge_attribute(self, subject_iri:URIRef, object_iri:URIRef, predicate_iri:URIRef, key:str, value:str)
        add_node_attribute(self, iri:URIRef, key:str, value:str)
        add_edge(self, subject_iri:URIRef, object_iri:URIRef, predicate_iri:URIRef) -> Tuple[str, str, str]
        add_node(self, iri:URIRef) -> str
        """
        pass

    def load_node_attributes(self, rdfgraph:rdflib.Graph):
        """
        This method loads the properties of nodes in the NetworkX graph. As there
        can be many values for a single key, all properties are lists by default.

        This method assumes that load_edges has been called, and that all nodes
        have had their IRI saved as an attribute.
        """
        with click.progressbar(self.graph.nodes(), label='loading node attributes') as bar:
            for n in bar:
                if 'iri' in self.graph.node[n]:
                    uriRef = URIRef(self.graph.node[n]['iri'])
                else:
                    provided_by = self.graph_metadata.get('provided_by')
                    logging.warning("Expected IRI for {} provided by {}".format(n, provided_by))
                    continue

                for s, p, o in rdfgraph.triples((uriRef, None, None)):
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        if not isinstance(s, rdflib.term.BNode) and not isinstance(o, rdflib.term.BNode):
                            self.add_node_attribute(uriRef, key=p, value=o)

                category = find_category(uriRef, [rdfgraph] + self.ontologies)

                if category is not None:
                    self.add_node_attribute(uriRef, key='category', value=category)

class ObanRdfTransformer(RdfTransformer):
    ontological_predicates = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    def load_networkx_graph(self, rdfgraph:rdflib.Graph):
        for rel in self.ontological_predicates:
            triples = list(rdfgraph.triples((None, rel, None)))
            with click.progressbar(triples, label='loading {}'.format(rel)) as bar:
                for s, p, o in bar:
                    if not isinstance(s, rdflib.term.BNode) and not isinstance(o, rdflib.term.BNode):
                        self.add_edge(s, o, p)

        associations = list(rdfgraph.subjects(RDF.type, OBAN.association))
        with click.progressbar(associations, label='loading graph') as bar:
            for association in bar:
                edge_attr = defaultdict(list)

                subjects = []
                objects = []
                predicates = []

                edge_attr['id'].append(str(association))

                for s, p, o in rdfgraph.triples((association, None, None)):
                    if o.startswith(PMID):
                        edge_attr['publications'].append(o)
                    if p in property_mapping or isinstance(o, rdflib.term.Literal):
                        p = property_mapping.get(p, p)
                        if p == 'subject':
                            subjects.append(o)
                        elif p == 'object':
                            objects.append(o)
                        elif p == 'predicate':
                            predicates.append(o)
                        else:
                            edge_attr[p].append(o)

                if len(predicates) == 0:
                    predicates.append('related_to')

                for subject_iri in subjects:
                    for object_iri in objects:
                        for predicate_iri in predicates:
                            self.add_edge(subject_iri, object_iri, predicate_iri)
                            for key, values in edge_attr.items():
                                for value in values:
                                    self.add_edge_attribute(
                                        subject_iri,
                                        object_iri,
                                        predicate_iri,
                                        key=key,
                                        value=value
                                    )
    def uriref(self, id) -> URIRef:
        if id in mapping:
            uri = mapping[id]
        else:
            uri = self.prefix_manager.expand(id)
        return URIRef(uri)

    def save_attribute(self, rdfgraph:rdflib.Graph, obj_iri:URIRef, *, key:str, value:Union[List[str], str]) -> None:
        """
        Saves a node or edge attributes from the biolink model in the rdfgraph.
        Intended to be used within `ObanRdfTransformer.save`.
        """
        element = bmt.get_element(key)
        if element is None:
            return
        if element.is_a == 'association slot' or element.is_a == 'node property':
            if key in mapping:
                key = mapping[key]
            else:
                key = URIRef('{}{}'.format(BIOLINK, element.name.replace(' ', '_')))
            if not isinstance(value, (list, tuple, set)):
                value = [value]
            for value in value:
                if element.range == 'iri type':
                    value = URIRef('{}{}'.format(BIOLINK, ''.join(value.title().split(' '))))
                rdfgraph.add((obj_iri, key, rdflib.term.Literal(value)))

    def save(self, filename:str = None, output_format:str = None, **kwargs):
        """
        Transform the internal graph into the RDF graphs that follow OBAN-style modeling and dump into the file.
        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()
        # Register OBAN's url prefix (http://purl.org/oban/) as `OBAN` in the namespace.
        rdfgraph.bind('OBAN', str(OBAN))

        # <http://purl.obolibrary.org/obo/RO_0002558> is currently stored as OBO:RO_0002558 rather than RO:0002558
        # because of the bug in rdflib. See https://github.com/RDFLib/rdflib/issues/632
        rdfgraph.bind('OBO', 'http://purl.obolibrary.org/obo/')
        rdfgraph.bind('biolink', 'http://w3id.org/biolink/vocab/')

        for n, data in self.graph.nodes(data=True):
            if 'iri' not in n:
                uriRef = self.uriref(n)
            else:
                uriRef = URIRef(data['iri'])

            for key, value in data.items():
                if key not in ['id', 'iri']:
                    self.save_attribute(rdfgraph, uriRef, key=key, value=value)

        for u, v, data in self.graph.edges(data=True):
            if 'relation' not in data:
                raise Exception('Relation is a required edge property in the biolink model, edge {} --> {}'.format(u, v))

            if 'id' in data and data['id'] is not None:
                assoc_id = URIRef(adjitem['id'])
            else:
                assoc_id = URIRef('urn:uuid:{}'.format(uuid.uuid4()))

            rdfgraph.add((assoc_id, RDF.type, OBAN.association))
            rdfgraph.add((assoc_id, OBAN.association_has_subject, self.uriref(u)))
            rdfgraph.add((assoc_id, OBAN.association_has_predicate, self.uriref(data['relation'])))
            rdfgraph.add((assoc_id, OBAN.association_has_object, self.uriref(v)))

            for key, value in data.items():
                if key not in ['subject', 'relation', 'object']:
                    self.save_attribute(rdfgraph, assoc_id, key=key, value=value)

        if output_format is None:
            output_format = "turtle"

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)

class HgncRdfTransformer(RdfTransformer):
    """
    Custom transformer for loading:
    https://data.monarchinitiative.org/ttl/hgnc.ttl
    """
    is_about = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
    has_subsequence = URIRef('http://purl.obolibrary.org/obo/RO_0002524')
    is_subsequence_of = URIRef('http://purl.obolibrary.org/obo/RO_0002525')
    ontological_predicates = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    def load_networkx_graph(self, rdfgraph:rdflib.Graph):
        triples = list(rdfgraph.triples((None, None, None)))

        with click.progressbar(triples, label='loading graph') as bar:
            for s, p, o in bar:
                if p == self.is_about:
                    self.add_node_attribute(o, key=s, value='publications')
                elif p == self.has_subsequence:
                    self.add_edge(o, s, self.is_subsequence_of)
                elif p == self.is_subsequence_of:
                    self.add_edge(s, o, self.is_subsequence_of)
                elif any(p.lower() == x.lower() for x in self.ontological_predicates):
                    self.add_edge(s, o, p)

class RdfOwlTransformer(RdfTransformer):
    """
    Transforms from an OWL ontology in RDF, retaining class-class
    relationships
    """
    def load_networkx_graph(self, rg:rdflib.Graph):
        triples = list(rg.triples((None,RDFS.subClassOf,None)))
        with click.progressbar(triples) as bar:
            for s, p, o in bar:
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
                        logging.warning("Do not know how to handle BNode: {}".format(o))
                        continue
                else:
                    # C SubClassOf D (C and D are named classes)
                    pred = p
                    parent = o
                self.add_edge(s, parent, pred)

        relations = list(rg.subjects(RDF.type, OWL.ObjectProperty))
        with click.progressbar(relations) as bar:
            for relation in bar:
                for _, p, o in rg.triples((relation, None, None)):
                    if o.startswith('http://purl.obolibrary.org/obo/RO_'):
                        self.add_edge(relation, o, p)
                    else:
                        self.add_node_attribute(relation, key=p, value=o)
                self.add_node_attribute(relation, key='category', value='relation')
