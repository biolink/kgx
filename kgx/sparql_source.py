import logging, yaml
import re
from collections import defaultdict
from itertools import zip_longest
from pystache import render

from .source import Source
from SPARQLWrapper import SPARQLWrapper, JSON, POSTDIRECTLY
from .utils.rdf_utils import property_mapping, process_iri, make_curie, is_property_multivalued

import rdflib
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL
from typing import Set, List, Dict, Generator

BIOLINK = Namespace('http://w3id.org/biolink/vocab/')
DEFAULT_EDGE_LABEL = 'related_to'


def un_camel_case(s):
    """
    https://stackoverflow.com/a/1176023/4750502
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).lower()


def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks"
    """
    yield from zip_longest(*[iter(iterable)] * n, fillvalue=fillvalue)


class SparqlSource(Source):
    """
    Source for SPARQL endpoints, though this may only work with Red Team KG.
    """
    is_about = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
    has_subsequence = URIRef('http://purl.obolibrary.org/obo/RO_0002524')
    is_subsequence_of = URIRef('http://purl.obolibrary.org/obo/RO_0002525')
    OWL_PREDICATES = [RDFS.subClassOf, OWL.sameAs, OWL.equivalentClass]

    count_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>
    SELECT (COUNT(*) AS ?triples)
    WHERE {
        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}
        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}
        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}
        ?a rdf:type {{{association}}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    """

    get_node_properties_query = """
    PREFIX bl: <http://w3id.org/biolink/vocab/>
    SELECT ?subject ?predicate ?object
    WHERE
    {{
        ?subject ?predicate ?object
        VALUES ?subject {{
            {curie_list}
        }}
    }}
    ORDER BY ?subject
    """

    edge_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>
    SELECT ?subject ?predicate ?object ?edge_property_key ?edge_property_value
    WHERE {
        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}
        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}
        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}
        ?a rdf:type {{{association}}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    ORDER BY ?subject ?predicate ?object
    OFFSET {{offset}}
    LIMIT {{limit}}
    """

    def __init__(self, sink):
        super(SparqlSource, self).__init__(sink)
        with open("config.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            self.url = cfg['sparql']['host']

    def load(self):
        nodes_seen = set()
        # TODO: how do we just get them all?
        predicates = ['bl:ChemicalToGeneAssociation',
                      'bl:ChemicalToThingAssociation']
        #if predicates is None:
            #predicates = set()
            #predicates = predicates.union(self.OWL_PREDICATES, [self.is_about, self.is_subsequence_of, self.has_subsequence])
        for predicate in predicates:
            sparql = SPARQLWrapper(self.url)
            association = predicate
            #association = '<{}>'.format(predicate)
            # association = 'bl:ChemicalToGeneAssociation'  # Old way, but why no <>?
            query = render(self.count_query, {'association': association})
            logging.debug(query)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            count = int(results['results']['bindings'][0]['triples']['value'])
            logging.info("Expected triples for query: {}".format(count))
            step = 1000
            start = 0
            current = (None, None, None)
            attrs = None
            for i in range(step, count + step, step):
                query = render(self.edge_query, {'association': association, 'offset': start, 'limit':step})
                start = i
                sparql.setQuery(query)
                logging.info("Fetching triples with predicate {}".format(predicate))
                results = sparql.query().convert()
                logging.info("Triples fetched")
                for r in results['results']['bindings']:
                    s = r['subject']['value']
                    o = r['object']['value']
                    p = r['predicate']['value']
                    if current != (s, o, p):
                        if attrs is not None:
                            self.add_edge(s, o, p, attrs)
                        current = (s, o, p)
                        attrs = defaultdict(list)
                        nodes_seen.add(s)
                        nodes_seen.add(o)
                    attrs[r['edge_property_key']['value']].append(r['edge_property_value']['value'])
            if attrs is not None:
                self.add_edge(s, o, p, attrs)
        self.load_nodes(["<{}>".format(n) for n in nodes_seen])

    def load_nodes(self, node_set):
        """
        This method queries the SPARQL endpoint for all triples where nodes in the
        node_set is a subject.
        Parameters
        ----------
        node_set: list
            A list of node CURIEs
        """
        node_generator = grouper(node_set, 10000)
        nodes = next(node_generator, None)
        while nodes is not None:
            logging.info("Fetching properties for {} nodes".format(len(nodes)))
            nodes = filter(None, nodes)
            # TODO: is there a better way to fetch node properties?
            query = self.get_node_properties_query.format(curie_list=' '.join(nodes))
            logging.info(query)
            sparql = SPARQLWrapper(self.url)
            sparql.setRequestMethod(POSTDIRECTLY)
            sparql.setMethod("POST")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            node_results = sparql.query().convert()
            current = None
            attrs = None
            for r in node_results['results']['bindings']:
                if r['object']['type'] != 'bnode':
                    subject = r['subject']['value']
                    if subject != current:
                        if attrs is not None:
                            self.add_node(current, attrs)
                        current = subject
                        attrs = defaultdict(list)
                    object = r['object']['value']
                    predicate = r['predicate']['value']
                    if predicate.startswith('bl:'):
                        predicate = predicate.split(':')[1]
                    attrs[predicate].append(object)
            if attrs is not None:
                self.add_node(current, attrs)
            nodes = next(node_generator, None)

    def add_edge(self, subject_iri, object_iri, predicate_iri, attrs):
        """
        This method should be used by all derived classes when adding an edge to
        the graph. This ensures that the nodes identifiers are CURIEs, and that
        their IRI properties are set.
        """
        s = make_curie(subject_iri)
        o = make_curie(object_iri)
        relation = make_curie(predicate_iri)
        edge_label = process_iri(predicate_iri).replace(' ', '_')
        if edge_label.startswith(BIOLINK):
            edge_label = edge_label.replace(BIOLINK, '')
        if ':' in edge_label:  # edge_label is a CURIE.
            edge_label = DEFAULT_EDGE_LABEL
        attrs['relation'] = relation
        attrs['edge_label'] = edge_label
        self.sink.add_edge(s, o, attrs)

    def add_node(self, node_iri, raw_attrs):
        """
        Adds a node with attributes, respecting whether or not those attributes
        should be multi-valued. Multi-valued properties will not contain
        duplicates.
        Some attributes are singular forms of others, e.g. name -> synonym. In
        such cases overflowing values will be placed into the correlating
        multi-valued attribute.
        The key may be a URIRef or a URI string that maps onto a property name
        in `property_mapping`.
        Parameters
        ----------
        node_iri : The iri of a node in the RDF graph
        raw_attrs: A dictionary to list values, with keys that are URIRef or URI string
        """
        n = make_curie(node_iri)
        attrs = {}
        synonyms = []
        for key, vs in raw_attrs.items():
            if key.lower() in is_property_multivalued:
                key = key.lower()
            else:
                if not isinstance(key, URIRef):
                    key = URIRef(key)
                key = property_mapping.get(key, str(key))
            vs = [make_curie(process_iri(v)) for v in vs]
            if is_property_multivalued.get(key, True):
                attrs[key] = vs
            elif key == 'name':
                attrs['name'] = vs[0]
                synonyms = vs[1:]
            else:
                attrs[key] = vs[0]
        if synonyms:
            if 'synonym' not in attrs:
                attrs['synonym'] = synonyms
            else:
                attrs['synonym'].extend(synonyms)
        if 'category' not in attrs and 'type' in attrs:
            attrs['category'] = [un_camel_case(t.replace('biolink:', '')) for t in attrs['type']]
        self.sink.add_node(n, attrs)
