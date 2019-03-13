import logging

from rdflib import Graph
from requests import HTTPError

from .transformer import Transformer
from pystache import render
from SPARQLWrapper import SPARQLWrapper, JSON, POSTDIRECTLY
# from prefixcommons import contract_uri
from kgx.utils.rdf_utils import make_curie
from itertools import zip_longest
from .rdf_transformer import RdfTransformer

import re

def uncamel_case(s):
    """
    https://stackoverflow.com/a/1176023/4750502
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).lower()

class SparqlTransformer(RdfTransformer):
    """
    Transformer for SPARQL endpoints
    """

    edge_query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT * WHERE {
        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{predicate}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{subject_category}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{object_category}} .
        {{/object_category}}

        ?subject ?predicate ?object .
    }

    """

    def __init__(self, graph, url):
        """
        Set URL for the SPARQL endpoint
        """
        super().__init__(graph)
        # TODO: overriding filters to be a dictionary here; this needs to be fixed upstream
        self.filters = {}
        self.url = url

    def load_edges(self):
        """
        Fetch triples from the SPARQL endpoint and load them as edges
        """
        filters = self.get_filters()
        q = render(self.edge_query, filters)
        results = self.query(q)
        for r in results:
            # make_curie_result(r)
            if r['object']['type'] == 'literal':
                self.add_node_attribute(
                    r['subject']['value'],
                    key=r['predicate']['value'],
                    value=r['object']['value'],
                )
                # self.graph.add_node(r['subject']['value'], **{r['predicate']['value']: r['object']['value']})
            else:
                self.add_edge(
                    r['subject']['value'],
                    r['object']['value'],
                    r['predicate']['value'],
                )
                # self.graph.add_node(r['subject']['value'])
                # self.graph.add_node(r['object']['value'])
                # self.graph.add_edge(r['subject']['value'], r['object']['value'], **{'relation': r['predicate']['value']})

    def query(self, q):
        """
        Query a SPARQL endpoint
        """
        sparql = SPARQLWrapper(self.url)
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        logging.info("Query: {}".format(q))
        results = sparql.query().convert()
        bindings = results['results']['bindings']
        logging.info("Rows fetched: {}".format(len(bindings)))
        return bindings

    def curiefy_result(self, result):
        """
        Convert subject, predicate and object IRIs to their respective CURIEs, where applicable
        """
        if result['subject']['type'] == 'uri':
            subject_curie = make_curie(result['subject']['value'])
            if subject_curie != result['subject']['value']:
                result['subject']['value'] = subject_curie
                result['subject']['type'] = 'curie'
            else:
                logging.warning("Could not CURIEfy {}".format(result['subject']['value']))

        if result['object']['type'] == 'curie':
            object_curie = make_curie(result['object']['value'])
            if object_curie != result['object']['value']:
                result['object']['value'] = object_curie
                result['object']['type'] = 'curie'
            else:
                logging.warning("Could not CURIEfy {}".format(result['object']['value']))

        predicate_curie = make_curie(result['predicate']['value'])
        if predicate_curie != result['predicate']['value']:
            result['predicate']['value'] = predicate_curie
            result['predicate']['type'] = 'curie'
        else:
            result['predicate']['type'] = 'uri'

        return result

    def get_filters(self):
        """
        gets the current filter map, transforming if necessary
        """
        d = {}
        for k,v in self.filters.items():
            # TODO: use biolink map here
            d[k] = v
        return d

class MonarchSparqlTransformer(SparqlTransformer):
    """
    see neo_transformer for discussion
    """
    # OBAN-specific
    edge_query = """
    SELECT ?subject ?predicate ?object ?prop ?val WHERE {
        ?a a Association: ;
           subject: ?subject ;
           predicate: ?predicate ;
           subject: ?object ;
           ?prop ?val .

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{predicate}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{subject_category}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{object_category}} .
        {{/object_category}}
    }
    """

class RedSparqlTransformer(SparqlTransformer):
    """

    """

    count_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>

    SELECT (COUNT(*) AS ?triples)
    WHERE {

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{predicate}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{subject_category}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{object_category}} .
        {{/object_category}}

        ?a rdf:type {{association}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    """

    edge_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>

    SELECT ?subject ?predicate ?object ?edge_property_key ?edge_property_value
    WHERE {

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{predicate}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{subject_category}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{object_category}} .
        {{/object_category}}

        ?a rdf:type {{association}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    ORDER BY ?subject ?predicate ?object
    OFFSET {{offset}}
    LIMIT {{limit}}
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

    """


    IS_DEFINED_BY = "Team Red"

    def __init__(self, graph=None, url='http://graphdb.dumontierlab.com/repositories/ncats-red-kg'):
        super().__init__(graph, url)
        self.rdf_graph = Graph()

    def load_networkx_graph():
        """
        Must implement this method to extend abstract RdfTransformer. We don't
        want to actually do anything here.
        TODO: refactor the desired methods out of RdfTransformer so that this
        is not needed.
        """
        pass

    def load_edges(self, association = 'bl:ChemicalToGeneAssociation', limit=None):
        sparql = SPARQLWrapper(self.url)
        query = render(self.count_query, {'association': association})
        logging.debug(query)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        count = int(results['results']['bindings'][0]['triples']['value'])
        logging.info("Expected triples for query: {}".format(count))
        step = 1000
        start = 0
        for i in range(step, count + step, step):
            end = i
            query = render(self.edge_query, {'association': association, 'offset': start, 'limit':step})
            sparql.setQuery(query)
            results = sparql.query().convert()
            node_list = set()
            for r in results['results']['bindings']:
                node_list.add("<{}>".format(r['subject']['value']))
                node_list.add("<{}>".format(r['object']['value']))
            start = end
            self.load_nodes(node_list)
            logging.info("Fetching edges...")
            map = {}
            for r in results['results']['bindings']:
                s = r['subject']['value']
                p = r['predicate']['value']
                o = r['object']['value']
                self.add_edge(s, o, p)
                continue

                # make_curie_result(r)
                key = ((r['subject']['value'], r['object']['value']), r['predicate']['value'])
                if key in map:
                    # seen this triple before. look at properties
                    edge_property_key = r['edge_property_key']
                    edge_property_key_curie = make_curie(edge_property_key['value'])
                    if edge_property_key_curie.startswith('bl:'):
                        edge_property_key_curie = edge_property_key_curie.split(':')[1]
                    edge_property_value = r['edge_property_value']
                    if edge_property_value['type'] == 'uri':
                        edge_property_value_curie = make_curie(edge_property_value['value'])
                    else:
                        edge_property_value_curie = edge_property_value['value']
                    map[key][edge_property_key_curie] = edge_property_value_curie
                else:
                    map[key] = {}
                    edge_property_key = r['edge_property_key']
                    edge_property_key_curie = make_curie(edge_property_key['value'])
                    if edge_property_key_curie.startswith('bl:'):
                        edge_property_key_curie = edge_property_key_curie.split(':')[1]

                    edge_property_value = r['edge_property_value']
                    if edge_property_value['type'] == 'uri':
                        edge_property_value_curie = make_curie(edge_property_value['value'])
                    else:
                        edge_property_value_curie = edge_property_value['value']
                    map[key][edge_property_key_curie] = edge_property_value_curie

            logging.info("Loading edges...")
            for key, properties in map.items():
                self.graph.add_node(key[0][0])
                self.graph.add_node(key[0][1])
                if 'is_defined_by' not in properties and self.IS_DEFINED_BY:
                    properties['is_defined_by'] = self.IS_DEFINED_BY
                if key[1].startswith('bl:'):
                    relation = key[1].split(':')[1]
                else:
                    relation = key[1]
                properties['edge_label'] = relation
                if 'relation' not in properties:
                    properties['relation'] = relation
                self.graph.add_edge(key[0][0], key[0][1], **properties)
            map.clear()

            if limit is not None and i > limit:
                break

        self.set_categories()

    def set_categories(self):
        for n, data in self.graph.nodes(data=True):
            if 'category' not in data and 'type' in data:
                data['category'] = uncamel_case(data['type'].replace('biolink:', ''))

    def load_nodes(self, node_list):
        logging.info("Loading nodes...")
        node_generator = grouper(node_list, 10000)
        nodes = next(node_generator, None)
        while nodes is not None:
            nodes = filter(None, nodes)
            query = self.get_node_properties_query.format(curie_list=' '.join(nodes))
            logging.info(query)
            sparql = SPARQLWrapper(self.url)
            sparql.setRequestMethod(POSTDIRECTLY)
            sparql.setMethod("POST")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            node_results = sparql.query().convert()
            d = {}
            for r in node_results['results']['bindings']:
                if r['object']['type'] != 'bnode':
                    # make_curie_result(r)
                    subject = r['subject']['value']
                    object = r['object']['value']
                    predicate = r['predicate']['value']
                    if predicate.startswith('bl:'):
                        predicate = predicate.split(':')[1]
                    if subject not in d:
                        d[subject] = {}
                    d[subject][predicate] = object

            for node, attr_dict in d.items():
                for key, value in attr_dict.items():
                    self.add_node_attribute(node, key=key, value=value)
                # self.graph.add_node(k, **v)
            d.clear()
            nodes = next(node_generator, None)

def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks"
    """
    yield from zip_longest(*[iter(iterable)] * n, fillvalue=fillvalue)
