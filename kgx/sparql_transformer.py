import logging
from .transformer import Transformer
from pystache import render
from SPARQLWrapper import SPARQLWrapper, JSON
from prefixcommons import contract_uri

class SparqlTransformer(Transformer):
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
            self.curiefy(r)
            if r['object']['type'] == 'literal':
                self.graph.add_node(r['subject']['value'], attr_dict={r['predicate']['value']: r['object']['value']})
            elif r['object']['type'] == 'bnode':
                continue
            else:
                self.graph.add_node(r['subject']['value'])
                self.graph.add_node(r['object']['value'])
                self.graph.add_edge(r['subject']['value'], r['object']['value'], attr_dict={'relation': r['predicate']['value']})

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
        
    def curiefy(self, result):
        """
        Convert subject, predicate and object IRIs to their respective CURIEs, where applicable
        """
        if result['subject']['type'] == 'uri':
            subject_curie = contract_uri(result['subject']['value'])
            if len(subject_curie) != 0:
                result['subject']['value'] = subject_curie[0]
                result['subject']['type'] = 'curie'
            else:
                logging.warning("Could not CURIEfy {}".format(result['subject']['value']))

        if result['object']['type'] == 'curie':
            object_curie = contract_uri(result['object']['value'])
            if len(object_curie) != 0:
                result['object']['value'] = object_curie[0]
                result['object']['type'] = 'curie'
            else:
                logging.warning("Could not CURIEfy {}".format(result['object']['value']))

        predicate_curie = contract_uri(result['predicate']['value'])
        result['predicate']['value'] = predicate_curie[0]
        result['predicate']['type'] = 'curie'

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
