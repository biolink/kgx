import logging
from .transformer import Transformer
from pystache import render
from SPARQLWrapper import SPARQLWrapper, JSON

registry = {
    'monarch': 'http://rdf.monarchinitiative.org/sparql/'
}

# NOTE: this is currently OBAN-specific
edge_q="""
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

class SparqlTransformer(Transformer):
    """
    Transformer for SPARQL endpoints
    """

    def load_edges(self):
        filter = self.get_filter()
        q = render(edge_q, filter)
        results = self.query(q)

    def query(self, q):
        #sparql = SPARQLWrapper(registry.get(self.endpoint))
        sparql = SPARQLWrapper()
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        logging.info("Query: {}".format(q))
        results = sparql.query().convert()
        bindings = results['results']['bindings']
        logging.info("Rows: {}".format(len(bindings)))
        for r in bindings:
            self.curiefy(r)
        return bindings
        


    def get_filter(self):
        """
        gets the current filter map, transforming if necessary
        """
        d = {}
        for k,v in self.filter.items():
            # TODO: use biolink map here
            d[k] = v
        return d
    
class MonarchSparqlTransformer(SparqlTransformer):
    """
    see neo_transformer for discussion
    """
    
