from SPARQLWrapper import SPARQLWrapper, JSON
from kgx.prefix_manager import PrefixManager
import logging
from collections import defaultdict

class SparqlMapper(object):

    def __init__(self):
        self.prefix_manager = PrefixManager()
        self.prefix_manager.fallback = False
    
    def query(self, q):
        sparql = SPARQLWrapper("http://sparql.hegroup.org/sparql")
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        logging.info("Query: {}".format(q))
        results = sparql.query().convert()
        bindings = results['results']['bindings']
        logging.info("Rows: {}".format(len(bindings)))
        return bindings

    def map_uris(self):
        parentmap = self.get_ro_parentmap()
        uris = parentmap.keys()
        m = {}
        for uri in uris:
            shortform = self.map_uri(parentmap, uri)
            if shortform is not None and ':' not in shortform:
                m[uri] = shortform
        return m
    
    def map_uri(self, parentmap, uri):
        pm = self.prefix_manager
        nodes = [uri]
        while len(nodes) > 0:
            next_uri = nodes.pop()
            shortform = pm.contract(next_uri)
            if shortform is not None:
                return shortform
            if next_uri in parentmap:
                nodes += list(parentmap[next_uri])
        return None

    def get_ro_parentmap(self):
        query = """
        SELECT ?sub ?super WHERE {{
        GRAPH <http://purl.obolibrary.org/obo/merged/RO>  {{
        ?sub rdfs:subPropertyOf ?super 
        }}
        }}
        """
        m = defaultdict(set)
        for row in self.query(query):
            print("Row={}".format(row))
            m[row['sub']['value']].add(row['super']['value'])
        return m
