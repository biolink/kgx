import networkx as nx
import rdflib

#from kgx.utils.rdf_utils import make_curie
from prefixcommons import contract_uri
from prefixcommons.curie_util import default_curie_maps
from rdflib import URIRef

CURIE_MAP = {
    'BFO:0000054': 'realized_in',
    'RO:0000091': 'has_disposition'
}


class CurieLookupService(object):
    """

    """

    ontologies = {
        'RO': 'data/ro.owl',
        'BFO': 'data/bfo.owl',
        'HP': 'data/hp.owl',
        'GO': 'data/go.owl'
    }
    ontology_graph = None

    def __init__(self, curie_map=None):
        if curie_map:
            self.curie_map = curie_map
        else:
            self.curie_map = CURIE_MAP
        self.ontology_graph = nx.MultiDiGraph()
        self.load_ontologies()

    def load_ontologies(self):
        for ontology in self.ontologies.values():
            rdfgraph = rdflib.Graph()
            input_format = rdflib.util.guess_format(ontology)
            rdfgraph.parse(ontology, format=input_format)
            triples = rdfgraph.triples((None, rdflib.RDFS.subClassOf, None))
            for s,p,o in triples:
                subject_curie = make_curie(s)
                object_curie = make_curie(o)
                self.ontology_graph.add_node(subject_curie)
                self.ontology_graph.add_node(object_curie)
                key = '{}-{}-{}'.format(subject_curie, 'subclass_of', object_curie)
                self.ontology_graph.add_edge(subject_curie, object_curie, key, **{'edge_label': 'subclass_of', 'relation': 'rdfs:subClassOf'})

            triples = rdfgraph.triples((None, rdflib.RDFS.label, None))
            for s,p,o in triples:
                key = make_curie(s)
                value = o.value
                value = value.replace(' ', '_')
                self.curie_map[key] = value
                self.ontology_graph.add_node(key, name=value)

cmaps = [
            {
                'OMIM': 'https://omim.org/entry/',
                'HGNC': 'http://identifiers.org/hgnc/',
                'DRUGBANK': 'http://identifiers.org/drugbank:',
                'biolink': 'http://w3id.org/biolink/vocab/'
            },
            {
                'DRUGBANK': 'http://w3id.org/data2services/data/drugbank/'
            }
        ] + default_curie_maps


def contract(uri) -> str:
    """
    We sort the curies to ensure that we take the same item every time
    """
    curies = contract_uri(str(uri), cmaps=cmaps)
    if len(curies) > 0:
        curies.sort()
        return curies[0]
    return None

def make_curie(uri) -> str:
    HTTP = 'http'
    HTTPS = 'https'

    curie = contract(uri)

    if curie is not None:
        return curie

    if uri.startswith(HTTPS):
        uri = HTTP + uri[len(HTTPS):]
    elif uri.startswith(HTTP):
        uri = HTTPS + uri[len(HTTP):]

    curie = contract(uri)

    if curie is None:
        return uri
    else:
        return curie