import networkx as nx
import rdflib
from kgx.config import get_logger, get_config
from kgx.utils.kgx_utils import generate_edge_key, contract

CURIE_MAP = {"BFO:0000054": "realized_in", "RO:0000091": "has_disposition"}

log = get_logger()


class CurieLookupService(object):
    """
    A service to lookup label for a given CURIE.
    """

    config = get_config()
    ontologies = config['ontologies'] if 'ontologies' in config else {}
    ontology_graph = None

    def __init__(self, curie_map: dict = None):
        if curie_map:
            self.curie_map = CURIE_MAP
            self.curie_map.update(curie_map)
        else:
            self.curie_map = CURIE_MAP
        self.ontology_graph = nx.MultiDiGraph()
        self.load_ontologies()

    def load_ontologies(self):
        """
        Load all required ontologies.
        """
        for ontology in self.ontologies.values():
            rdfgraph = rdflib.Graph()
            input_format = rdflib.util.guess_format(ontology)
            rdfgraph.parse(ontology, format=input_format)
            # triples = rdfgraph.triples((None, rdflib.RDFS.subClassOf, None))
            # for s,p,o in triples:
            #     subject_curie = contract(s)
            #     object_curie = contract(o)
            #     self.ontology_graph.add_node(subject_curie)
            #     self.ontology_graph.add_node(object_curie)
            #     key = generate_edge_key(subject_curie, 'subclass_of', object_curie)
            #     self.ontology_graph.add_edge(subject_curie, object_curie, key, **{'predicate': 'subclass_of', 'relation': 'rdfs:subClassOf'})

            triples = rdfgraph.triples((None, rdflib.RDFS.label, None))
            for s, p, o in triples:
                key = contract(s)
                value = o.value
                value = value.replace(" ", "_")
                self.curie_map[key] = value
                self.ontology_graph.add_node(key, name=value)
