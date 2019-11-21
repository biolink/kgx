from collections import defaultdict
from typing import List, Tuple

from networkx import MultiDiGraph

from kgx.utils.kgx_utils import get_toolkit
from kgx.utils.rdf_utils import make_curie

toolkit = get_toolkit()

iri_mapping = {
# subclasses mapped onto their superclasses:
    "http://purl.obolibrary.org/obo/SO_0000405" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000001" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000100" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000336" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000340" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000404" : "transcript",
    "http://purl.obolibrary.org/obo/SO_0000460" : "sequence feature",
    "http://purl.obolibrary.org/obo/SO_0000651" : "transcript",
    "http://purl.obolibrary.org/obo/SO_0000655" : "transcript",
#?
    "http://purl.obolibrary.org/obo/SO_0001217" : "gene",
    "http://purl.obolibrary.org/obo/GENO_0000002" : "sequence variant",
    'http://purl.obolibrary.org/obo/UPHENO_0001002' : 'phenotypic feature',
# Taken from the yaml
    "http://purl.obolibrary.org/obo/CL_0000000" : "cell",
    "http://purl.obolibrary.org/obo/UBERON_0001062" : "anatomical entity",
    "http://purl.obolibrary.org/obo/ZFA_0009000" : "cell",
    "http://purl.obolibrary.org/obo/UBERON_0004529" : "anatomical projection",
    "http://purl.obolibrary.org/obo/UBERON_0000468" : "multi-cellular organism",
    "http://purl.obolibrary.org/obo/UBERON_0000955" : "brain",
    "http://purl.obolibrary.org/obo/PATO_0000001" : "quality",
    "http://purl.obolibrary.org/obo/GO_0005623" : "cell",
    "http://purl.obolibrary.org/obo/WBbt_0007833" : "organism",
    "http://purl.obolibrary.org/obo/WBbt_0004017" : "cell",
    "http://purl.obolibrary.org/obo/MONDO_0000001" : "disease",
    "http://purl.obolibrary.org/obo/PATO_0000003" : "assay",
    "http://purl.obolibrary.org/obo/PATO_0000006" : "process",
    "http://purl.obolibrary.org/obo/PATO_0000011" : "age",
    "http://purl.obolibrary.org/obo/ZFA_0000008" : "brain",
    "http://purl.obolibrary.org/obo/ZFA_0001637" : "bony projection",
    "http://purl.obolibrary.org/obo/WBPhenotype_0000061" : "extended life span",
    "http://purl.obolibrary.org/obo/WBPhenotype_0000039" : "life span variant",
    "http://purl.obolibrary.org/obo/WBPhenotype_0001171" : "shortened life span",
    "http://purl.obolibrary.org/obo/CHEBI_23367" : "molecular entity",
    "http://purl.obolibrary.org/obo/CHEBI_23888" : "drug",
    "http://purl.obolibrary.org/obo/CHEBI_51086" : "chemical role",
    "http://purl.obolibrary.org/obo/UPHENO_0001001" : "phenotypic feature",
    "http://purl.obolibrary.org/obo/GO_0008150" : "biological_process",
    "http://purl.obolibrary.org/obo/GO_0005575" : "cellular component",
    "http://purl.obolibrary.org/obo/SO_0000704" : "gene",
    "http://purl.obolibrary.org/obo/SO_0000110" : "sequence feature",
    "http://purl.obolibrary.org/obo/GENO_0000536" : "genotype",
}

mapping = defaultdict(set)

for key, value in iri_mapping.items():
    mapping[make_curie(key)].add(value)

for key, value in toolkit.generator.mappings.items():
    mapping[key].update(value)

def walk(node, next_node_generator):
    to_visit = {node : 0} # Dict[URIRef, Integer]
    visited = {} # Dict[URIRef, Integer]

    while to_visit != {}:
        m, score = to_visit.popitem()
        visited[m] = score
        for t in next_node_generator(m):
            if isinstance(t, tuple) and len(t) > 1:
                n, s = t
            else:
                n, s = t, 0
            if n not in visited:
                to_visit[n] = score + s
                yield n, to_visit[n]

def find_categories(node, graph:MultiDiGraph) -> List[str]:
    """
    Finds a category for the given node by walking up `subclass_of` edges and
    walking across `same_as` edges.

    Tries to get a category in mapping or one whose name is a class in the
    biolink model. If no such categories are found then takes the name or id of
    a highest superclass.
    """
    def super_class_generator(n) -> Tuple[str, int]:
        for _, m, data in graph.out_edges(n, data=True):
            edge_label = data.get('edge_label')
            if edge_label is None:
                continue
            elif edge_label == 'same_as':
                yield m, 0
            elif edge_label == 'subclass_of':
                yield m, 1

        for m, _, data in graph.in_edges(n, data=True):
            edge_label = data.get('edge_label')
            if edge_label is None:
                continue
            elif data['edge_label'] == 'same_as':
                yield m, 0

    best_node, best_score = None, 0
    for node, score in walk(iri, super_class_generator):
        name = graph.node[node].get('name')
        c = toolkit.get_element(name)
        if c is not None:
            return [c.name]
        elif node in mapping and score > 0:
            return list(mapping[node])
        elif score > best_score:
            best_node, best_score = node, score

    name = graph.node[best_node].get('name')

    if name is not None:
        return [name]
    else:
        return [best_node]
