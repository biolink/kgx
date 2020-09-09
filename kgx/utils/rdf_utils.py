from collections import OrderedDict
from typing import List, Optional
import rdflib
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS

from kgx.config import get_logger
from kgx.utils.graph_utils import get_category_via_superclass
from kgx.utils.kgx_utils import get_curie_lookup_service, contract
import uuid

log = get_logger()

OBAN = Namespace('http://purl.org/oban/')
BIOLINK = Namespace('https://w3id.org/biolink/vocab/')
OIO = Namespace('http://www.geneontology.org/formats/oboInOwl#')
OBO = Namespace('http://purl.obolibrary.org/obo/')

property_mapping: OrderedDict = OrderedDict()
reverse_property_mapping: OrderedDict = OrderedDict()

# TODO: this should be populated via bmt
is_property_multivalued = {
    'id': False,
    'subject': False,
    'object': False,
    'edge_label': False,
    'description': False,
    'synonym': True,
    'in_taxon': False,
    'same_as': True,
    'name': False,
    'has_evidence': False,
    'provided_by': True,
    'category': True,
    'publications': True,
    'type': False,
    'relation': False
}


top_level_terms = {
    OBO.term('CL_0000000'): 'cell',
    OBO.term('UBERON_0001062'): 'anatomical_entity',
    OBO.term('PATO_0000001'): 'quality',
    OBO.term('NCBITaxon_131567'): 'organism',
    OBO.term('CLO_0000031'): 'cell_line',
    OBO.term('MONDO_0000001'): 'disease',
    OBO.term('CHEBI_23367'): 'molecular_entity',
    OBO.term('CHEBI_23888'): 'drug',
    OBO.term('UPHENO_0001001'): 'phenotypic_feature',
    OBO.term('GO_0008150'): 'biological_process',
    OBO.term('GO_0009987'): 'cellular_process',
    OBO.term('GO_0005575'): 'cellular_component',
    OBO.term('GO_0003674'): 'molecular_function',
    OBO.term('SO_0000704'): 'gene',
    OBO.term('GENO_0000002'): 'variant_locus',
    OBO.term('GENO_0000536'): 'genotype',
    OBO.term('SO_0000110'): 'sequence_feature',
    OBO.term('ECO_0000000'): 'evidence',
    OBO.term('PW_0000001'): 'pathway',
    OBO.term('IAO_0000310'): 'publication',
    OBO.term('SO_0001483'): 'snv',
    OBO.term('GENO_0000871'): 'haplotype',
    OBO.term('SO_0001024'): 'haplotype',
    OBO.term('SO_0000340'): 'chromosome',
    OBO.term('SO_0000104'): 'protein',
    OBO.term('SO_0001500'): 'phenotypic_marker',
    OBO.term('SO_0000001'): 'region',
    OBO.term('HP_0032223'): 'blood_group',
    OBO.term('HP_0031797'): 'clinical_course',
    OBO.term('HP_0040279'): 'frequency',
    OBO.term('HP_0000118'): 'phenotypic_abnormality',
    OBO.term('HP_0032443'): 'past_medical_history',
    OBO.term('HP_0000005'): 'mode_of_inheritance',
    OBO.term('HP_0012823'): 'clinical_modifier'
}


def generate_uuid():
    """
    Generates a UUID.

    Returns
    -------
    str
        A UUID

    """
    return f"urn:uuid:{uuid.uuid4()}"


def infer_category(iri: URIRef, rdfgraph:rdflib.Graph) -> Optional[List]:
    """
    Infer category for a given iri by traversing rdfgraph.

    Parameters
    ----------
    iri: rdflib.term.URIRef
        IRI
    rdfgraph: rdflib.Graph
        A graph to traverse

    Returns
    -------
    Optional[List]
        A list of category corresponding to the given IRI

    """
    closure = list(rdfgraph.transitive_objects(iri, URIRef(RDFS.subClassOf)))
    category = [top_level_terms[x] for x in closure if x in top_level_terms.keys()]
    if category:
        log.debug("Inferred category as {} based on transitive closure over 'subClassOf' relation".format(category))
    else:
        subj = closure[-1]
        if subj == iri:
            return category
        subject_curie: Optional[str] = contract(subj)
        if subject_curie and '_' in subject_curie:
            fixed_curie = subject_curie.split(':', 1)[1].split('_', 1)[1]
            log.warning("Malformed CURIE {} will be fixed to {}".format(subject_curie, fixed_curie))
            subject_curie = fixed_curie
        cls = get_curie_lookup_service()
        category = list(get_category_via_superclass(cls.ontology_graph, subject_curie))
    return category
