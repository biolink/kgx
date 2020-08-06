import logging
from collections import OrderedDict
from typing import List, Union, Optional
import rdflib
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS
from kgx.utils.graph_utils import get_category_via_superclass
from kgx.utils.kgx_utils import get_curie_lookup_service, contract
import uuid


OBAN = Namespace('http://purl.org/oban/')
BIOLINK = Namespace('https://w3id.org/biolink/vocab/')
OIO = Namespace('http://www.geneontology.org/formats/oboInOwl#')
OBO = Namespace('http://purl.obolibrary.org/obo/')

# TODO: any biolink model property should be accounted for
property_mapping = OrderedDict({
    OBAN.association_has_subject: 'subject',
    BIOLINK.subject: 'subject',
    OBAN.association_has_object: 'object',
    BIOLINK.object: 'object',
    OBAN.association_has_predicate: 'predicate',
    BIOLINK.predicate: 'predicate',
    BIOLINK.edge_label: 'edge_label',
    BIOLINK.category: 'category',
    BIOLINK.relation: 'relation',
    BIOLINK.name: 'name',
    RDFS.label: 'name',
    RDFS.comment: 'comment',
    RDF.type: 'type',
    URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'): 'type',
    URIRef('http://purl.obolibrary.org/obo/IAO_0000115'): 'description',
    URIRef('http://purl.org/dc/elements/1.1/description'): 'description',
    BIOLINK.description: 'description',
    BIOLINK.has_evidence: 'has_evidence',
    URIRef('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym'): 'synonym',
    BIOLINK.synonym: 'synonym',
    OWL.sameAs: 'same_as',
    OWL.equivalentClass: 'same_as',
    URIRef('http://purl.obolibrary.org/obo/RO_0002162'): 'in_taxon',
    BIOLINK.in_taxon: 'in_taxon',
    BIOLINK.provided_by: 'provided_by',
    OIO.source: 'source',
    OIO.inSubset: 'subsets',
    OIO.hasDbXref: 'xrefs',
    OIO.hasAlternativeId: 'xrefs',
    OIO.hasNarrowSynonym: 'synonym',
    OIO.hasBroadSynonym: 'synonym',
    OIO.hasRelatedSynonym: 'synonym',
    SKOS.narrowMatch: 'same_as',
    SKOS.broadMatch: 'same_as',
    SKOS.exactMatch: 'same_as',
    SKOS.closeMatch: 'same_as'
})

reverse_property_mapping = {}
for k, v in property_mapping.items():
    reverse_property_mapping[v] = k

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


def infer_category(iri: URIRef, rdfgraph:rdflib.Graph) -> List[str]:
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
    List[str]
        A list of category corresponding to the given IRI

    """
    category = None
    subj = None
    closure = list(rdfgraph.transitive_objects(iri, URIRef(RDFS.subClassOf)))
    category = [top_level_terms[x] for x in closure if x in top_level_terms.keys()]
    if category:
        logging.debug("Inferred category as {} based on transitive closure over 'subClassOf' relation".format(category))
    else:
        subj = closure[-1]
        if subj == iri:
            return category
        subject_curie = contract(subj)
        if '_' in subject_curie:
            fixed_curie = subject_curie.split(':', 1)[1].split('_', 1)[1]
            logging.warning("Malformed CURIE {} will be fixed to {}".format(subject_curie, fixed_curie))
            subject_curie = fixed_curie
        cls = get_curie_lookup_service()
        category = get_category_via_superclass(cls.ontology_graph, subject_curie)
    return category
