from collections import OrderedDict
from typing import List, Optional, Any, Union, Dict, Tuple
import rdflib
from linkml_runtime.linkml_model.meta import Element, SlotDefinition, ClassDefinition
from cachetools import cached, LRUCache
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, SKOS

from kgx.config import get_logger
from kgx.prefix_manager import PrefixManager
from kgx.utils.graph_utils import get_category_via_superclass
from kgx.utils.kgx_utils import (
    get_curie_lookup_service,
    contract,
    get_cache,
    get_toolkit,
    sentencecase_to_snakecase,
    sentencecase_to_camelcase,
    get_biolink_ancestors,
)
from pprint import pprint
log = get_logger()

OBAN = Namespace("http://purl.org/oban/")
BIOLINK = Namespace("https://w3id.org/biolink/vocab/")
OIO = Namespace("http://www.geneontology.org/formats/oboInOwl#")
OBO = Namespace("http://purl.obolibrary.org/obo/")

property_mapping: Dict = dict()
reverse_property_mapping: Dict = dict()

is_property_multivalued = {
    "id": False,
    "subject": False,
    "object": False,
    "predicate": False,
    "description": False,
    "synonym": True,
    "exact_synonym": True,
    "narrow_synonym": True,
    "relation_synonym": True,
    "broad_synonym": True,
    "in_taxon": False,
    "same_as": True,
    "name": False,
    "has_evidence": False,
    "provided_by": True,
    "category": True,
    "publications": True,
    "type": False,
    "relation": False,
}


top_level_terms = {
    OBO.term("CL_0000000"): "cell",
    OBO.term("UBERON_0001062"): "anatomical_entity",
    OBO.term("PATO_0000001"): "quality",
    OBO.term("NCBITaxon_131567"): "organism",
    OBO.term("CLO_0000031"): "cell_line",
    OBO.term("MONDO_0000001"): "disease",
    OBO.term("CHEBI_23367"): "molecular_entity",
    OBO.term("CHEBI_23888"): "drug",
    OBO.term("UPHENO_0001001"): "phenotypic_feature",
    OBO.term("GO_0008150"): "biological_process",
    OBO.term("GO_0009987"): "cellular_process",
    OBO.term("GO_0005575"): "cellular_component",
    OBO.term("GO_0003674"): "molecular_function",
    OBO.term("SO_0000704"): "gene",
    OBO.term("GENO_0000002"): "variant_locus",
    OBO.term("GENO_0000536"): "genotype",
    OBO.term("SO_0000110"): "sequence_feature",
    OBO.term("ECO_0000000"): "evidence",
    OBO.term("PW_0000001"): "pathway",
    OBO.term("IAO_0000310"): "publication",
    OBO.term("SO_0001483"): "snv",
    OBO.term("GENO_0000871"): "haplotype",
    OBO.term("SO_0001024"): "haplotype",
    OBO.term("SO_0000340"): "chromosome",
    OBO.term("SO_0000104"): "protein",
    OBO.term("SO_0001500"): "phenotypic_marker",
    OBO.term("SO_0000001"): "region",
    OBO.term("HP_0032223"): "blood_group",
    OBO.term("HP_0031797"): "clinical_course",
    OBO.term("HP_0040279"): "frequency",
    OBO.term("HP_0000118"): "phenotypic_abnormality",
    OBO.term("HP_0032443"): "past_medical_history",
    OBO.term("HP_0000005"): "mode_of_inheritance",
    OBO.term("HP_0012823"): "clinical_modifier",
}


def infer_category(iri: URIRef, rdfgraph: rdflib.Graph) -> Optional[List]:
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
        log.debug(
            "Inferred category as {} based on transitive closure over 'subClassOf' relation".format(
                category
            )
        )
    else:
        subj = closure[-1]
        if subj == iri:
            return category
        subject_curie: Optional[str] = contract(subj)
        if subject_curie and "_" in subject_curie:
            fixed_curie = subject_curie.split(":", 1)[1].split("_", 1)[1]
            log.warning(
                "Malformed CURIE {} will be fixed to {}".format(
                    subject_curie, fixed_curie
                )
            )
            subject_curie = fixed_curie
        cls = get_curie_lookup_service()
        category = list(get_category_via_superclass(cls.ontology_graph, subject_curie))
    return category


@cached(LRUCache(maxsize=1024))
def get_biolink_element(
    prefix_manager: PrefixManager, predicate: Any
) -> Optional[Element]:
    """
    Returns a Biolink Model element for a given predicate.

    Parameters
    ----------
    prefix_manager: PrefixManager
        An instance of prefix manager
    predicate: Any
        The CURIE of a predicate

    Returns
    -------
    Optional[Element]
        The corresponding Biolink Model element

    """
    toolkit = get_toolkit()
    element = None
    reference = None
    if prefix_manager.is_iri(predicate):
        predicate_curie = prefix_manager.contract(predicate)
    else:
        predicate_curie = predicate
    if prefix_manager.is_curie(predicate_curie):
        element = toolkit.get_element(predicate_curie)
        if element is None:
            reference = prefix_manager.get_reference(predicate_curie)
    else:
        reference = predicate_curie
    if element is None and reference is not None:
        element = toolkit.get_element(reference)
    if not element:
        try:
            mapping = toolkit.get_element_by_mapping(predicate)
            if mapping:
                element = toolkit.get_element(mapping)
        except ValueError as e:
            log.error(e)
    return element


def process_predicate(
    prefix_manager: PrefixManager,
    p: Union[URIRef, str],
    predicate_mapping: Optional[Dict] = None,
) -> Tuple:
    """
    Process a predicate where the method checks if there is a mapping in Biolink Model.

    Parameters
    ----------
    prefix_manager: PrefixManager
        An instance of prefix manager
    p: Union[URIRef, str]
        The predicate
    predicate_mapping: Optional[Dict]
        Predicate mappings

    Returns
    -------
    Tuple[str, str, str, str]
        A tuple that contains the Biolink CURIE (if available), the Biolink slot_uri CURIE (if available),
        the CURIE form of p, the reference of p

    """
    if prefix_manager.is_iri(p):
        predicate = prefix_manager.contract(str(p))
    else:
        predicate = None
    if prefix_manager.is_curie(p):
        property_name = prefix_manager.get_reference(p)
        predicate = p
    else:
        if predicate and prefix_manager.is_curie(predicate):
            property_name = prefix_manager.get_reference(predicate)
        else:
            property_name = p
            predicate = f":{p}"
    element = get_biolink_element(prefix_manager, p)
    canonical_uri = None
    if element is None:
        element = get_biolink_element(prefix_manager, predicate)
    if element:
        if isinstance(element, SlotDefinition):
            # predicate corresponds to a biolink slot
            if element.definition_uri:
                element_uri = prefix_manager.contract(element.definition_uri)
            else:
                element_uri = f"biolink:{sentencecase_to_snakecase(element.name)}"
                canonical_uri = element_uri
            if element.slot_uri:
                canonical_uri = element.slot_uri
        elif isinstance(element, ClassDefinition):
            # this will happen only when the IRI is actually
            # a reference to a class
            element_uri = prefix_manager.contract(element.class_uri)
        else:
            element_uri = f"biolink:{sentencecase_to_camelcase(element.name)}"
        if "biolink:Attribute" in get_biolink_ancestors(element.name):
            element_uri = f"biolink:{sentencecase_to_snakecase(element.name)}"
        if not predicate:
            predicate = element_uri
    else:
        # no mapping to biolink model;
        # look at predicate mappings
        element_uri = None
        if predicate_mapping:
            if p in predicate_mapping:
                property_name = predicate_mapping[p]
                predicate = f":{property_name}"
    return element_uri, canonical_uri, predicate, property_name
