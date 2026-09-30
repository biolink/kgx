import gzip
import re
import typing
from itertools import chain
from typing import Optional, Tuple, Dict, Generator, Any
import ijson
import stringcase
import inflection
from bmt import Toolkit
from kgx.error_detection import ErrorType, MessageLevel
from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger
from kgx.source.json_source import JsonSource
from kgx.utils.kgx_utils import get_biolink_element, format_biolink_slots

log = get_logger()


class ObographSource(JsonSource):
    """
    ObographSource is responsible for reading data as records
    from an OBO Graph JSON.
    """

    HAS_OBO_NAMESPACE = "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace"
    SKOS_EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"

    # Axiom annotation predicates that carry evidence and provenance. OWL axiom
    # annotations survive an obographs conversion as ``edges[].meta``, which is
    # where ontologies such as MONDO record why an assertion was made.
    PROV_WAS_DERIVED_FROM = "http://www.w3.org/ns/prov#wasDerivedFrom"
    OIO_EVIDENCE = "http://www.geneontology.org/formats/oboInOwl#evidence"
    OIO_IS_INFERRED = "http://www.geneontology.org/formats/oboInOwl#is_inferred"
    SSSOM_PREFIX = "https://w3id.org/sssom/"
    # MONDO writes oboInOwl#source; PHENIO rewrites it to dc:source when it
    # merges components, so both spellings show up in the wild.
    SOURCE_PREDICATES = frozenset(
        {
            "http://purl.org/dc/elements/1.1/source",
            "http://purl.org/dc/terms/source",
            "http://www.geneontology.org/formats/oboInOwl#source",
        }
    )

    # Prefixes of ``source`` values that name a publication rather than a
    # database record or a curator.
    PUBLICATION_PREFIXES = frozenset(
        {"PMID", "PMCID", "PMC", "DOI", "ISBN", "ISSN", "MEDLINE"}
    )
    # Sentinel ``source`` values meaning "a reasoner put this edge here".
    ENTAILMENT_MARKERS = frozenset({"MONDO:Inferred", "MONDO:Entailed"})
    # MONDO overloads ``source`` with curation bookkeeping such as
    # "MONDO:Redundant" and "MONDO:prototype". Those share the MONDO namespace
    # with real term references like "MONDO:0016133-obsoleted", so tell them
    # apart by whether the local part starts with a digit.
    CURATION_MARKER_NAMESPACES = frozenset({"MONDO"})

    # ``prov:wasDerivedFrom`` names the contributing ontology as a release IRI,
    # e.g. ".../phenio/releases/2026-07-07/components/mondo.owl". The file stem
    # is taken as the InfoRes ID as-is; the derivation stays mechanical rather
    # than second-guessing what a project calls a source. Note the InfoRes
    # registry does not agree with every stem (it lists "hpo", not "hp"), so a
    # project that cares should pass ``infores_map``. The one override here is
    # for PHENIO's own merge of its imports, which has no ontology of its own.
    INFORES_OVERRIDES = {
        "merged_import": "phenio",
    }

    # Edge fields that a passthrough annotation must never overwrite.
    RESERVED_EDGE_PROPERTIES = frozenset(
        {
            "id",
            "subject",
            "predicate",
            "object",
            "relation",
            "category",
            "knowledge_source",
            "provided_by",
        }
    )

    # Axiom annotations with no Biolink slot but a well-known meaning; these get
    # a column of their own, named readably rather than after an IAO number.
    # Anything outside this set (and the SSSOM vocabulary) goes to _annotations:
    # across PHENIO's components the tail is long and very sparse -- one-off
    # per-ontology properties like "todo", "quote" and even a misspelled
    # "cardonality" would otherwise each claim a column of their own.
    ANNOTATION_ALIASES = {
        "http://purl.obolibrary.org/obo/IAO_0000233": "term_tracker_item",
        "http://purl.obolibrary.org/obo/IAO_0000116": "editor_note",
        "http://www.w3.org/2000/01/rdf-schema#comment": "comment",
        "http://www.w3.org/2000/01/rdf-schema#seeAlso": "see_also",
        "http://www.geneontology.org/formats/oboInOwl#notes": "notes",
        "http://purl.org/dc/terms/creator": "creator",
        "http://purl.org/dc/elements/1.1/date": "date",
    }

    # Column holding annotations that get no column of their own, as
    # "predicate=value" pairs so the data is still recoverable.
    ANNOTATION_CATCHALL = "_annotations"

    def __init__(self, owner):
        super().__init__(owner)
        self.toolkit = Toolkit()
        self.ecache: Dict = {}
        self.infores_map: Dict = dict(self.INFORES_OVERRIDES)

    def parse(
        self,
        filename: str,
        format: str = "json",
        compression: Optional[str] = None,
        **kwargs: Any,
    ) -> typing.Generator:
        """
        This method reads from JSON and yields records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``json``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """
        self.set_provenance_map(kwargs)
        if "infores_map" in kwargs:
            self.infores_map.update(kwargs["infores_map"])
        n = self.read_nodes(filename, compression)
        e = self.read_edges(filename, compression)
        yield from chain(n, e)

    def read_nodes(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read node records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for node records

        """
        if compression and compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for n in ijson.items(FH, "graphs.item.nodes.item"):
            yield self.read_node(n)

    def read_node(self, node: Dict) -> Optional[Tuple[str, Dict]]:
        """
        Read and parse a node record.

        Parameters
        ----------
        node: Dict
            The node record

        Returns
        -------
        Dict
            The processed node

        """
        curie = self.prefix_manager.contract(node["id"])
        node_properties = {}
        if "meta" in node:
            # Returns a dictionary that contains 'description', 'subsets',
            # 'synonym', 'xrefs', a 'deprecated' flag and/or
            # 'equivalent_nodes', if the corresponding key values are set
            node_properties = self.parse_meta(node["id"], node["meta"])

        fixed_node = dict()
        fixed_node["id"] = curie
        if "lbl" in node:
            fixed_node["name"] = node["lbl"]
        fixed_node["iri"] = node["id"]

        if "description" in node_properties:
            fixed_node["description"] = node_properties["description"]

        if "subsets" in node_properties:
            fixed_node["subsets"] = node_properties["subsets"]

        if "synonym" in node_properties:
            fixed_node["synonym"] = node_properties["synonym"]

        if "exact_synonym" in node_properties:
            fixed_node["exact_synonym"] = node_properties["exact_synonym"]

        if "related_synonym" in node_properties:
            fixed_node["related_synonym"] = node_properties["related_synonym"]

        if "narrow_synonym" in node_properties:
            fixed_node["narrow_synonym"] = node_properties["narrow_synonym"]

        if "broad_synonym" in node_properties:
            fixed_node["broad_synonym"] = node_properties["broad_synonym"]

        if "xrefs" in node_properties:
            fixed_node["xref"] = node_properties["xrefs"]

        if "deprecated" in node_properties:
            fixed_node["deprecated"] = node_properties["deprecated"]

        if "category" not in node:
            category = self.get_category(curie, node)
            if category:
                fixed_node["category"] = [category]
            else:
                fixed_node["category"] = ["biolink:OntologyClass"]

        if "equivalent_nodes" in node_properties:
            equivalent_nodes = node_properties["equivalent_nodes"]
            fixed_node["same_as"] = equivalent_nodes

        return super().read_node(fixed_node)

    def read_edges(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read edge records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for edge records

        """
        if compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for e in ijson.items(FH, "graphs.item.edges.item"):
            yield self.read_edge(e)

    def read_edge(self, edge: Dict) -> Optional[Tuple]:
        """
        Read and parse an edge record.

        Parameters
        ----------
        edge: Dict
            The edge record

        Returns
        -------
        Dict
            The processed edge

        """
        fixed_edge = dict()
        fixed_edge["subject"] = self.prefix_manager.contract(edge["sub"])
        if PrefixManager.is_iri(edge["pred"]):
            curie = self.prefix_manager.contract(edge["pred"])
            if curie in self.ecache:
                edge_predicate = self.ecache[curie]
            else:
                element = get_biolink_element(curie)
                if not element:
                    try:
                        # bmt's get_element_by_mapping recognizes CURIE form
                        # (e.g. "RO:0002162"), not IRIs. The CURIE is computed
                        # above as `curie`; passing the IRI here silently falls
                        # back to biolink:related_to for relations that have a
                        # perfectly good biolink slot mapping.
                        mapping = self.toolkit.get_element_by_mapping(curie)
                        if mapping:
                            element = self.toolkit.get_element(mapping)

                    except ValueError as e:
                        self.owner.log_error(
                            entity=str(edge["pred"]),
                            error_type=ErrorType.INVALID_EDGE_PREDICATE,
                            message=str(e)
                        )
                        element = None

                if element:
                    edge_predicate = format_biolink_slots(element.name.replace(",", ""))
                    fixed_edge["predicate"] = edge_predicate
                else:
                    edge_predicate = "biolink:related_to"
                self.ecache[curie] = edge_predicate
            fixed_edge["predicate"] = edge_predicate
            fixed_edge["relation"] = curie
        else:
            if edge["pred"] == "is_a":
                fixed_edge["predicate"] = "biolink:subclass_of"
                fixed_edge["relation"] = "rdfs:subClassOf"
            elif edge["pred"] == "has_part":
                fixed_edge["predicate"] = "biolink:has_part"
                fixed_edge["relation"] = "BFO:0000051"
            elif edge["pred"] == "part_of":
                fixed_edge["predicate"] = "biolink:part_of"
                fixed_edge["relation"] = "BFO:0000050"
            else:
                fixed_edge["predicate"] = f"biolink:{edge['pred'].replace(' ', '_')}"
                fixed_edge["relation"] = edge["pred"]

        fixed_edge["object"] = self.prefix_manager.contract(edge["obj"])
        if "meta" in edge:
            fixed_edge.update(self.parse_edge_meta(edge["meta"]))
        for x in edge.keys():
            # 'meta' is expanded into Biolink slots by parse_edge_meta above;
            # copying it verbatim would emit the raw dict as a column value.
            if x not in {"sub", "pred", "obj", "meta"}:
                fixed_edge[x] = edge[x]
        return super().read_edge(fixed_edge)

    def parse_edge_meta(self, meta: Dict) -> Dict:
        """
        Parse the 'meta' field of an edge into evidence and provenance properties.

        OWL axiom annotations are carried through an obographs conversion as
        ``edges[].meta``. This maps the ones with Biolink equivalents onto
        association slots (``primary_knowledge_source``, ``publications``,
        ``has_evidence``, ``knowledge_level``, ``agent_type``, ``xref``) and
        passes the rest through under their own names, so nothing is dropped.
        Every ``source`` value is also retained verbatim in ``_source``.

        Parameters
        ----------
        meta: Dict
            meta dictionary for the edge

        Returns
        -------
        Dict
            Edge properties derived from the axiom annotations.

        """
        properties: Dict[str, Any] = {}
        publications = []
        evidence = []
        xrefs = [x["val"] for x in meta.get("xrefs", []) if "val" in x]
        sources = []
        has_curator = False
        entailed = False
        passthrough: Dict[str, list] = {}
        other = []

        for bpv in meta.get("basicPropertyValues", []):
            pred = bpv.get("pred")
            val = bpv.get("val")
            if not pred or val is None:
                continue

            if pred == self.PROV_WAS_DERIVED_FROM:
                infores = self.derive_infores(val)
                if infores:
                    properties["primary_knowledge_source"] = infores
            elif pred in self.SOURCE_PREDICATES:
                sources.append(val)
                kind = self.classify_source(val)
                if kind == "publication":
                    publications.extend(self.split_references(val))
                elif kind == "agent":
                    has_curator = True
                elif kind == "entailment":
                    entailed = True
                elif kind == "xref":
                    xrefs.extend(self.split_references(val))
                # 'marker' values are curation bookkeeping; they stay in
                # '_source' but are not cross-references.
            elif pred == self.OIO_EVIDENCE:
                evidence.append(self.normalize_reference(val))
            elif pred == self.OIO_IS_INFERRED:
                if str(val).lower() == "true":
                    entailed = True
            else:
                key = self.annotation_property_name(pred)
                if key is None:
                    curie = self.prefix_manager.contract(pred) or pred
                    other.append(f"{curie}={val}")
                else:
                    passthrough.setdefault(key, []).append(val)

        if publications:
            properties["publications"] = publications
        if evidence:
            properties["has_evidence"] = evidence
        if xrefs:
            properties["xref"] = xrefs
        if sources:
            properties["_source"] = sources

        # An ORCID on the axiom means a human asserted it; a reasoner marker
        # means it was entailed. When both are present the human wins, since the
        # markers MONDO also emits (Redundant, indirect) describe the axiom's
        # position in the hierarchy rather than how it was arrived at.
        if has_curator:
            properties["agent_type"] = "manual_agent"
            properties["knowledge_level"] = "knowledge_assertion"
        elif entailed:
            properties["agent_type"] = "automated_agent"
            properties["knowledge_level"] = "logical_entailment"
        elif sources:
            properties["knowledge_level"] = "knowledge_assertion"

        for key, values in passthrough.items():
            # An annotation whose local name happens to match a core edge field
            # (oboInOwl#id, say) would otherwise corrupt the record.
            if key in self.RESERVED_EDGE_PROPERTIES or key in properties:
                key = f"annotation_{key}"
            properties[key] = values[0] if len(values) == 1 else values

        if other:
            properties[self.ANNOTATION_CATCHALL] = other

        return properties

    def split_references(self, val: str) -> list:
        """
        Split an annotation value that packs several references into one string.

        A handful of MONDO axioms carry values like
        ``"PMID:32181500, PMID:32905580"``. Splitting only when every resulting
        token still looks like a CURIE leaves ordinary values, including
        ``"PMID: 16322613"``, alone.

        Parameters
        ----------
        val: str
            The annotation value

        Returns
        -------
        list
            One or more normalized references.

        """
        if not PrefixManager.is_iri(val):
            tokens = [t for t in re.split(r"[,;\s]+", val.strip()) if t]
            if len(tokens) > 1 and all(":" in t for t in tokens):
                return [self.normalize_reference(t) for t in tokens]
        return [self.normalize_reference(val)]

    def normalize_reference(self, val: str) -> str:
        """
        Contract an annotation value to a CURIE, tidying stray whitespace.

        MONDO contains hand-entered values such as ``"PMID: 16322613"``; left
        alone the space makes the CURIE unusable downstream.

        Parameters
        ----------
        val: str
            The annotation value

        Returns
        -------
        str
            The value as a CURIE where one could be derived, else the input
            with surrounding whitespace removed.

        """
        val = val.strip()
        if not PrefixManager.is_iri(val):
            prefix, colon, local = val.partition(":")
            if colon:
                val = f"{prefix.strip()}:{local.strip()}"
        return self.prefix_manager.contract(val) or val

    def classify_source(self, val: str) -> str:
        """
        Classify a ``source`` axiom annotation value by its shape.

        Parameters
        ----------
        val: str
            The annotation value

        Returns
        -------
        str
            One of ``publication``, ``agent``, ``entailment``, ``marker`` or
            ``xref``.

        """
        if val.startswith(("https://orcid.org/", "http://orcid.org/", "ORCID:")):
            return "agent"
        if val in self.ENTAILMENT_MARKERS or val.endswith("/inferred"):
            return "entailment"
        prefix, _, local = val.partition(":")
        if prefix.upper() in self.PUBLICATION_PREFIXES:
            return "publication"
        if prefix in self.CURATION_MARKER_NAMESPACES and not local[:1].isdigit():
            return "marker"
        return "xref"

    def derive_infores(self, iri: str) -> Optional[str]:
        """
        Derive an InfoRes CURIE from a ``prov:wasDerivedFrom`` release IRI.

        Parameters
        ----------
        iri: str
            The IRI of the contributing ontology, e.g.
            ``http://purl.obolibrary.org/obo/phenio/releases/2026-07-07/components/mondo.owl``

        Returns
        -------
        Optional[str]
            An InfoRes CURIE such as ``infores:mondo``, or None if no stem
            could be recovered.

        """
        stem = iri.rsplit("/", 1)[-1]
        for extension in (".owl", ".obo", ".json", ".ttl"):
            if stem.endswith(extension):
                stem = stem[: -len(extension)]
                break
        stem = stem.lower()
        if not stem:
            return None
        return f"infores:{self.infores_map.get(stem, stem.replace('_', '-'))}"

    def annotation_property_name(self, pred: str) -> Optional[str]:
        """
        Return the column name for an axiom annotation with no Biolink slot.

        Only the SSSOM vocabulary and the properties named in
        ``ANNOTATION_ALIASES`` earn a column; everything else returns None and
        is folded into ``_annotations`` instead.

        Parameters
        ----------
        pred: str
            The annotation property IRI

        Returns
        -------
        Optional[str]
            A property name, e.g. ``mapping_justification`` for an SSSOM slot
            or ``notes`` for ``oboInOwl#notes``, or None if the annotation
            belongs in the catch-all.

        """
        if pred in self.ANNOTATION_ALIASES:
            return self.ANNOTATION_ALIASES[pred]
        if pred.startswith(self.SSSOM_PREFIX):
            local = pred[len(self.SSSOM_PREFIX) :]
            return inflection.underscore(local.replace("-", "_"))
        return None

    def get_category(self, curie: str, node: dict) -> Optional[str]:
        """
        Get category for a given CURIE.

        Parameters
        ----------
        curie: str
            Curie for node
        node: dict
            Node data

        Returns
        -------
        Optional[str]
            Category for the given node CURIE.

        """
        category = None
        # use meta.basicPropertyValues
        if "meta" in node and "basicPropertyValues" in node["meta"]:
            for p in node["meta"]["basicPropertyValues"]:
                if p["pred"] == self.HAS_OBO_NAMESPACE:
                    category = p["val"]
                    element = self.toolkit.get_element(category)
                    if element:
                        if "OBO" in element.name:
                            category = f"biolink:{inflection.camelize(inflection.underscore(element.name))}"
                        else:
                            category = f"biolink:{inflection.camelize(stringcase.snakecase(element.name))}"
                    else:
                        element = self.toolkit.get_element_by_mapping(category)
                        if element:
                            if "OBO" in element:
                                category = f"biolink:{inflection.camelize(inflection.underscore(element))}"
                            else:
                                category = f"biolink:{inflection.camelize(stringcase.snakecase(element))}"
                        else:
                            category = "biolink:OntologyClass"

        if not category or category == "biolink:OntologyClass":
            prefix = PrefixManager.get_prefix(curie)
            if prefix == "HP":
                category = "biolink:PhenotypicFeature"
            elif prefix == "CHEBI":
                category = "biolink:ChemicalSubstance"
            elif prefix == "MONDO":
                category = "biolink:Disease"
            elif prefix == "UBERON":
                category = "biolink:AnatomicalEntity"
            elif prefix == "SO":
                category = "biolink:SequenceFeature"
            elif prefix == "CL":
                category = "biolink:Cell"
            elif prefix == "PR":
                category = "biolink:Protein"
            elif prefix == "NCBITaxon":
                category = "biolink:OrganismTaxon"
            else:
                self.owner.log_error(
                    entity=f"{str(category)} for node {curie}",
                    error_type=ErrorType.MISSING_CATEGORY,
                    message=f"Missing category; Defaulting to 'biolink:OntologyClass'",
                    message_level=MessageLevel.WARNING
                )
        return category

    def parse_meta(self, node: str, meta: Dict) -> Dict:
        """
        Parse 'meta' field of a node.

        Parameters
        ----------
        node: str
            Node identifier
        meta: Dict
            meta dictionary for the node

        Returns
        -------
        Dict
            A dictionary that contains 'description', 'subsets',
            'synonyms', 'xrefs', a 'deprecated' flag and/or 'equivalent_nodes'.

        """
        # cross species links are in meta; this needs to be parsed properly too
        # do not put assumptions in code; import as much as possible

        properties = {}
        if "definition" in meta:
            # parse 'definition' as 'description'
            description = meta["definition"]["val"]
            properties["description"] = description

        if "subsets" in meta:
            # parse 'subsets'
            subsets = meta["subsets"]
            properties["subsets"] = [
                x.split("#")[1] if "#" in x else x for x in subsets
            ]

        if "synonyms" in meta:
            # parse 'synonyms' as 'synonym'
            properties["synonym"] = [s["val"] for s in meta["synonyms"] if "val" in s]
            properties["exact_synonym"] = [x['val'] for x in meta["synonyms"] if "pred" in x and x["pred"] == "hasExactSynonym" ]
            properties["related_synonym"] = [x['val'] for x in meta["synonyms"] if "pred" in x and x["pred"] == "hasRelatedSynonym" ]
            properties["broad_synonym"] = [x['val'] for x in meta["synonyms"] if "pred" in x and x["pred"] == "hasBroadSynonym" ]
            properties["narrow_synonym"] = [x['val'] for x in meta["synonyms"] if "pred" in x and x["pred"] == "hasNarrowSynonym" ]

        if "xrefs" in meta:
            # parse 'xrefs' as 'xrefs'
            xrefs = [x["val"] for x in meta["xrefs"]]
            properties["xrefs"] = xrefs

        if "deprecated" in meta:
            # parse 'deprecated' flag
            properties["deprecated"] = meta["deprecated"]

        equivalent_nodes = []
        if "basicPropertyValues" in meta:
            # parse SKOS_EXACT_MATCH entries as 'equivalent_nodes'
            for p in meta["basicPropertyValues"]:
                if p["pred"] in {self.SKOS_EXACT_MATCH}:
                    n = self.prefix_manager.contract(p["val"])
                    if not n:
                        n = p["val"]
                    equivalent_nodes.append(n)
        properties["equivalent_nodes"] = equivalent_nodes

        return properties
