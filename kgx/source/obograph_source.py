import gzip
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

    def __init__(self, owner):
        super().__init__(owner)
        self.toolkit = Toolkit()
        self.ecache: Dict = {}

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
                        mapping = self.toolkit.get_element_by_mapping(edge["pred"])
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
        for x in edge.keys():
            if x not in {"sub", "pred", "obj"}:
                fixed_edge[x] = edge[x]
        return super().read_edge(fixed_edge)

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
