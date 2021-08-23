import gzip
from collections import OrderedDict
from typing import Optional, Union, Tuple, Any, Dict

import rdflib
from linkml_runtime.linkml_model.meta import Element, ClassDefinition, SlotDefinition
from rdflib import URIRef, Literal, Namespace, RDF
from rdflib.plugins.serializers.nt import _nt_row
from rdflib.term import _is_valid_uri

from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger
from kgx.sink.sink import Sink
from kgx.utils.kgx_utils import (
    get_toolkit,
    sentencecase_to_camelcase,
    get_biolink_ancestors,
    sentencecase_to_snakecase,
    generate_uuid,
    get_biolink_property_types,
)
from kgx.utils.rdf_utils import process_predicate

log = get_logger()

property_mapping: OrderedDict = OrderedDict()
reverse_property_mapping: OrderedDict = OrderedDict()


class RdfSink(Sink):
    """
    RdfSink is responsible for writing data as records
    to an RDF serialization.

    .. note::
        Currently only RDF N-Triples serialization is supported.

    Parameters
    ----------
    filename: str
        The filename to write to
    format: str
        The file format (``nt``)
    compression: str
        The compression type (``gz``)
    reify_all_edges: bool
        Whether or not to reify all the edges
    kwargs: Any
        Any additional arguments

    """

    def __init__(
        self,
        filename: str,
        format: str = "nt",
        compression: Optional[bool] = None,
        reify_all_edges: bool = False,
        **kwargs: Any,
    ):
        super().__init__()
        if format not in {"nt"}:
            raise ValueError(f"Only RDF N-Triples ('nt') serialization supported.")
        self.DEFAULT = Namespace(self.prefix_manager.prefix_map[""])
        # self.OBO = Namespace('http://purl.obolibrary.org/obo/')
        self.OBAN = Namespace(self.prefix_manager.prefix_map["OBAN"])
        self.PMID = Namespace(self.prefix_manager.prefix_map["PMID"])
        self.BIOLINK = Namespace(self.prefix_manager.prefix_map["biolink"])
        self.toolkit = get_toolkit()
        self.reverse_predicate_mapping = {}
        self.property_types = get_biolink_property_types()
        self.cache = {}
        self.reify_all_edges = reify_all_edges
        self.reification_types = {
            RDF.Statement,
            self.BIOLINK.Association,
            self.OBAN.association,
        }
        if compression == "gz":
            f = gzip.open(filename, "wb")
        else:
            f = open(filename, "wb")
        self.FH = f
        self.encoding = "ascii"

    def set_reverse_predicate_mapping(self, m: Dict) -> None:
        """
        Set reverse predicate mappings.

        Use this method to update mappings for predicates that are
        not in Biolink Model.

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are property names and values
            are their corresponding IRI.

        """
        for k, v in m.items():
            self.reverse_predicate_mapping[v] = URIRef(k)

    def set_property_types(self, m: Dict) -> None:
        """
        Set export type for properties that are not in
        Biolink Model.

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are property names and values
            are their corresponding types.

        """
        for k, v in m.items():
            (element_uri, canonical_uri, predicate, property_name) = process_predicate(
                self.prefix_manager, k
            )
            if element_uri:
                key = element_uri
            elif predicate:
                key = predicate
            else:
                key = property_name
            self.property_types[key] = v

    def write_node(self, record: Dict) -> None:
        """
        Write a node record as triples.

        Parameters
        ----------
        record: Dict
            A node record

        """
        for k, v in record.items():
            if k in {"id", "iri"}:
                continue
            (
                element_uri,
                canonical_uri,
                predicate,
                property_name,
            ) = self.process_predicate(k)
            if element_uri is None:
                # not a biolink predicate
                if k in self.reverse_predicate_mapping:
                    prop_uri = self.reverse_predicate_mapping[k]
                    # prop_uri = self.prefix_manager.contract(prop_uri)
                else:
                    prop_uri = k
            else:
                prop_uri = canonical_uri if canonical_uri else element_uri
            prop_type = self._get_property_type(prop_uri)
            log.debug(f"prop {k} has prop_uri {prop_uri} and prop_type {prop_type}")
            prop_uri = self.uriref(prop_uri)
            if isinstance(v, (list, set, tuple)):
                for x in v:
                    value_uri = self._prepare_object(k, prop_type, x)
                    self._write_triple(self.uriref(record["id"]), prop_uri, value_uri)
            else:
                value_uri = self._prepare_object(k, prop_type, v)
                self._write_triple(self.uriref(record["id"]), prop_uri, value_uri)

    def _write_triple(self, s: URIRef, p: URIRef, o: Union[URIRef, Literal]) -> None:
        """
        Serialize a triple.

        Parameters
        ----------
        s: rdflib.URIRef
            The subject
        p: rdflib.URIRef
            The predicate
        o: Union[rdflib.URIRef, rdflib.Literal]
            The object

        """
        self.FH.write(_nt_row((s, p, o)).encode(self.encoding, "_rdflib_nt_escape"))

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record as triples.

        Parameters
        ----------
        record: Dict
            An edge record

        """
        ecache = []
        associations = set(
            [self.prefix_manager.contract(x) for x in self.reification_types]
        )
        associations.update(
            [str(x) for x in set(self.toolkit.get_all_associations(formatted=True))]
        )
        if self.reify_all_edges:
            reified_node = self.reify(record["subject"], record["object"], record)
            s = reified_node["subject"]
            p = reified_node["predicate"]
            o = reified_node["object"]
            ecache.append((s, p, o))
            n = reified_node["id"]
            for prop, value in reified_node.items():
                if prop in {"id", "association_id", "edge_key"}:
                    continue
                (
                    element_uri,
                    canonical_uri,
                    predicate,
                    property_name,
                ) = self.process_predicate(prop)
                if element_uri:
                    prop_uri = canonical_uri if canonical_uri else element_uri
                else:
                    if prop in self.reverse_predicate_mapping:
                        prop_uri = self.reverse_predicate_mapping[prop]
                        # prop_uri = self.prefix_manager.contract(prop_uri)
                    else:
                        prop_uri = predicate
                prop_type = self._get_property_type(prop)
                log.debug(
                    f"prop {prop} has prop_uri {prop_uri} and prop_type {prop_type}"
                )
                prop_uri = self.uriref(prop_uri)
                if isinstance(value, list):
                    for x in value:
                        value_uri = self._prepare_object(prop, prop_type, x)
                        self._write_triple(URIRef(n), prop_uri, value_uri)
                else:
                    value_uri = self._prepare_object(prop, prop_type, value)
                    self._write_triple(URIRef(n), prop_uri, value_uri)
        else:
            if (
                ("type" in record and record["type"] in associations)
                or (
                    "association_type" in record
                    and record["association_type"] in associations
                )
                or ("category" in record and any(record["category"]) in associations)
            ):
                reified_node = self.reify(record["subject"], record["object"], record)
                s = reified_node["subject"]
                p = reified_node["predicate"]
                o = reified_node["object"]
                ecache.append((s, p, o))
                n = reified_node["id"]
                for prop, value in reified_node.items():
                    if prop in {"id", "association_id", "edge_key"}:
                        continue
                    (
                        element_uri,
                        canonical_uri,
                        predicate,
                        property_name,
                    ) = self.process_predicate(prop)
                    if element_uri:
                        prop_uri = canonical_uri if canonical_uri else element_uri
                    else:
                        if prop in self.reverse_predicate_mapping:
                            prop_uri = self.reverse_predicate_mapping[prop]
                            # prop_uri = self.prefix_manager.contract(prop_uri)
                        else:
                            prop_uri = predicate
                    prop_type = self._get_property_type(prop)
                    prop_uri = self.uriref(prop_uri)
                    if isinstance(value, list):
                        for x in value:
                            value_uri = self._prepare_object(prop, prop_type, x)
                            self._write_triple(URIRef(n), prop_uri, value_uri)
                    else:
                        value_uri = self._prepare_object(prop, prop_type, value)
                        self._write_triple(URIRef(n), prop_uri, value_uri)
            else:
                s = self.uriref(record["subject"])
                p = self.uriref(record["predicate"])
                o = self.uriref(record["object"])
                self._write_triple(s, p, o)
        for t in ecache:
            self._write_triple(t[0], t[1], t[2])

    def uriref(self, identifier: str) -> URIRef:
        """
        Generate a rdflib.URIRef for a given string.

        Parameters
        ----------
        identifier: str
            Identifier as string.

        Returns
        -------
        rdflib.URIRef
            URIRef form of the input ``identifier``

        """
        if identifier.startswith("urn:uuid:"):
            uri = identifier
        elif identifier in reverse_property_mapping:
            # identifier is a property
            uri = reverse_property_mapping[identifier]
        else:
            # identifier is an entity
            fixed_identifier = identifier
            if fixed_identifier.startswith(":"):
                # TODO: this should be handled upstream by prefixcommons-py
                fixed_identifier = fixed_identifier.replace(":", "", 1)
            if " " in identifier:
                fixed_identifier = fixed_identifier.replace(" ", "_")

            if self.prefix_manager.is_curie(fixed_identifier):
                uri = self.prefix_manager.expand(fixed_identifier)
                if fixed_identifier == uri:
                    uri = self.DEFAULT.term(fixed_identifier)
            elif self.prefix_manager.is_iri(fixed_identifier):
                uri = fixed_identifier
            else:
                uri = self.DEFAULT.term(fixed_identifier)
            # if identifier == uri:
            #     if PrefixManager.is_curie(identifier):
            #         identifier = identifier.replace(':', '_')
        return URIRef(uri)

    def _prepare_object(
        self, prop: str, prop_type: str, value: Any
    ) -> rdflib.term.Identifier:
        """
        Prepare the object of a triple.

        Parameters
        ----------
        prop: str
            property name
        prop_type: str
            property type
        value: Any
            property value

        Returns
        -------
        rdflib.term.Identifier
            An instance of rdflib.term.Identifier

        """
        if prop_type == "uriorcurie" or prop_type == "xsd:anyURI":
            if isinstance(value, str) and PrefixManager.is_curie(value):
                o = self.uriref(value)
            elif isinstance(value, str) and PrefixManager.is_iri(value):
                if _is_valid_uri(value):
                    o = URIRef(value)
                else:
                    o = Literal(value)
            else:
                o = Literal(value)
        elif prop_type.startswith("xsd"):
            o = Literal(value, datatype=self.prefix_manager.expand(prop_type))
        else:
            o = Literal(value, datatype=self.prefix_manager.expand("xsd:string"))
        return o

    def _get_property_type(self, p: str) -> str:
        """
        Get type for a given property name.

        Parameters
        ----------
        p: str
            property name

        Returns
        -------
        str
            The type for property name

        """
        # TODO: this should be properly defined in the model
        default_uri_types = {
            "biolink:type",
            "biolink:category",
            "biolink:subject",
            "biolink:object",
            "biolink:relation",
            "biolink:predicate",
            "rdf:type",
            "rdf:subject",
            "rdf:predicate",
            "rdf:object",
        }
        if p in default_uri_types:
            t = "uriorcurie"
        else:
            if p in self.property_types:
                t = self.property_types[p]
            elif f":{p}" in self.property_types:
                t = self.property_types[f":{p}"]
            elif f"biolink:{p}" in self.property_types:
                t = self.property_types[f"biolink:{p}"]
            else:
                t = "xsd:string"
            # if value:
            #     if isinstance(value, (list, set, tuple)):
            #         x = value[0]
            #         if self.graph.has_node(x):
            #             t = 'uriorcurie'
            #         else:
            #             t = 'xsd:string'
            #     else:
            #         if self.graph.has_node(value):
            #             t = 'uriorcurie'
            #         else:
            #             t = 'xsd:string'
        return t

    def process_predicate(self, p: Optional[Union[URIRef, str]]) -> Tuple:
        """
        Process a predicate where the method checks if there is a mapping in Biolink Model.

        Parameters
        ----------
        p: Optional[Union[URIRef, str]]
            The predicate

        Returns
        -------
        Tuple
            A tuple that contains the Biolink CURIE (if available), the Biolink slot_uri CURIE (if available),
            the CURIE form of p, the reference of p

        """
        if p in self.cache:
            # already processed this predicate before; pull from cache
            element_uri = self.cache[p]["element_uri"]
            canonical_uri = self.cache[p]["canonical_uri"]
            predicate = self.cache[p]["predicate"]
            property_name = self.cache[p]["property_name"]
        else:
            # haven't seen this property before; map to element
            if self.prefix_manager.is_iri(p):
                predicate = self.prefix_manager.contract(str(p))
            else:
                predicate = None
            if self.prefix_manager.is_curie(p):
                property_name = self.prefix_manager.get_reference(p)
                predicate = p
            else:
                if predicate and self.prefix_manager.is_curie(predicate):
                    property_name = self.prefix_manager.get_reference(predicate)
                else:
                    property_name = p
                    predicate = f":{p}"
            element = self.get_biolink_element(p)
            canonical_uri = None
            if element:
                if isinstance(element, SlotDefinition):
                    # predicate corresponds to a biolink slot
                    if element.definition_uri:
                        element_uri = self.prefix_manager.contract(
                            element.definition_uri
                        )
                    else:
                        element_uri = (
                            f"biolink:{sentencecase_to_snakecase(element.name)}"
                        )
                    if element.slot_uri:
                        canonical_uri = element.slot_uri
                elif isinstance(element, ClassDefinition):
                    # this will happen only when the IRI is actually
                    # a reference to a class
                    element_uri = self.prefix_manager.contract(element.class_uri)
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
                if p in self.reverse_predicate_mapping:
                    property_name = self.reverse_predicate_mapping[p]
                    predicate = f":{property_name}"
            self.cache[p] = {
                "element_uri": element_uri,
                "canonical_uri": canonical_uri,
                "predicate": predicate,
                "property_name": property_name,
            }
        return element_uri, canonical_uri, predicate, property_name

    def get_biolink_element(self, predicate: Any) -> Optional[Element]:
        """
        Returns a Biolink Model element for a given predicate.

        Parameters
        ----------
        predicate: Any
            The CURIE of a predicate

        Returns
        -------
        Optional[Element]
            The corresponding Biolink Model element

        """
        toolkit = get_toolkit()
        if self.prefix_manager.is_iri(predicate):
            predicate_curie = self.prefix_manager.contract(predicate)
        else:
            predicate_curie = predicate
        if self.prefix_manager.is_curie(predicate_curie):
            reference = self.prefix_manager.get_reference(predicate_curie)
        else:
            reference = predicate_curie
        element = toolkit.get_element(reference)
        if not element:
            try:
                mapping = toolkit.get_element_by_mapping(predicate)
                if mapping:
                    element = toolkit.get_element(mapping)
            except ValueError as e:
                log.error(e)
        return element

    def reify(self, u: str, v: str, data: Dict) -> Dict:
        """
        Create a node representation of an edge.

        Parameters
        ----------
        u: str
            Subject
        v: str
            Object
        k: str
            Edge key
        data: Dict
            Edge data

        Returns
        -------
        Dict
            The reified node

        """
        s = self.uriref(u)
        p = self.uriref(data["predicate"])
        o = self.uriref(v)

        if "id" in data:
            node_id = self.uriref(data["id"])
        else:
            # generate a UUID for the reified node
            node_id = self.uriref(generate_uuid())
        reified_node = data.copy()
        if "category" in reified_node:
            del reified_node["category"]
        reified_node["id"] = node_id
        reified_node["type"] = "biolink:Association"
        reified_node["subject"] = s
        reified_node["predicate"] = p
        reified_node["object"] = o
        return reified_node

    def finalize(self) -> None:
        """
        Perform any operations after writing the file.
        """
        self.FH.close()
