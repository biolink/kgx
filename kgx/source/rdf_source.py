import gzip
import typing
from typing import Set, Dict, Union, Optional, Any, Tuple, List, Generator

import rdflib
from linkml_runtime.linkml_model.meta import SlotDefinition, ClassDefinition, Element
from rdflib import URIRef, RDF, Namespace

from kgx.error_detection import ErrorType, MessageLevel
from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger
from kgx.parsers.ntriples_parser import CustomNTriplesParser
from kgx.source.source import Source, DEFAULT_EDGE_PREDICATE
from kgx.utils.graph_utils import curie_lookup
from kgx.utils.kgx_utils import (
    get_toolkit,
    is_property_multivalued,
    generate_edge_key,
    sentencecase_to_snakecase,
    sentencecase_to_camelcase,
    get_biolink_ancestors,
    sanitize_import,
    prepare_data_dict,
    CORE_NODE_PROPERTIES,
    CORE_EDGE_PROPERTIES,
    knowledge_provenance_properties,
)

log = get_logger()

NAMED_THING = "biolink:NamedThing"


class RdfSource(Source):
    """
    RdfSource is responsible for reading data as records
    from RDF.

    .. note::
        Currently only RDF N-Triples are supported.

    """

    def __init__(self, owner):
        super().__init__(owner)
        self.DEFAULT = Namespace(self.prefix_manager.prefix_map[""])
        self.OBAN = Namespace(self.prefix_manager.prefix_map["OBAN"])
        self.PMID = Namespace(self.prefix_manager.prefix_map["PMID"])
        self.BIOLINK = Namespace(self.prefix_manager.prefix_map["biolink"])
        self.predicate_mapping = {}
        self.cache: Dict = {}
        self.toolkit = get_toolkit()
        self.node_property_predicates = set(
            [
                URIRef(self.prefix_manager.expand(x))
                for x in self.toolkit.get_all_node_properties(formatted=True)
            ]
        )
        self.node_property_predicates.update(
            set(self.toolkit.get_all_node_properties(formatted=True))
        )
        self.node_property_predicates.update(
            set(self.toolkit.get_all_edge_properties(formatted=True))
        )

        for ksf in knowledge_provenance_properties:
            self.node_property_predicates.add(
                URIRef(self.prefix_manager.expand("biolink:" + ksf))
            )

        self.reification_types = {
            RDF.Statement,
            self.BIOLINK.Association,
            self.OBAN.association,
        }
        self.reification_predicates = {
            self.BIOLINK.subject,
            self.BIOLINK.predicate,
            self.BIOLINK.object,
            RDF.subject,
            RDF.object,
            RDF.predicate,
            self.OBAN.association_has_subject,
            self.OBAN.association_has_predicate,
            self.OBAN.association_has_object,
        }
        self.reified_nodes: Set = set()
        self.start: int = 0
        self.count: int = 0
        self.CACHE_SIZE = 10000
        self.node_record = {}
        self.edge_record = {}
        self.node_cache = {}
        self.edge_cache = {}
        self._incomplete_nodes = {}

    def set_predicate_mapping(self, m: Dict) -> None:
        """
        Set predicate mappings.

        Use this method to update mappings for predicates that are
        not in Biolink Model.

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are IRIs and values are their
            corresponding property names

        """
        for k, v in m.items():
            self.predicate_mapping[URIRef(k)] = v

    def set_node_property_predicates(self, predicates) -> None:
        """
        Set predicates that are to be treated as node properties.

        Parameters
        ----------
        predicates: Set
            Set of predicates

        """
        for p in predicates:
            self.node_property_predicates.add(URIRef(p))

    def parse(
        self,
        filename: str,
        format: str = "nt",
        compression: Optional[str] = None,
        **kwargs: Any,
    ) -> typing.Generator:
        """
        This method reads from RDF N-Triples and yields records.

        .. note::
            To ensure proper parsing of N-Triples and a relatively low memory footprint,
            it is recommended that the N-Triples be sorted based on the subject IRIs.

            ```sort -k 1,2 -t ' ' data.nt > data_sorted.nt```

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``nt``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """
        p = CustomNTriplesParser(self)

        self.set_provenance_map(kwargs)

        if compression == "gz":
            yield from p.parse(gzip.open(filename, "rb"))
        else:
            yield from p.parse(open(filename, "rb"))
        log.info(f"Done parsing {filename}")

        for n in self.reified_nodes:
            data = self.node_cache.pop(n)
            self.dereify(n, data)

        for k in self.node_cache.keys():
            node_data = self.node_cache[k]
            if "category" in node_data:
                if NAMED_THING not in set(node_data["category"]):
                    node_data["category"].append(NAMED_THING)
            else:
                node_data["category"] = [NAMED_THING]

            node_data = self.validate_node(node_data)
            if not node_data:
                continue

            node_data = sanitize_import(node_data)

            self.set_node_provenance(node_data)

            if self.check_node_filter(node_data):
                self.node_properties.update(node_data.keys())
                yield k, node_data

        self.node_cache.clear()

        for k in self.edge_cache.keys():
            edge_data = self.edge_cache[k]

            edge_data = self.validate_edge(edge_data)
            if not edge_data:
                continue

            edge_data = sanitize_import(edge_data)

            self.set_edge_provenance(edge_data)

            if self.check_edge_filter(edge_data):
                self.edge_properties.update(edge_data.keys())
                yield k[0], k[1], k[2], edge_data

        self.edge_cache.clear()

    def triple(self, s: URIRef, p: URIRef, o: URIRef) -> None:
        """
        Parse a triple.

        Parameters
        ----------
        s: URIRef
            Subject
        p: URIRef
            Predicate
        o: URIRef
            Object

        """
        self.count += 1
        (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(
            p
        )
        if element_uri:
            prop_uri = element_uri
        elif predicate:
            prop_uri = predicate
        else:
            prop_uri = property_name

        s_curie = self.prefix_manager.contract(s)
        if s_curie.startswith("biolink") or s_curie.startswith("OBAN"):
            log.warning(f"Skipping {s} {p} {o}")
        elif s_curie in self.reified_nodes:
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif p in self.reification_predicates:
            # subject is a reified node
            self.reified_nodes.add(s_curie)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif property_name in {
            "subject",
            "predicate",
            "object",
            "predicate",
            "relation",
        }:
            # subject is a reified node
            self.reified_nodes.add(s_curie)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif o in self.reification_types:
            # subject is a reified node
            self.reified_nodes.add(s_curie)
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif element_uri and element_uri in self.node_property_predicates:
            # treating predicate as a node property
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif (
            p in self.node_property_predicates
            or predicate in self.node_property_predicates
            or property_name in self.node_property_predicates
        ):
            # treating predicate as a node property
            self.add_node_attribute(s, key=prop_uri, value=o)
        elif isinstance(o, rdflib.term.Literal):
            self.add_node_attribute(s, key=prop_uri, value=o)
        else:
            # treating predicate as an edge
            self.add_edge(s, o, p)

        if len(self.edge_cache) >= self.CACHE_SIZE:
            while self.reified_nodes:
                n = self.reified_nodes.pop()
                data = self.node_cache.pop(n)
                try:
                    self.dereify(n, data)
                except ValueError as e:
                    self.owner.log_error(
                        entity=str(data),
                        error_type=ErrorType.INVALID_EDGE_PROPERTY,
                        message=str(e),
                        message_level=MessageLevel.WARNING
                    )
                    self._incomplete_nodes[n] = data

            for n in self._incomplete_nodes.keys():
                self.node_cache[n] = self._incomplete_nodes[n]
                self.reified_nodes.add(n)
            self._incomplete_nodes.clear()

            for k in self.edge_cache.keys():
                if (
                    "id" not in self.edge_cache[k]
                    and "association_id" not in self.edge_cache[k]
                ):
                    edge_key = generate_edge_key(
                        self.edge_cache[k]["subject"],
                        self.edge_cache[k]["predicate"],
                        self.edge_cache[k]["object"],
                    )
                    self.edge_cache[k]["id"] = edge_key
                data = self.edge_cache[k]
                data = self.validate_edge(data)
                data = sanitize_import(data)

                self.set_edge_provenance(data)

                if self.check_edge_filter(data):
                    self.edge_properties.update(data.keys())
                    yield k[0], k[1], k[2], data
            self.edge_cache.clear()
        yield None

    def dereify(self, n: str, node: Dict) -> None:
        """
        Dereify a node to create a corresponding edge.

        Parameters
        ----------
        n: str
            Node identifier
        node: Dict
            Node data

        """
        if "predicate" not in node:
            node["predicate"] = "biolink:related_to"
        if "relation" not in node:
            node["relation"] = node["predicate"]
        if "subject" in node and "object" in node:
            self.edge_properties.update(node.keys())
            self.add_edge(node["subject"], node["object"], node["predicate"], node)
        else:
            self.owner.log_error(
                entity=str(node),
                error_type=ErrorType.MISSING_CATEGORY,
                message=f"Missing 'subject' or 'object' in reified edge node",
                message_level=MessageLevel.WARNING
            )

    def add_node_attribute(
        self, iri: Union[URIRef, str], key: str, value: Union[str, List]
    ) -> None:
        """
        Add an attribute to a node in cache, while taking into account whether the attribute
        should be multi-valued.

        The ``key`` may be a rdflib.URIRef or an URI string that maps onto a
        property name as defined in ``rdf_utils.property_mapping``.

        Parameters
        ----------
        iri: Union[rdflib.URIRef, str]
            The IRI of a node in the rdflib.Graph
        key: str
            The name of the attribute. Can be a rdflib.URIRef or URI string
        value: Union[str, List]
            The value of the attribute

        Returns
        -------
        Dict
            The node data

        """
        if self.prefix_manager.is_iri(key):
            key_curie = self.prefix_manager.contract(key)
        else:
            key_curie = key
        c = curie_lookup(key_curie)
        if c:
            key_curie = c
        if self.prefix_manager.is_curie(key_curie):
            # property names will always be just the reference
            mapped_key = self.prefix_manager.get_reference(key_curie)
        else:
            mapped_key = key_curie

        if isinstance(value, rdflib.term.Identifier):
            if isinstance(value, rdflib.term.URIRef):
                value_curie = self.prefix_manager.contract(value)
                value = value_curie
            else:
                value = value.toPython()
        if (
            mapped_key in is_property_multivalued
            and is_property_multivalued[mapped_key]
        ):
            value = [value]
        if mapped_key in self.node_record:
            if isinstance(self.node_record[mapped_key], str):
                _ = self.node_record[mapped_key]
                self.node_record[mapped_key] = [_]
            self.node_record[mapped_key].append(value)
        else:
            self.node_record[mapped_key] = [value]
        curie = self.prefix_manager.contract(iri)
        if curie in self.node_cache:
            if mapped_key in self.node_cache[curie]:
                node = self.node_cache[curie]
                updated_node = prepare_data_dict(node, {mapped_key: value})
                self.node_cache[curie] = updated_node
            else:
                self.node_cache[curie][mapped_key] = value
        else:
            self.node_cache[curie] = {"id": curie, mapped_key: value}

    def add_node(self, iri: URIRef, data: Optional[Dict] = None) -> Dict:
        """
        Add a node to cache.

        Parameters
        ----------
        iri: rdflib.URIRef
            IRI of a node
        data: Optional[Dict]
            Additional node properties

        Returns
        -------
        Dict
            The node data

        """
        n = self.prefix_manager.contract(str(iri))
        if n == iri:
            if self.prefix_manager.has_urlfragment(iri):
                n = rdflib.namespace.urldefrag(iri).fragment

        if not n:
            n = iri
        if n in self.node_cache:
            node_data = self.update_node(n, data)
        else:
            if data:
                node_data = data
            else:
                node_data = {}
            node_data["id"] = n

        if "category" in node_data:
            if NAMED_THING not in set(node_data["category"]):
                node_data["category"].append(NAMED_THING)
        else:
            node_data["category"] = [NAMED_THING]

        self.set_node_provenance(node_data)

        self.node_cache[n] = node_data
        return node_data

    def add_edge(
        self,
        subject_iri: URIRef,
        object_iri: URIRef,
        predicate_iri: URIRef,
        data: Optional[Dict[Any, Any]] = None,
    ) -> Dict:
        """
        Add an edge to cache.

        Parameters
        ----------
        subject_iri: rdflib.URIRef
            Subject IRI for the subject in a triple
        object_iri: rdflib.URIRef
            Object IRI for the object in a triple
        predicate_iri: rdflib.URIRef
            Predicate IRI for the predicate in a triple
        data: Optional[Dict[Any, Any]]
            Additional edge properties

        Returns
        -------
        Dict
            The edge data

        """
        (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(
            predicate_iri
        )
        subject_curie = self.prefix_manager.contract(subject_iri)
        object_curie = self.prefix_manager.contract(object_iri)
        if subject_curie in self.node_cache:
            subject_node = self.node_cache[subject_curie]
        else:
            subject_node = self.add_node(subject_iri)

        if object_curie in self.node_cache:
            object_node = self.node_cache[object_curie]
        else:
            object_node = self.add_node(object_iri)
        edge_predicate = element_uri if element_uri else predicate
        if not edge_predicate:
            edge_predicate = property_name

        if " " in edge_predicate:
            log.debug(
                f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that not in snake_case form; replacing ' ' with '_'"
            )
        edge_predicate_prefix = self.prefix_manager.get_prefix(edge_predicate)
        if edge_predicate_prefix not in {"biolink", "rdf", "rdfs", "skos", "owl"}:
            if PrefixManager.is_curie(edge_predicate):
                # name = curie_lookup(edge_predicate)
                # if name:
                #     log.debug(f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that is actually a CURIE; Using its mapping instead: {name}")
                #     edge_predicate = f"{edge_predicate_prefix}:{name}"
                # else:
                #     log.debug(f"predicate IRI '{predicate_iri}' yields edge_predicate '{edge_predicate}' that is actually a CURIE; defaulting back to {self.DEFAULT_EDGE_PREDICATE}")
                edge_predicate = DEFAULT_EDGE_PREDICATE

        edge_key = generate_edge_key(
            subject_node["id"], edge_predicate, object_node["id"]
        )
        if (subject_node["id"], object_node["id"], edge_key) in self.edge_cache:
            # edge already exists; process kwargs and update the edge
            edge_data = self.update_edge(
                subject_node["id"], object_node["id"], edge_key, data
            )
        else:
            # add a new edge
            edge_data = data if data else {}
            edge_data.update(
                {
                    "subject": subject_node["id"],
                    "predicate": f"{edge_predicate}",
                    "object": object_node["id"],
                }
            )
            if "relation" not in edge_data:
                edge_data["relation"] = predicate

            self.set_edge_provenance(edge_data)

        self.edge_cache[(subject_node["id"], object_node["id"], edge_key)] = edge_data
        return edge_data

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
            if not element:
                element = self.get_biolink_element(predicate)
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
                # look at predicate mappings
                element_uri = None
                if p in self.predicate_mapping:
                    property_name = self.predicate_mapping[p]
                    predicate = f":{property_name}"
            self.cache[p] = {
                "element_uri": element_uri,
                "canonical_uri": canonical_uri,
                "predicate": predicate,
                "property_name": property_name,
            }
        return element_uri, canonical_uri, predicate, property_name

    def update_node(self, n: Union[URIRef, str], data: Optional[Dict] = None) -> Dict:
        """
        Update a node with properties.

        Parameters
        ----------
        n: Union[URIRef, str]
            Node identifier
        data: Optional[Dict]
            Node properties

        Returns
        -------
        Dict
            The node data

        """
        node_data = self.node_cache[str(n)]
        if data:
            new_data = self._prepare_data_dict(node_data, data)
            node_data.update(new_data)
        return node_data

    def update_edge(
        self,
        subject_curie: str,
        object_curie: str,
        edge_key: str,
        data: Optional[Dict[Any, Any]],
    ) -> Dict:
        """
        Update an edge with properties.

        Parameters
        ----------
        subject_curie: str
            Subject CURIE
        object_curie: str
            Object CURIE
        edge_key: str
            Edge key
        data: Optional[Dict[Any, Any]]
            Edge properties

        Returns
        -------
        Dict
            The edge data

        """
        key = (subject_curie, object_curie, edge_key)
        if key in self.edge_cache:
            edge_data = self.edge_cache[key]
        else:
            edge_data = {}
        if data:
            new_data = self._prepare_data_dict(edge_data, data)
            edge_data.update(new_data)
        return edge_data

    def _prepare_data_dict(self, d1: Dict, d2: Dict) -> Dict:
        """
        Given two dict objects, make a new dict object that is the intersection of the two.

        If a key is known to be multivalued then it's value is converted to a list.
        If a key is already multivalued then it is updated with new values.
        If a key is single valued, and a new unique value is found then the existing value is
        converted to a list and the new value is appended to this list.

        Parameters
        ----------
        d1: Dict
            Dict object
        d2: Dict
            Dict object

        Returns
        -------
        Dict
            The intersection of d1 and d2

        """
        new_data = {}
        for key, value in d2.items():
            if isinstance(value, (list, set, tuple)):
                new_value = [
                    self.prefix_manager.contract(x)
                    if self.prefix_manager.is_iri(x)
                    else x
                    for x in value
                ]
            else:
                new_value = (
                    self.prefix_manager.contract(value)
                    if self.prefix_manager.is_iri(value)
                    else value
                )

            if key in is_property_multivalued:
                if is_property_multivalued[key]:
                    # key is supposed to be multivalued
                    if key in d1:
                        # key is in data
                        if isinstance(d1[key], list):
                            # existing key has value type list
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [
                                    x for x in new_value if x not in new_data[key]
                                ]
                            else:
                                if new_value not in new_data[key]:
                                    new_data[key].append(new_value)
                        else:
                            if (
                                key in CORE_NODE_PROPERTIES
                                or key in CORE_EDGE_PROPERTIES
                            ):
                                log.debug(
                                    f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                                )
                            else:
                                # existing key does not have value type list; converting to list
                                new_data[key] = [d1[key]]
                                if isinstance(new_value, (list, set, tuple)):
                                    new_data[key] += [
                                        x for x in new_value if x not in new_data[key]
                                    ]
                                else:
                                    if new_value not in new_data[key]:
                                        new_data[key].append(new_value)
                    else:
                        # key is not in data; adding
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] = [x for x in new_value]
                        else:
                            new_data[key] = [new_value]
                else:
                    # key is not multivalued; adding/replacing as-is
                    if key in d1:
                        if isinstance(d1[key], list):
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value]
                            else:
                                new_data[key].append(new_value)
                        else:
                            if (
                                key in CORE_NODE_PROPERTIES
                                or key in CORE_EDGE_PROPERTIES
                            ):
                                log.debug(
                                    f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                                )
                            else:
                                new_data[key] = new_value
                    else:
                        new_data[key] = new_value
            else:
                # treating key as multivalued
                if key in d1:
                    # key is in data
                    if key in CORE_NODE_PROPERTIES or key in CORE_EDGE_PROPERTIES:
                        log.debug(
                            f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                        )
                    else:
                        if isinstance(d1[key], list):
                            # existing key has value type list
                            new_data[key] = d1[key]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [
                                    x for x in new_value if x not in new_data[key]
                                ]
                            else:
                                new_data[key].append(new_value)
                        else:
                            # existing key does not have value type list; converting to list
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [
                                    x for x in new_value if x not in new_data[key]
                                ]
                            else:
                                new_data[key].append(new_value)
                else:
                    new_data[key] = new_value
        return new_data

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
                else:
                    mapping = toolkit.get_element_by_mapping(reference)
                    if mapping:
                        element = toolkit.get_element(mapping)
            except ValueError as e:
                self.owner.log_error(
                    entity=str(predicate),
                    error_type=ErrorType.INVALID_EDGE_PREDICATE,
                    message=str(e)
                )
                element = None
        return element
