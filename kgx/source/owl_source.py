from typing import Set, Optional, Generator, Any, Dict

import rdflib
from rdflib import Namespace, URIRef, OWL, RDFS, RDF

from kgx.config import get_logger
from kgx.source import RdfSource
from kgx.utils.kgx_utils import (
    current_time_in_millis,
    generate_uuid,
    generate_edge_identifiers,
    validate_node,
    sanitize_import,
    validate_edge,
)

log = get_logger()


class OwlSource(RdfSource):
    """
    OwlSource is responsible for parsing an OWL ontology.

    ..note::
        This is a simple parser that loads direct class-class relationships.
        For more formal OWL parsing, refer to Robot: http://robot.obolibrary.org/

    """

    def __init__(self):
        self.imported: Set = set()
        super().__init__()
        self.OWLSTAR = Namespace("http://w3id.org/owlstar/")
        self.excluded_predicates = {
            URIRef("https://raw.githubusercontent.com/geneontology/go-ontology/master/contrib/oboInOwl#id")
        }

    def parse(
        self,
        filename: str,
        format: str = "owl",
        compression: Optional[str] = None,
        **kwargs: Any,
    ) -> Generator:
        """
        This method reads from an OWL and yields records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``owl``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records read from the file

        """
        rdfgraph = rdflib.Graph()
        if compression:
            log.warning(f"compression mode '{compression}' not supported by OwlSource")
        if format is None:
            format = rdflib.util.guess_format(filename)

        if format == "owl":
            format = "xml"

        log.info("Parsing {} with '{}' format".format(filename, format))
        rdfgraph.parse(filename, format=format)
        log.info("{} parsed with {} triples".format(filename, len(rdfgraph)))

        self.set_provenance_map(kwargs)

        self.start = current_time_in_millis()
        log.info(f"Done parsing {filename}")

        triples = rdfgraph.triples((None, OWL.imports, None))
        for s, p, o in triples:
            # Load all imports first
            if p == OWL.imports:
                if o not in self.imported:
                    input_format = rdflib.util.guess_format(o)
                    imported_rdfgraph = rdflib.Graph()
                    log.info(f"Parsing OWL import: {o}")
                    self.imported.add(o)
                    imported_rdfgraph.parse(o, format=input_format)
                    self.load_graph(imported_rdfgraph)
                else:
                    log.warning(f"Trying to import {o} but its already done")
        yield from self.load_graph(rdfgraph)

    def load_graph(self, rdfgraph: rdflib.Graph, **kwargs: Any) -> None:
        """
        Walk through the rdflib.Graph and load all triples into kgx.graph.base_graph.BaseGraph

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            Graph containing nodes and edges
        kwargs: Any
            Any additional arguments

        """
        seen = set()
        seen.add(RDFS.subClassOf)
        for s, p, o in rdfgraph.triples((None, RDFS.subClassOf, None)):
            # ignoring blank nodes
            if isinstance(s, rdflib.term.BNode):
                continue
            pred = None
            parent = None
            os_interpretation = None
            if isinstance(o, rdflib.term.BNode):
                # C SubClassOf R some D
                for x in rdfgraph.objects(o, OWL.onProperty):
                    pred = x
                # owl:someValuesFrom
                for x in rdfgraph.objects(o, OWL.someValuesFrom):
                    os_interpretation = self.OWLSTAR.term("AllSomeInterpretation")
                    parent = x
                # owl:allValuesFrom
                for x in rdfgraph.objects(o, OWL.allValuesFrom):
                    os_interpretation = self.OWLSTAR.term("AllOnlyInterpretation")
                    parent = x
                if pred is None or parent is None:
                    log.warning(
                        f"{s} {p} {o} has OWL.onProperty {pred} and OWL.someValuesFrom {parent}"
                    )
                    log.warning("Do not know how to handle BNode: {}".format(o))
                    continue
            else:
                # C rdfs:subClassOf D (where C and D are named classes)
                pred = p
                parent = o
            if os_interpretation:
                # reify edges that have logical interpretation
                eid = generate_uuid()
                self.reified_nodes.add(eid)
                yield from self.triple(
                    URIRef(eid), self.BIOLINK.term("category"), self.BIOLINK.Association
                )
                yield from self.triple(URIRef(eid), self.BIOLINK.term("subject"), s)
                yield from self.triple(
                    URIRef(eid), self.BIOLINK.term("predicate"), pred
                )
                yield from self.triple(URIRef(eid), self.BIOLINK.term("object"), parent)
                yield from self.triple(
                    URIRef(eid),
                    self.BIOLINK.term("logical_interpretation"),
                    os_interpretation,
                )
            else:
                yield from self.triple(s, pred, parent)

        seen.add(OWL.equivalentClass)
        for s, p, o in rdfgraph.triples((None, OWL.equivalentClass, None)):
            # A owl:equivalentClass B (where A and B are named classes)
            if not isinstance(o, rdflib.term.BNode):
                yield from self.triple(s, p, o)

        for relation in rdfgraph.subjects(RDF.type, OWL.ObjectProperty):
            seen.add(relation)
            for s, p, o in rdfgraph.triples((relation, None, None)):
                if not isinstance(o, rdflib.term.BNode):
                    if p not in self.excluded_predicates:
                        yield from self.triple(s, p, o)

        for s, p, o in rdfgraph.triples((None, None, None)):
            if isinstance(s, rdflib.term.BNode) or isinstance(o, rdflib.term.BNode):
                continue
            if p in seen:
                continue
            if p in self.excluded_predicates:
                continue
            yield from self.triple(s, p, o)

        for n in self.reified_nodes:
            data = self.node_cache.pop(n)
            self.dereify(n, data)

        for k, data in self.node_cache.items():
            node_data = validate_node(data)
            node_data = sanitize_import(node_data)
            self.set_node_provenance(node_data)
            if self.check_node_filter(node_data):
                self.node_properties.update(node_data.keys())
                yield k, node_data
        self.node_cache.clear()

        for k, data in self.edge_cache.items():
            edge_data = validate_edge(data)
            edge_data = sanitize_import(edge_data)
            self.set_edge_provenance(edge_data)
            if self.check_edge_filter(edge_data):
                self.edge_properties.update(edge_data.keys())
                yield k[0], k[1], k[2], edge_data
        self.edge_cache.clear()
