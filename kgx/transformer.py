import itertools
import os
from os.path import exists
from sys import stderr
from typing import Dict, Generator, List, Optional, Callable, Set

from kgx.config import get_logger
from kgx.source import (
    GraphSource,
    Source,
    TsvSource,
    JsonSource,
    JsonlSource,
    ObographSource,
    TrapiSource,
    NeoSource,
    RdfSource,
    OwlSource,
    SssomSource,
)
from kgx.sink import (
    Sink,
    GraphSink,
    TsvSink,
    JsonSink,
    JsonlSink,
    NeoSink,
    RdfSink,
    NullSink,
)
from kgx.utils.kgx_utils import (
    apply_graph_operations,
    GraphEntityType,
    knowledge_provenance_properties,
)

SOURCE_MAP = {
    "tsv": TsvSource,
    "csv": TsvSource,
    "graph": GraphSource,
    "json": JsonSource,
    "jsonl": JsonlSource,
    "obojson": ObographSource,
    "obo-json": ObographSource,
    "trapi-json": TrapiSource,
    "neo4j": NeoSource,
    "nt": RdfSource,
    "owl": OwlSource,
    "sssom": SssomSource,
}

SINK_MAP = {
    "tsv": TsvSink,
    "csv": TsvSink,
    "graph": GraphSink,
    "json": JsonSink,
    "jsonl": JsonlSink,
    "neo4j": NeoSink,
    "nt": RdfSink,
    "null": NullSink,
}


log = get_logger()


class Transformer(object):
    """
    The Transformer class is responsible for transforming data from one
    form to another.

    Parameters
    ----------
    stream: bool
        Whether or not to stream
    infores_catalog: Optional[str]
        Optional dump of a TSV file of InfoRes CURIE to Knowledge Source mappings

    """

    def __init__(self, stream: bool = False, infores_catalog: Optional[str] = None):
        self.stream = stream
        self.node_filters = {}
        self.edge_filters = {}

        self.inspector: Optional[Callable[[GraphEntityType, List], None]] = None

        self.store = self.get_source("graph")
        self._seen_nodes = set()
        self._infores_catalog: Dict[str, str] = dict()

        if infores_catalog and exists(infores_catalog):
            with open(infores_catalog, "r") as irc:
                for entry in irc:
                    if len(entry):
                        entry = entry.strip()
                        if entry:
                            print("entry: " + entry, file=stderr)
                            source, infores = entry.split("\t")
                            self._infores_catalog[source] = infores

    def transform(
        self,
        input_args: Dict,
        output_args: Optional[Dict] = None,
        inspector: Optional[Callable[[GraphEntityType, List], None]] = None,
    ) -> None:
        """
        Transform an input source and write to an output sink.

        If ``output_args`` is not defined then the data is persisted to
        an in-memory graph.

        The 'inspector' argument is an optional Callable which the
        transformer.process() method applies to 'inspect' source records
        prior to writing them out to the Sink. The first (GraphEntityType)
        argument of the Callable tags the record as a NODE or an EDGE.
        The second argument given to the Callable is the current record
        itself. This Callable is strictly meant to be procedural and should
        *not* mutate the record.

        Parameters
        ----------
        input_args: Dict
            Arguments relevant to your input source
        output_args: Optional[Dict]
            Arguments relevant to your output sink (
        inspector: Optional[Callable[[GraphEntityType, List], None]]
            Optional Callable to 'inspect' source records during processing.
        """
        sources = []
        generators = []
        input_format = input_args["format"]
        prefix_map = input_args.pop("prefix_map", {})
        predicate_mappings = input_args.pop("predicate_mappings", {})
        node_property_predicates = input_args.pop("node_property_predicates", {})
        node_filters = input_args.pop("node_filters", {})
        edge_filters = input_args.pop("edge_filters", {})
        operations = input_args.pop("operations", [])

        # Optional process() data stream inspector
        self.inspector = inspector

        if input_format in {"neo4j", "graph"}:
            source = self.get_source(input_format)
            source.set_prefix_map(prefix_map)
            source.set_node_filters(node_filters)
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters
            source.set_edge_filters(edge_filters)
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters

            if "uri" in input_args:
                default_provenance = input_args["uri"]
            else:
                default_provenance = None

            g = source.parse(default_provenance=default_provenance, **input_args)

            sources.append(source)
            generators.append(g)
        else:
            filename = input_args.pop("filename", {})
            for f in filename:
                source = self.get_source(input_format)
                source.set_prefix_map(prefix_map)
                if isinstance(source, RdfSource):
                    source.set_predicate_mapping(predicate_mappings)
                    source.set_node_property_predicates(node_property_predicates)
                source.set_node_filters(node_filters)
                self.node_filters = source.node_filters
                self.edge_filters = source.edge_filters
                source.set_edge_filters(edge_filters)
                self.node_filters = source.node_filters
                self.edge_filters = source.edge_filters

                default_provenance = os.path.basename(f)

                g = source.parse(f, default_provenance=default_provenance, **input_args)

                sources.append(source)
                generators.append(g)

        source_generator = itertools.chain(*generators)

        if output_args:
            if self.stream:
                if output_args["format"] in {"tsv", "csv"}:
                    if "node_properties" not in output_args:
                        log.warning(
                            f"'node_properties' not defined for output while streaming. "
                            f"The exported {output_args['format']} will be limited to a subset of the columns."
                        )
                    if "edge_properties" not in output_args:
                        log.warning(
                            f"'edge_properties' not defined for output while streaming. "
                            f"The exported {output_args['format']} will be limited to a subset of the columns."
                        )
                sink = self.get_sink(**output_args)
                if "reverse_prefix_map" in output_args:
                    sink.set_reverse_prefix_map(output_args["reverse_prefix_map"])
                if isinstance(sink, RdfSink):
                    if "reverse_predicate_mapping" in output_args:
                        sink.set_reverse_predicate_mapping(
                            output_args["reverse_predicate_mapping"]
                        )
                    if "property_types" in output_args:
                        sink.set_property_types(output_args["property_types"])
                # stream from source to sink
                self.process(source_generator, sink)
                sink.finalize()
            else:
                # stream from source to intermediate
                intermediate_sink = GraphSink(self.store.graph)
                intermediate_sink.node_properties.update(self.store.node_properties)
                intermediate_sink.edge_properties.update(self.store.edge_properties)
                self.process(source_generator, intermediate_sink)
                for s in sources:
                    intermediate_sink.node_properties.update(s.node_properties)
                    intermediate_sink.edge_properties.update(s.edge_properties)
                apply_graph_operations(intermediate_sink.graph, operations)
                # stream from intermediate to output sink
                intermediate_source = self.get_source("graph")
                intermediate_source.node_properties.update(
                    intermediate_sink.node_properties
                )
                intermediate_source.edge_properties.update(
                    intermediate_sink.edge_properties
                )

                # Need to propagate knowledge source specifications here?
                ks_args = dict()
                for ksf in knowledge_provenance_properties:
                    if ksf in input_args:
                        ks_args[ksf] = input_args[ksf]

                # TODO: does this call also need the default_provenance named argument?
                intermediate_source_generator = intermediate_source.parse(
                    intermediate_sink.graph, **ks_args
                )

                if output_args["format"] in {"tsv", "csv"}:
                    if "node_properties" not in output_args:
                        output_args[
                            "node_properties"
                        ] = intermediate_source.node_properties
                    if "edge_properties" not in output_args:
                        output_args[
                            "edge_properties"
                        ] = intermediate_source.edge_properties
                    sink = self.get_sink(**output_args)
                    if "reverse_prefix_map" in output_args:
                        sink.set_reverse_prefix_map(output_args["reverse_prefix_map"])
                    if isinstance(sink, RdfSink):
                        if "reverse_predicate_mapping" in output_args:
                            sink.set_reverse_predicate_mapping(
                                output_args["reverse_predicate_mapping"]
                            )
                    if "property_types" in output_args:
                        sink.set_property_types(output_args["property_types"])
                else:
                    sink = self.get_sink(**output_args)
                    sink.node_properties.update(intermediate_source.node_properties)
                    sink.edge_properties.update(intermediate_source.edge_properties)

                self.process(intermediate_source_generator, sink)
                sink.finalize()
                self.store.node_properties.update(sink.node_properties)
                self.store.edge_properties.update(sink.edge_properties)
        else:
            # stream from source to intermediate
            sink = GraphSink(self.store.graph)
            self.process(source_generator, sink)
            sink.node_properties.update(self.store.node_properties)
            sink.edge_properties.update(self.store.edge_properties)
            for s in sources:
                sink.node_properties.update(s.node_properties)
                sink.edge_properties.update(s.edge_properties)
            sink.finalize()
            self.store.node_properties.update(sink.node_properties)
            self.store.edge_properties.update(sink.edge_properties)
            apply_graph_operations(sink.graph, operations)

        # Aggregate the InfoRes catalogs from  all sources
        for s in sources:
            for k, v in s.get_infores_catalog().items():
                self._infores_catalog[k] = v

    def get_infores_catalog(self):
        """
        Return catalog of Information Resource mappings
         aggregated from all Transformer associated sources
        """
        return self._infores_catalog

    def process(self, source: Generator, sink: Sink) -> None:
        """
        This method is responsible for reading from ``source``
        and writing to ``sink`` by calling the relevant methods
        based on the incoming data.

        .. note::
            The streamed data must not be mutated.

        Parameters
        ----------
        source: Generator
            A generator from a Source
        sink: kgx.sink.sink.Sink
            An instance of Sink

        """
        for rec in source:
            if rec:
                if len(rec) == 4:  # infer an edge record
                    write_edge = True
                    if "subject_category" in self.edge_filters:
                        if rec[0] in self._seen_nodes:
                            write_edge = True
                        else:
                            write_edge = False
                    if "object_category" in self.edge_filters:
                        if rec[1] in self._seen_nodes:
                            if "subject_category" in self.edge_filters:
                                if write_edge:
                                    write_edge = True
                            else:
                                write_edge = True
                        else:
                            write_edge = False
                    if write_edge:
                        if self.inspector:
                            self.inspector(GraphEntityType.EDGE, rec)
                        sink.write_edge(rec[-1])
                else:  # infer a node record
                    if "category" in self.node_filters:
                        self._seen_nodes.add(rec[0])
                    if self.inspector:
                        self.inspector(GraphEntityType.NODE, rec)
                    sink.write_node(rec[-1])

    # TODO: review whether or not the 'save()' method need to be 'knowledge_source' aware?
    def save(self, output_args: Dict) -> None:
        """
        Save data from the in-memory store to a desired sink.

        Parameters
        ----------
        output_args: Dict
            Arguments relevant to your output sink

        """
        if not self.store:
            raise Exception("self.store is empty.")
        source = self.store
        source.node_properties.update(self.store.node_properties)
        source.edge_properties.update(self.store.edge_properties)
        source_generator = source.parse(self.store.graph)
        if "node_properties" not in output_args:
            output_args["node_properties"] = source.node_properties
        if "edge_properties" not in output_args:
            output_args["edge_properties"] = source.edge_properties
        sink = self.get_sink(**output_args)
        sink.node_properties.update(source.node_properties)
        sink.edge_properties.update(source.edge_properties)
        if "reverse_prefix_map" in output_args:
            sink.set_reverse_prefix_map(output_args["reverse_prefix_map"])
        if isinstance(sink, RdfSink):
            if "reverse_predicate_mapping" in output_args:
                sink.set_reverse_predicate_mapping(
                    output_args["reverse_predicate_mapping"]
                )
            if "property_types" in output_args:
                sink.set_property_types(output_args["property_types"])
        self.process(source_generator, sink)
        sink.finalize()

    def get_source(self, format: str) -> Source:
        """
        Get an instance of Source that corresponds to a given format.

        Parameters
        ----------
        format: str
            The input store format

        Returns
        -------
        Source:
            An instance of kgx.source.Source

        """
        if format in SOURCE_MAP:
            s = SOURCE_MAP[format]
            return s()
        else:
            raise TypeError(f"{format} in an unrecognized format")

    def get_sink(self, **kwargs: Dict) -> Sink:
        """
        Get an instance of Sink that corresponds to a given format.

        Parameters
        ----------
        kwargs: Dict
            Arguments required for initializing an instance of Sink

        Returns
        -------
        Sink:
            An instance of kgx.sink.Sink

        """
        if kwargs["format"] in SINK_MAP:
            s = SINK_MAP[kwargs["format"]]
            return s(**kwargs)
        else:
            raise TypeError(f"{kwargs['format']} in an unrecognized format")
