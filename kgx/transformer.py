import itertools
import os
from typing import Dict, Union, Generator, List

from kgx.config import get_logger
from kgx.sink import GraphSink, Sink, TsvSink, JsonSink, JsonlSink, NeoSink, RdfSink
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
)
from kgx.source.sssom_source import SssomSource
from kgx.source.owl_source import OwlSource
from kgx.utils.kgx_utils import apply_graph_operations

SOURCE_MAP = {
    'tsv': TsvSource,
    'csv': TsvSource,
    'graph': GraphSource,
    'json': JsonSource,
    'jsonl': JsonlSource,
    'obojson': ObographSource,
    'obo-json': ObographSource,
    'trapi-json': TrapiSource,
    'neo4j': NeoSource,
    'nt': RdfSource,
    'owl': OwlSource,
    'sssom': SssomSource,
}

SINK_MAP = {
    'tsv': TsvSink,
    'csv': TsvSink,
    'graph': GraphSink,
    'json': JsonSink,
    'jsonl': JsonlSink,
    'neo4j': NeoSink,
    'nt': RdfSink,
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

    """

    def __init__(self, stream: bool = False):
        self.stream = stream
        self.node_filters = {}
        self.edge_filters = {}
        self.store = self.get_source('graph')
        self._seen_nodes = set()

    def transform(self, input_args: Dict, output_args: Dict = None) -> None:
        """
        Transform an input source and write to an output sink.

        If ``output_args`` is not defined then the data is persisted to
        an in-memory graph.

        Parameters
        ----------
        input_args: Dict
            Arguments relevant to your input source
        output_args: Dict
            Arguments relevant to your output sink

        """
        sources = []
        generators = []
        input_format = input_args['format']
        prefix_map = input_args.pop('prefix_map', {})
        predicate_mappings = input_args.pop('predicate_mappings', {})
        node_property_predicates = input_args.pop('node_property_predicates', {})
        node_filters = input_args.pop('node_filters', {})
        edge_filters = input_args.pop('edge_filters', {})
        operations = input_args.pop('operations', [])

        if input_format in {'neo4j', 'graph'}:
            source = self.get_source(input_format)
            source.set_prefix_map(prefix_map)
            source.set_node_filters(node_filters)
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters
            source.set_edge_filters(edge_filters)
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters
            if 'provided_by' not in input_args:
                if 'name' in input_args:
                    input_args['provided_by'] = input_args['name']
                else:
                    if 'uri' in input_args:
                        input_args['provided_by'] = input_args['uri']
            g = source.parse(**input_args)
            sources.append(source)
            generators.append(g)
        else:
            filename = input_args.pop('filename', {})
            provided_by = input_args.pop('provided_by', None)
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
                if not provided_by:
                    if 'name' in input_args:
                        provided_by = input_args.pop('name')
                    else:
                        provided_by = os.path.basename(f)
                g = source.parse(f, **input_args)
                sources.append(source)
                generators.append(g)

        source_generator = itertools.chain(*generators)

        if output_args:
            if self.stream:
                if output_args['format'] in {'tsv', 'csv'}:
                    if 'node_properties' not in output_args:
                        log.warning(
                            f"'node_properties' not defined for output while streaming. The exported {output_args['format']} will be limited to a subset of the columns."
                        )
                    if 'edge_properties' not in output_args:
                        log.warning(
                            f"'edge_properties' not defined for output while streaming. The exported {output_args['format']} will be limited to a subset of the columns."
                        )
                sink = self.get_sink(**output_args)
                if 'reverse_prefix_map' in output_args:
                    sink.set_reverse_prefix_map(output_args['reverse_prefix_map'])
                if isinstance(sink, RdfSink):
                    if 'reverse_predicate_mapping' in output_args:
                        sink.set_reverse_predicate_mapping(output_args['reverse_predicate_mapping'])
                    if 'property_types' in output_args:
                        sink.set_property_types(output_args['property_types'])
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
                intermediate_source = self.get_source('graph')
                intermediate_source.node_properties.update(intermediate_sink.node_properties)
                intermediate_source.edge_properties.update(intermediate_sink.edge_properties)
                intermediate_source_generator = intermediate_source.parse(intermediate_sink.graph)

                if output_args['format'] in {'tsv', 'csv'}:
                    if 'node_properties' not in output_args:
                        output_args['node_properties'] = intermediate_source.node_properties
                    if 'edge_properties' not in output_args:
                        output_args['edge_properties'] = intermediate_source.edge_properties
                    sink = self.get_sink(**output_args)
                    if 'reverse_prefix_map' in output_args:
                        sink.set_reverse_prefix_map(output_args['reverse_prefix_map'])
                    if isinstance(sink, RdfSink):
                        if 'reverse_predicate_mapping' in output_args:
                            sink.set_reverse_predicate_mapping(
                                output_args['reverse_predicate_mapping']
                            )
                    if 'property_types' in output_args:
                        sink.set_property_types(output_args['property_types'])
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
                if len(rec) == 4:
                    write_edge = True
                    if 'subject_category' in self.edge_filters:
                        if rec[0] in self._seen_nodes:
                            write_edge = True
                        else:
                            write_edge = False
                    if 'object_category' in self.edge_filters:
                        if rec[1] in self._seen_nodes:
                            if 'subject_category' in self.edge_filters:
                                if write_edge:
                                    write_edge = True
                            else:
                                write_edge = True
                        else:
                            write_edge = False
                    if write_edge:
                        sink.write_edge(rec[-1])
                else:
                    if 'category' in self.node_filters:
                        self._seen_nodes.add(rec[0])
                    sink.write_node(rec[-1])

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
        if 'node_properties' not in output_args:
            output_args['node_properties'] = source.node_properties
        if 'edge_properties' not in output_args:
            output_args['edge_properties'] = source.edge_properties
        sink = self.get_sink(**output_args)
        sink.node_properties.update(source.node_properties)
        sink.edge_properties.update(source.edge_properties)
        if 'reverse_prefix_map' in output_args:
            sink.set_reverse_prefix_map(output_args['reverse_prefix_map'])
        if isinstance(sink, RdfSink):
            if 'reverse_predicate_mapping' in output_args:
                sink.set_reverse_predicate_mapping(output_args['reverse_predicate_mapping'])
            if 'property_types' in output_args:
                sink.set_property_types(output_args['property_types'])
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
        if kwargs['format'] in SINK_MAP:
            s = SINK_MAP[kwargs['format']]
            return s(**kwargs)
        else:
            raise TypeError(f"{kwargs['format']} in an unrecognized format")
