import os
from typing import Dict, Union, Generator

from kgx.config import get_logger
from kgx.sink import GraphSink, Sink, TsvSink, JsonSink, JsonlSink, NeoSink, RdfSink
from kgx.source import GraphSource, Source, TsvSource, JsonSource, JsonlSource, ObographSource, TrapiSource, NeoSource, RdfSource
from kgx.source.owl_source import OwlSource

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
    'owl': OwlSource
}

SINK_MAP = {
    'tsv': TsvSink,
    'csv': TsvSink,
    'graph': GraphSink,
    'json': JsonSink,
    'jsonl': JsonlSink,
    'neo4j': NeoSink,
    'nt': RdfSink
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
        input_format = input_args['format']
        if 'provided_by' not in input_format:
            if 'name' in input_args:
                input_args['provided_by'] = input_args['name']
            elif 'uri' in input_args:
                input_args['provided_by'] = input_args['uri']
            else:
                input_args['provided_by'] = os.path.basename(input_args['filename'])

        source = self.get_source(input_format)
        if 'prefix_map' in input_args:
            source.set_prefix_map(input_args['prefix_map'])
        if isinstance(source, RdfSource):
            if 'predicate_mappings' in input_args:
                source.set_predicate_mapping(input_args['predicate_mappings'])
            if 'node_property_predicates' in input_args:
                source.set_node_property_predicates(input_args['node_property_predicates'])
        if 'node_filters' in input_args:
            source.set_node_filters(input_args['node_filters'])
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters
            del input_args['node_filters']
        if 'edge_filters' in input_args:
            source.set_edge_filters(input_args['edge_filters'])
            self.node_filters = source.node_filters
            self.edge_filters = source.edge_filters
            del input_args['edge_filters']
        source_generator = source.parse(**input_args)
        if output_args:
            sink = self.get_sink(**output_args)
            if 'reverse_prefix_map' in output_args:
                sink.set_reverse_prefix_map(input_args['reverse_prefix_map'])
            if isinstance(source, RdfSink):
                if 'reverse_predicate_mapping' in output_args:
                    sink.set_reverse_predicate_mapping(output_args['reverse_predicate_mapping'])
            if 'property_types' in output_args:
                sink.set_property_types(output_args['property_types'])

            if self.stream:
                # stream from source to sink
                self.process(source_generator, sink)
            else:
                # stream from source to intermediate to output sink
                intermediate_sink = GraphSink(self.store.graph)
                self.process(source_generator, intermediate_sink)
                # self.apply_graph_operations
                intermediate_source = self.get_source('graph')
                intermediate_source_generator = intermediate_source.parse(intermediate_sink.graph)
                self.process(intermediate_source_generator, sink)
                sink.finalize()
        else:
            # stream from source to intermediate
            sink = GraphSink(self.store.graph)
            self.process(source_generator, sink)
            sink.finalize()
        # self.node_filters.clear()
        # self.edge_filters.clear()
        # self._seen_nodes.clear()

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
                    if 'subject_category' in self.edge_filters or 'object_category' in self.edge_filters:
                        # The assumption here is that the relevant nodes have already
                        # been seen and thus only load those edges for which both
                        # nodes have been loaded
                        if rec[0] in self._seen_nodes and rec[1] in self._seen_nodes:
                            log.info(f"EDGE: {rec[-1]}")
                            sink.write_edge(rec[-1])
                    else:
                        sink.write_edge(rec[-1])
                    # self.apply_stream_operations
                else:
                    if 'category' in self.node_filters:
                        self._seen_nodes.add(rec[0])
                    sink.write_node(rec[-1])
                    # self.apply_stream_operations

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
        source = self.get_source('graph')
        source_generator = source.parse(self.store.graph)
        sink = self.get_sink(**output_args)
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
