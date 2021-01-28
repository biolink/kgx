from typing import Dict, Union

from kgx.sink import GraphSink, Sink, TsvSink, JsonSink, JsonlSink, NeoSink, RdfSink
from kgx.source import GraphSource, Source, TsvSource, JsonSource, JsonlSource, ObographSource, TrapiSource, NeoSource, \
    RdfSource
from kgx.stream import Stream

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
    'nt': RdfSource
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


class Transformer(object):
    """
    The Transformer class is responsible for transforming data from one
    form to another.

    Parameters
    ----------
    stream: bool
        Whether or not to stream
    node_filters: Dict
        Node filters
    edge_filters: Dict
        Edge filters

    """

    def __init__(self, stream: bool = False, node_filters: Dict = None, edge_filters: Dict = None):
        self.stream = stream
        self.node_filters = node_filters
        self.edge_filters = edge_filters
        if self.stream:
            self.store = None
        else:
            self.store = self.get_source('graph')

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
        source = self.get_source(input_format)
        source_generator = source.parse(**input_args)
        if output_args:
            sink = self.get_sink(**output_args)
            if self.stream:
                # stream from source to sink
                stream = Stream()
                stream.process(source_generator, sink)
            else:
                # stream from source to intermediate to output sink
                intermediate_sink = GraphSink(self.store.graph)
                input_stream = Stream()
                input_stream.process(source_generator, intermediate_sink)
                intermediate_source = self.get_source('graph')
                intermediate_source_generator = intermediate_source.parse(intermediate_sink.graph)
                output_stream = Stream()
                output_stream.process(intermediate_source_generator, sink)
        else:
            # stream from source to intermediate
            sink = GraphSink(self.store.graph)
            input_stream = Stream()
            input_stream.process(source_generator, sink)

    def save(self, output_args: Dict) -> None:
        """
        Save data from the in-memory store to a desired sink.

        Parameters
        ----------
        output_args: Dict
            Arguments relevant to your output sink

        """
        stream = Stream()
        if not self.store:
            raise Exception("self.store is empty.")
        source = self.get_source('graph')
        source_generator = source.parse(self.store.graph)
        sink = self.get_sink(**output_args)
        stream.process(source_generator, sink)

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

    def set_node_filter(self, key: str, value: Union[str, set]) -> None:
        """
        Set a node filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

        .. note::
            When defining the 'category' filter, the value should be of type ``set``.
            This method also sets the 'subject_category' and 'object_category'
            edge filters, to get a consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for node filter
        value: Union[str, set]
            The value for the node filter.
            Can be either a string or a set.

        """
        if key == 'category':
            if isinstance(value, set):
                if 'subject_category' in self.edge_filters:
                    self.edge_filters['subject_category'].update(value)
                else:
                    self.edge_filters['subject_category'] = value
                if 'object_category' in self.edge_filters:
                    self.edge_filters['object_category'].update(value)
                else:
                    self.edge_filters['object_category'] = value
            else:
                raise TypeError("'category' node filter should have a value of type 'set'")

        if key in self.node_filters:
            self.node_filters[key].update(value)
        else:
            self.node_filters[key] = value

    def set_edge_filter(self, key: str, value: set) -> None:
        """
        Set an edge filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

        .. note::
            When defining the 'subject_category' or 'object_category' filter,
            the value should be of type ``set``.
            This method also sets the 'category' node filter, to get a
            consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for edge filter
        value: Union[str, set]
            The value for the edge filter.
            Can be either a string or a set.

        """
        if key in {'subject_category', 'object_category'}:
            if isinstance(value, set):
                if 'category' in self.node_filters:
                    self.node_filters['category'].update(value)
                else:
                    self.node_filters['category'] = value
            else:
                raise TypeError(f"'{key}' edge filter should have a value of type 'set'")

        if key in self.edge_filters:
            self.edge_filters[key].update(value)
        else:
            self.edge_filters[key] = value