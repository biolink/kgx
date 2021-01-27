import os
from typing import Dict, Any, Union

from kgx.stream.stream import Stream

from kgx.config import get_logger
from kgx.sink.json_sink import JsonSink
from kgx.sink.jsonl_sink import JsonlSink
from kgx.sink.neo_sink import NeoSink
from kgx.sink.nx_graph_sink import NxGraphSink
from kgx.sink.rdf_sink import RdfSink
from kgx.sink.tab_sink import TsvSink
from kgx.source.json_source import JsonSource
from kgx.source.jsonl_source import JsonlSource
from kgx.source.neo_source import NeoSource
from kgx.source.nx_graph_source import NxGraphSource
from kgx.source.obojson_source import ObojsonSource
from kgx.source.rdf_source import RdfSource
from kgx.source.tsv_source import TsvSource
from kgx.source.trapi_source import TrapiSource

SOURCE_MAP = {
    'tsv': TsvSource,
    'csv': TsvSource,
    'graph': NxGraphSource,
    'json': JsonSource,
    'jsonl': JsonlSource,
    'obojson': ObojsonSource,
    'trapi-json': TrapiSource,
    'neo4j': NeoSource,
    'nt': RdfSource
}

SINK_MAP = {
    'tsv': TsvSink,
    'csv': TsvSink,
    'graph': NxGraphSink,
    'json': JsonSink,
    'jsonl': JsonlSink,
    'neo4j': NeoSink,
    'nt': RdfSink
}


log = get_logger()


class ConfigurableTransformer(object):
    def __init__(self, stream = False):
        self.stream = stream
        self.node_filters: Dict[str, Any] = {}
        self.edge_filters: Dict[str, Any] = {}
        self._node_properties = set()
        self._edge_properties = set()
        if self.stream:
            self.store = None
        else:
            self.store = self.get_source('graph')()

    # Responsible for orchestrating the read, transient store, write

    def load(self, input_filename, input_format, compression = None):
        input_stream = Stream()
        intermediate_sink = self.get_sink('graph')()
        intermediate_sink.graph = self.store.graph
        input_source = self.get_source(input_format)()
        input_source_generator = input_source.parse(input_filename, input_format, compression)
        input_stream.process(input_source_generator, intermediate_sink)
        self._node_properties.update(input_source._node_properties)
        self._edge_properties.update(input_source._edge_properties)
        self.store = intermediate_sink

    def transform(self, input_filename, input_format, input_compression = None, output_filename = None, output_format = None, output_compression = None):
        if self.stream:
            input_stream = Stream()
            # TODO: The type of source/sink can be inferred based
            #  on input_format/output_format
            input_source = self.get_source(input_format)()
            input_source_generator = input_source.parse(input_filename, input_format, input_compression)
            output_sink = self.get_sink(output_format)(output_filename, output_format, output_compression)
            input_stream.process(input_source_generator, output_sink)
        else:
            # TODO: handle node and edge properties when not streaming
            self.load(input_filename, input_format)
            self.apply_operations()
            if output_filename:
                self.save(output_filename, output_format)

    def save(self, output_filename, output_format, compression = None):
        intermediate_source = self.get_source('graph')()
        intermediate_source_generator = intermediate_source.parse(self.store.graph)
        output_sink = self.get_sink(output_format)({'filename': output_filename, 'format': output_format, 'compression': compression, 'node_properties': self._node_properties, 'edge_properties': self._edge_properties})
        output_stream = Stream()
        output_stream.process(intermediate_source_generator, output_sink)

    def apply_operations(self):
        pass

    def get_source(self, format):
        if format in SOURCE_MAP:
            return SOURCE_MAP[format]
        else:
            raise TypeError(f"{format} is an unrecognized format")

    def get_sink(self, format):
        if format in SINK_MAP:
            return SINK_MAP[format]
        else:
            raise TypeError(f"{format} in an unrecognized format")

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

    def check_node_filter(self, node: Dict) -> bool:
        """
        Check if a node passes defined node filters.

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        bool
            Whether the given node has passed all defined node filters

        """
        pass_filter = False
        if self.node_filters:
            for k, v in self.node_filters.items():
                if k in node:
                    # filter key exists in node
                    if isinstance(v, (list, set, tuple)):
                        if any(x in node[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if node[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} node filter of type {type(v)}")
                        return False
                else:
                    # filter key does not exist in node
                    return False
        else:
            # no node filters defined
            pass_filter = True
        return pass_filter

    def check_edge_filter(self, edge: Dict) -> bool:
        """
        Check if an edge passes defined edge filters.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        bool
            Whether the given edge has passed all defined edge filters
        """
        pass_filter = False
        if self.edge_filters:
            for k, v in self.edge_filters.items():
                if k in {'subject_category', 'object_category'}:
                    continue
                if k in edge:
                    # filter key exists in edge
                    if isinstance(v, (list, set, tuple)):
                        if any(x in edge[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if edge[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} edge filter of type {type(v)}")
                        return False
                else:
                    # filter does not exist in edge
                    return False

            # Check for subject and object filter
            if 'subject_category' in self.edge_filters:
                if self.stream:
                    log.warning(f"Cannot filter for 'subject_category' when streaming")
                else:
                    if self.store.graph.has_node(edge['subject']):
                        subject_node = self.store.graph.nodes()[edge['subject']]
                    else:
                        subject_node = None

                    f = self.edge_filters['subject_category']
                    if subject_node:
                        # subject node exists in graph
                        if any(x in subject_node['category'] for x in f):
                            pass_filter = True
                        else:
                            return False
                    else:
                        # subject node does not exist in graph
                        return False

            if 'object_category' in self.edge_filters:
                if self.stream:
                    log.warning(f"Cannot filter for 'object_category' when streaming")
                else:
                    if self.store.graph.has_node(edge['object']):
                        object_node = self.store.graph.nodes()[edge['object']]
                    else:
                        object_node = None

                    f = self.edge_filters['object_category']
                    if object_node:
                        # object node exists in graph
                        if any(x in object_node['category'] for x in f):
                            pass_filter = True
                        else:
                            return False
                    else:
                        # object node does not exist in graph
                        return False
        else:
            # no edge filters defined
            pass_filter = True
        return pass_filter


class ConfigurableTransformer2(object):
    def __init__(self, stream = False):
        self.stream = stream
        self._node_properties = set()
        self._edge_properties = set()
        if self.stream:
            self.store = None
        else:
            self.store = self.get_source('graph')()

    def load(self, uri, username, password):
        input_stream = Stream()
        intermediate_sink = self.get_sink('graph')()
        intermediate_sink.graph = self.store.graph
        input_source = self.get_source('neo4j')()
        input_source_generator = input_source.parse(uri, username, password)
        input_stream.process(input_source_generator, intermediate_sink)
        #self._node_properties.update(input_source._node_properties)
        #self._edge_properties.update(input_source._edge_properties)
        self.store = intermediate_sink

    def transform(self, uri, username, password, output_filename = None, output_format = None, output_compression = None):
        if self.stream:
            input_stream = Stream()
            # TODO: The type of source/sink can be inferred based
            #  on input_format/output_format
            input_source = self.get_source('neo4j')()
            input_source_generator = input_source.parse(uri, username, password)
            output_sink = self.get_sink(output_format)(output_filename, output_format, output_compression)
            input_stream.process(input_source_generator, output_sink)
        else:
            self.load(uri, username, password)
            #elf.apply_operations()
            if output_filename:
                self.save(output_filename, output_format)

    def save(self, output_filename, output_format, compression = None):
        intermediate_source = self.get_source('graph')()
        intermediate_source_generator = intermediate_source.parse(self.store.graph)
        output_sink = self.get_sink(output_format)(output_filename, output_format, compression, node_properties=self._node_properties, edge_properties=self._edge_properties)
        output_stream = Stream()
        output_stream.process(intermediate_source_generator, output_sink)


    def get_source(self, format):
        if format in SOURCE_MAP:
            return SOURCE_MAP[format]
        else:
            raise TypeError(f"{format} is an unrecognized format")

    def get_sink(self, format):
        if format in SINK_MAP:
            return SINK_MAP[format]
        else:
            raise TypeError(f"{format} in an unrecognized format")

class ConfigurableTransformer3(object):
    def __init__(self, stream = False):
        self.stream = stream
        self._node_properties = set()
        self._edge_properties = set()
        if self.stream:
            self.store = None
        else:
            self.store = self.get_source('graph')()

    def load(self, uri, username, password):
        input_stream = Stream()
        intermediate_sink = self.get_sink('graph')()
        intermediate_sink.graph = self.store.graph
        input_source = self.get_source('neo4j')()
        input_source_generator = input_source.parse(uri, username, password)
        input_stream.process(input_source_generator, intermediate_sink)
        #self._node_properties.update(input_source._node_properties)
        #self._edge_properties.update(input_source._edge_properties)
        self.store = intermediate_sink

    def transform(self, s_uri, s_username, s_password, t_uri = None, t_username = None, t_password = None):
        if self.stream:
            input_stream = Stream()
            # TODO: The type of source/sink can be inferred based
            #  on input_format/output_format
            input_source = self.get_source('neo4j')()
            input_source_generator = input_source.parse(s_uri, s_username, s_password)
            output_sink = self.get_sink('neo4j')(t_uri, t_username, t_password)
            input_stream.process(input_source_generator, output_sink)
            print(f"INPUT SOURCE NODE COUNT: {len(input_source.seen_nodes)}")
            print(f"INPUT SOURCE EDGE COUNT: {input_source.edge_count}")
        else:
            self.load(s_uri, s_username, s_password)
            #elf.apply_operations()
            self.save(t_uri, t_username, t_password)


    def save(self, uri, username, password):
        intermediate_source = self.get_source('graph')()
        intermediate_source_generator = intermediate_source.parse(self.store.graph)
        output_sink = self.get_sink('neo4j')(uri, username, password)
        output_stream = Stream()
        output_stream.process(intermediate_source_generator, output_sink)

    def get_source(self, format):
        if format in SOURCE_MAP:
            return SOURCE_MAP[format]
        else:
            raise TypeError(f"{format} is an unrecognized format")

    def get_sink(self, format):
        if format in SINK_MAP:
            return SINK_MAP[format]
        else:
            raise TypeError(f"{format} in an unrecognized format")