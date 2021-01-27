import os

import pytest

from kgx.sink.json_sink import JsonSink
from kgx.sink.nx_graph_sink import NxGraphSink
from kgx.source.json_source import JsonSource
from kgx.source.nx_graph_source import NxGraphSource
from kgx.source.tsv_source import TsvSource
from kgx.sink.tab_sink import TsvSink
from kgx.stream.stream import Stream
from kgx.transformers.configurable_transformer import ConfigurableTransformer, ConfigurableTransformer2, \
    ConfigurableTransformer3
from tests import print_graph

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_tab_transform():
    tt = Stream()
    # during stream, need to define ahead of time what the node properties
    # and edge properties are
    sink = TsvSink(
        filename=os.path.join(target_dir, 'my_graph'),
        format='tsv',
        node_properties=['id', 'name', 'category'],
        edge_properties=['subject', 'predicate', 'object', 'relation']
    )
    source = TsvSource()
    g = source.parse(
        filename=os.path.join(resource_dir, 'graph_nodes.tsv'),
        format='tsv'
    )
    tt.process(g, sink)
    g = source.parse(
        filename=os.path.join(resource_dir, 'graph_edges.tsv'),
        format='tsv'
    )
    tt.process(g, sink)


def test_json_transform():
    tt = Stream()
    sink = JsonSink(
        filename=os.path.join(target_dir, 'my_graph.json'),
        format='json',
    )
    source = TsvSource()
    g = source.parse(
        filename=os.path.join(resource_dir, 'graph_nodes.tsv'),
        format='tsv'
    )
    tt.process(g, sink)
    g = source.parse(
        filename=os.path.join(resource_dir, 'graph_edges.tsv'),
        format='tsv'
    )
    tt.process(g, sink)
    sink.finalize()


def test_nx_transform():
    input_channel = Stream()
    intermediate_sink = NxGraphSink()
    input_source = TsvSource()
    input_source_generator = input_source.parse(os.path.join(resource_dir, 'graph_nodes.tsv'), 'tsv')
    input_channel.process(input_source_generator, intermediate_sink)
    input_source_generator = input_source.parse(os.path.join(resource_dir, 'graph_edges.tsv'), 'tsv')
    input_channel.process(input_source_generator, intermediate_sink)

    intermediate_source = NxGraphSource()
    intermediate_source_generator = intermediate_source.parse(intermediate_sink.graph)

    output_sink = TsvSink(os.path.join(target_dir, 'my_graph'), 'tsv', None)
    output_channel = Stream()
    output_channel.process(intermediate_source_generator, output_sink)


def test_configurable_transformer1():
    input_files = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
       os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.transform(f, 'tsv')
    print_graph(cct.store.graph)

def test_configurable_transformer2():
    input_files = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
       os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'tsv')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'tsv')

def test_configurable_transformer3():
    input_files = [
        os.path.join(resource_dir, 'cm_nodes.csv'),
       os.path.join(resource_dir, 'cm_edges.csv')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'csv')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'tsv')

def test_configurable_transformer4():
    input_files = [
        os.path.join(resource_dir, 'test.tar.gz'),
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'tsv', 'tar.gz')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'tsv')

def test_json_source():
    input_files = [
        os.path.join(resource_dir, '../../chebi_kgx.json.gz'),
    ]
    js = JsonSource()
    for f in input_files:
        for r in js.parse(f, 'json', compression='gz'):
            print(r)


@pytest.mark.skip()
def test_configurable_transformer5():
    # Tough to implement a stream export of JSON
    input_files = [
        os.path.join(resource_dir, 'valid.json'),
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'json')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'json')


def test_configurable_transformer6():
    input_files = [
        os.path.join(resource_dir, 'valid_nodes.jsonl'),
        os.path.join(resource_dir, 'valid_edges.jsonl')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'jsonl')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'jsonl')

def test_configurable_transformer7():
    input_files = [
        os.path.join(resource_dir, 'valid_nodes.jsonl'),
        os.path.join(resource_dir, 'valid_edges.jsonl')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'jsonl')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'jsonl', compression='gz')


def test_configurable_transformer8():
    input_files = [
        os.path.join(resource_dir, 'goslim_generic.json')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'obojson')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'jsonl')


def test_configurable_transformer9():
    input_files = [
        os.path.join(resource_dir, 'rsa_sample.json')
    ]
    cct = ConfigurableTransformer()
    for f in input_files:
        cct.load(f, 'trapi-json')
    print_graph(cct.store.graph)
    cct.save('my_new_graph', 'jsonl')


def test_configurable_transformer10():
    cct = ConfigurableTransformer2()
    cct.load('http://localhost:9000', 'neo4j', 'admin')
    print_graph(cct.store.graph)
    cct.save('my_chebi_graph', 'jsonl')


def test_configurable_transformer11():
    cct = ConfigurableTransformer2(stream=True)
    cct.transform('http://localhost:9000', 'neo4j', 'admin', 'my-streamed-chebi-graph', 'jsonl')


def test_configurable_transformer12():
    cct = ConfigurableTransformer3(stream=False)
    cct.transform('http://localhost:9000', 'neo4j', 'admin', 'http://localhost:8484', 'neo4j', 'test')
    #print_graph(cct.store.graph)


def test_configurable_transformer13():
    cct = ConfigurableTransformer3(stream=True)
    cct.transform('http://localhost:9000', 'neo4j', 'admin', 'http://localhost:8484', 'neo4j', 'test')


def test_configurable_transformer14():
    cct = ConfigurableTransformer(stream=True)
    #cct.transform(os.path.join(resource_dir, '../../','orphanet-export.sorted.nt'), 'nt', None, 'my_orpha_graph', 'jsonl')
    cct.transform(os.path.join(resource_dir, '../../', 'chebi_sorted.nt'), 'nt', None, 'my_orpha_graph',
                  'jsonl')


def test_configurable_transformer15():
    cct = ConfigurableTransformer(stream=True)
    #cct.transform(os.path.join(resource_dir, '../../', 'chebi_sorted.nt'), 'nt', None, 'my_chebi_graph', 'nt')
    cct.transform(os.path.join(resource_dir, '../../', 'orphanet-export.sorted.nt'), 'nt', None, 'my_orpha_graph.nt', 'nt')


