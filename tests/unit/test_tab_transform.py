import os

from kgx.sink.json_sink import JsonSink
from kgx.sink.graph_sink import GraphSink
from kgx.source.json_source import JsonSource
from kgx.source.graph_source import GraphSource
from kgx.source.tsv_source import TsvSource
from kgx.sink.tsv_sink import TsvSink
from kgx.stream import Stream
from kgx.new_transformer import Transformer
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
        compression='gz'
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
    intermediate_sink = GraphSink()
    input_source = TsvSource()
    input_source_generator = input_source.parse(os.path.join(resource_dir, 'graph_nodes.tsv'), 'tsv')
    input_channel.process(input_source_generator, intermediate_sink)
    input_source_generator = input_source.parse(os.path.join(resource_dir, 'graph_edges.tsv'), 'tsv')
    input_channel.process(input_source_generator, intermediate_sink)

    intermediate_source = GraphSource()
    intermediate_source_generator = intermediate_source.parse(intermediate_sink.graph)

    output_sink = TsvSink(os.path.join(target_dir, 'my_graph'), 'tsv', None)
    output_channel = Stream()
    output_channel.process(intermediate_source_generator, output_sink)


def test_configurable_transformer1():
    input_files = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
       os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'tsv'}
        t.transform(input_args)
    print_graph(t.store.graph)


def test_configurable_transformer2():
    input_files = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
       os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'tsv'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': os.path.join(target_dir, 'my_new_graph'), 'format': 'tsv'}
    # TODO: empty output due to node_properties and edge_properties being empty
    t.save(output_args)


def test_configurable_transformer3():
    input_files = [
        os.path.join(resource_dir, 'cm_nodes.csv'),
       os.path.join(resource_dir, 'cm_edges.csv')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'csv'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': os.path.join(target_dir, 'my_new_graph'), 'format': 'tsv'}
    t.save(output_args)


def test_configurable_transformer4():
    input_files = [
        os.path.join(resource_dir, 'test.tar.gz'),
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'tsv', 'compression': 'tar.gz'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'tsv'}
    t.save(output_args)


def test_json_source():
    input_files = [
        os.path.join(resource_dir, '../../chebi_kgx.json.gz'),
    ]
    js = JsonSource()
    for f in input_files:
        for r in js.parse(f, 'json', compression='gz'):
            print(r)


def test_configurable_transformer5():
    # Tough to implement a stream export of JSON
    input_files = [
        os.path.join(resource_dir, 'valid.json'),
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'json'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'json'}
    t.save(output_args)


def test_configurable_transformer6():
    input_files = [
        os.path.join(resource_dir, 'valid_nodes.jsonl'),
        os.path.join(resource_dir, 'valid_edges.jsonl')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'jsonl'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'jsonl'}
    t.save(output_args)


def test_configurable_transformer7():
    input_files = [
        os.path.join(resource_dir, 'valid_nodes.jsonl'),
        os.path.join(resource_dir, 'valid_edges.jsonl')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'jsonl'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'jsonl', 'compression': 'gz'}
    t.save(output_args)


def test_configurable_transformer8():
    input_files = [
        os.path.join(resource_dir, 'goslim_generic.json')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'obojson'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'jsonl'}
    t.save(output_args)


def test_configurable_transformer9():
    input_files = [
        os.path.join(resource_dir, 'rsa_sample.json')
    ]
    t = Transformer()
    for f in input_files:
        input_args = {'filename': f, 'format': 'trapi-json'}
        t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_new_graph', 'format': 'jsonl'}
    t.save(output_args)


def test_configurable_transformer10():
    t = Transformer()
    input_args = {'format': 'neo4j', 'uri': 'http://localhost:9000', 'username': 'neo4j', 'password': 'admin'}
    t.transform(input_args)
    print_graph(t.store.graph)
    output_args = {'filename': 'my_chebi_graph', 'format': 'jsonl'}
    t.save(output_args)


def test_configurable_transformer11():
    t = Transformer(stream=True)
    input_args = {'format': 'neo4j', 'uri': 'http://localhost:9000', 'username': 'neo4j', 'password': 'admin'}
    output_args = {'filename': 'my-streamed-chebi-graph', 'format': 'jsonl'}
    t.transform(input_args, output_args)


def test_configurable_transformer12():
    t = Transformer(stream=False)
    input_args = {'format': 'neo4j', 'uri': 'http://localhost:9000', 'username': 'neo4j', 'password': 'admin'}
    output_args = {'format': 'neo4j', 'uri': 'http://localhost:8484', 'username': 'neo4j', 'password': 'test'}
    t.transform(input_args, output_args)
    #print_graph(cct.store.graph)


def test_configurable_transformer13():
    t = Transformer(stream=True)
    input_args = {'format': 'neo4j', 'uri': 'http://localhost:9000', 'username': 'neo4j', 'password': 'admin'}
    output_args = {'format': 'neo4j', 'uri': 'http://localhost:8484', 'username': 'neo4j', 'password': 'test'}
    t.transform(input_args, output_args)


def test_configurable_transformer14():
    t = Transformer(stream=True)
    input_args = {'filename': os.path.join(resource_dir, '../../', 'chebi_sorted.nt'), 'format': 'nt'}
    output_args = {'filename': 'my_orpha_graph.nt', 'format': 'jsonl'}
    t.transform(input_args, output_args)


def test_configurable_transformer15():
    t = Transformer(stream=True)
    input_args = {'filename': os.path.join(resource_dir, '../../', 'orphanet-export.sorted.nt'), 'format': 'nt'}
    output_args = {'filename': 'my_orpha_graph.nt', 'format': 'nt'}
    t.transform(input_args, output_args)



