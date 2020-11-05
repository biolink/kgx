import os

from kgx import JsonlTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_jsonl_load():
    jlt = JsonlTransformer()
    jlt.parse(os.path.join(resource_dir, 'valid_nodes.jsonl'), input_format='jsonl')
    jlt.parse(os.path.join(resource_dir, 'valid_edges.jsonl'), input_format='jsonl')

    assert jlt.graph.number_of_nodes() == 6
    assert jlt.graph.number_of_edges() == 5

    n1 = jlt.graph.nodes['HGNC:11603']
    assert n1['name'] == 'TBX4'
    assert 'biolink:Gene' in n1['category']

    e1 = list(jlt.graph.get_edge_data('HGNC:11603', 'MONDO:0005002').values())[0]
    assert e1['subject'] == 'HGNC:11603'
    assert e1['object'] == 'MONDO:0005002'
    assert e1['predicate'] == 'biolink:related_to'
    assert e1['relation'] == 'RO:0003304'


def test_jsonl_save1():
    jlt = JsonlTransformer()
    jlt.parse(os.path.join(resource_dir, 'valid_nodes.jsonl'), input_format='jsonl')
    jlt.parse(os.path.join(resource_dir, 'valid_edges.jsonl'), input_format='jsonl')

    assert jlt.graph.number_of_nodes() == 6
    assert jlt.graph.number_of_edges() == 5

    jlt.save(os.path.join(target_dir, 'valid-export'))
    assert os.path.exists(os.path.join(target_dir, 'valid-export_nodes.jsonl'))
    assert os.path.exists(os.path.join(target_dir, 'valid-export_edges.jsonl'))

    jlt.save(os.path.join(target_dir, 'valid-export'), compression='gz')
    assert os.path.exists(os.path.join(target_dir, 'valid-export_nodes.jsonl.gz'))
    assert os.path.exists(os.path.join(target_dir, 'valid-export_edges.jsonl.gz'))

    jlt2 = JsonlTransformer()
    jlt2.parse(os.path.join(target_dir, 'valid-export_nodes.jsonl'))
    jlt2.parse(os.path.join(target_dir, 'valid-export_edges.jsonl'))

    assert jlt2.graph.number_of_nodes() == 6
    assert jlt2.graph.number_of_edges() == 5

    jlt3 = JsonlTransformer()
    jlt3.parse(os.path.join(target_dir, 'valid-export_nodes.jsonl.gz'), compression='gz')
    jlt3.parse(os.path.join(target_dir, 'valid-export_edges.jsonl.gz'), compression='gz')

    assert jlt3.graph.number_of_nodes() == 6
    assert jlt3.graph.number_of_edges() == 5