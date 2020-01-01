from kgx import NeoTransformer, PandasTransformer, JsonTransformer

from neo4jrestclient.client import GraphDatabase as http_gdb, Node, Relationship
from neo4jrestclient.query import CypherException

def test_csv_to_neo_load():
    """
    load csv to neo4j test
    """
    return

    pt = PandasTransformer()
    pt.parse("resources/nodes.csv")
    pt.parse("resources/edges.csv")
    nt = NeoTransformer(pt.graph, host='localhost', port='7474', username='', password='')
    nt.save_with_unwind()
    nt.neo4j_report()

def test_neo_to_graph_transform():
    """
    load from neo4j and transform to nx graph
    """
    return

    nt = NeoTransformer(host='localhost', port='7474', username='', password='')
    nt.load()
    nt.report()
    t = PandasTransformer(nt.graph)
    t.save("target/neo_graph.csv")

def test_neo_to_graph_upload():
    """ loads a neo4j graph from a json file
    """
    #return

    jt = JsonTransformer()
    jt.parse('resources/robodb2.json')

    nt = NeoTransformer(jt.graph, host='localhost', port='7474', username='', password='')
    nt.save_with_unwind()
    nt.neo4j_report()

def test_neo_to_graph_download():
    """ downloads a neo4j graph
    """
    return

    subject_label = 'gene'
    object_label = None
    edge_type = None
    stop_after = 100

    output_transformer =  JsonTransformer()
    G = output_transformer.graph

    driver = http_gdb('http://localhost:7474', username='', password='')

    subject_label = ':`{}`'.format(subject_label) if isinstance(subject_label, str) else ''
    object_label = ':`{}`'.format(object_label) if isinstance(object_label, str) else ''
    edge_type = ':`{}`'.format(edge_type) if isinstance(edge_type, str) else ''

    match = 'match (n{})-[e{}]->(m{})'.format(subject_label, edge_type, object_label)

    results = driver.query('{} return count(*)'.format(match))

    print('Using cyper query: {} return n, e, m'.format(match))

    for a, in results:
        size = a
        break

    if size == 0:
        print('No data available')
        quit()

    page_size = 1_000

    skip_flag = False

    for i in range(0, size, page_size):
        q = '{} return n, e, m skip {} limit {}'.format(match, i, page_size)
        results = driver.query(q)

        for n, e, m in results:
            subject_attr = n['data']
            object_attr = m['data']
            edge_attr = e['data']

            if 'id' not in subject_attr or 'id' not in object_attr:
                if not skip_flag:
                    print('Skipping records that have no id attribute')
                    skip_flag = True
                continue

            s = subject_attr['id']
            o = object_attr['id']

            if 'edge_label' not in edge_attr:
                edge_attr['edge_label'] = e['metadata']['type']

            if 'category' not in subject_attr:
                subject_attr['category'] = n['metadata']['labels']

            if 'category' not in object_attr:
                object_attr['category'] = m['metadata']['labels']

            if s not in G:
                G.add_node(s, **subject_attr)
            if o not in G:
                G.add_node(o, **object_attr)

            G.add_edge(s, o, key=edge_attr['edge_label'], **edge_attr)

        if stop_after is not None and G.number_of_edges() > stop_after:
            break
