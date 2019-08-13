from kgx import NeoTransformer, PandasTransformer

def test_csv_to_neo_load():
    """
    load csv to neo4j test
    """
    pt = PandasTransformer()
    pt.parse("tests/resources/x1n.csv")
    pt.parse("tests/resources/x1e.csv")
    nt = NeoTransformer(pt.graph, host='http://localhost', port='7474', username='neo4j', password='test')
    nt.save_with_unwind()
    nt.neo4j_report()

def test_neo_to_graph_transform():
    """
    load from neo4j and transform to nx graph
    """

    nt = NeoTransformer(host='http://localhost', port='7474', username='neo4j', password='test')
    nt.load()
    nt.report()
    t = PandasTransformer(nt.graph)
    t.save("target/neo_graph.csv")
