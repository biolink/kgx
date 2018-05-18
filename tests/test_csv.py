from kgx import PandasTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = PandasTransformer()
    t.parse("tests/resources/x1n.csv")
    t.parse("tests/resources/x1e.csv")
    t.report()
    t.save('target/x1copy.csv')
    w = GraphMLTransformer(t.graph)
    w.save("target/x1n.graphml")
