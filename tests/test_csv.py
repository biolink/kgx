from kgx import PandasTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = PandasTransformer()
    t.parse("tests/resources/x1n.csv")
    t.parse("tests/resources/x1e.csv")
    t.report()
    w = GraphMLTransformer(t)
    w.save("target/x1n.graphml")
