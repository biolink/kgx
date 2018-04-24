from kgx import PandasTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = PandasTransformer()
    w = GraphMLTransformer()
    t.parse("tests/resources/x1n.csv")
    t.parse("tests/resources/x1e.csv")
    w.save("target/x1n.graphml")
