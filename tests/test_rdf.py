from kgx import ObanRdfTransformer, PandasTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = ObanRdfTransformer()
    t.parse("tests/resources/monarch/biogrid_test.ttl")
    t.report()
    w1 = PandasTransformer(t)
    w1.save('target/bgcopy.csv', type='e')
    w2 = GraphMLTransformer(t)
    w2.save("target/x1n.graphml")
