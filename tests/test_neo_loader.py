from kgx import NeoTransformer, PandasTransformer

def test_csv_to_neo_load():
    """
    load csv to neo4j test
    """

    n = NeoTransformer()
    n.save_from_csv("tests/resources/x1n.csv", "tests/resources/x1e.csv")

def test_graph_to_neo_load():
    """
    load nx graph to neo4j test
    """

    t = PandasTransformer()
    t.parse("tests/resources/x1n.csv")
    t.parse("tests/resources/x1e.csv")
    t.report()
    n = NeoTransformer(t)
    n.save()