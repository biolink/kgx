from kgx import SparqlTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = SparqlTransformer()
    t.set_filter('predicate', 'foo')
    t.set_filter('subject_category', 'gene')
    t.load_edges()
