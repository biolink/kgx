from kgx import SparqlTransformer

def test_dummy():
    pass

def TODO_test_load():
    """
    load tests
    """
    t = SparqlTransformer()
    t.set_filter('predicate', 'foo')
    t.set_filter('subject_category', 'gene')
    t.load_edges()
