import os

from kgx import PandasTransformer, GraphMLTransformer

def test_load():
    """
    load tests
    """
    t = PandasTransformer()
    cwd = os.path.abspath(os.path.dirname(__file__))
    resdir = os.path.join(cwd, 'resources')
    targetdir = os.path.join(cwd, 'target')
    os.makedirs(targetdir, exist_ok=True)

    t.parse(os.path.join(resdir, "x1n.csv"))
    t.parse(os.path.join(resdir, "x1e.csv"))
    t.report()
    t.save(os.path.join(targetdir, 'x1copy.csv'))
    w = GraphMLTransformer(t.graph)
    w.save(os.path.join(targetdir, "x1n.graphml"))
