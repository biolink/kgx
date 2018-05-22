import os

from kgx import RdfOwlTransformer, PandasTransformer, JsonTransformer
import gzip


def test_load():
    """
    load tests
    """
    cwd = os.path.abspath(os.path.dirname(__file__))
    resdir = os.path.join(cwd, 'resources')
    tdir = os.path.join(cwd, 'target')
    os.makedirs(tdir, exist_ok=True)
    
    t = RdfOwlTransformer()
    fn = os.path.join(resdir, "mody.ttl.gz")
    f = gzip.open(fn, 'rb')
    t.parse(f, input_format='ttl')
    t.report()
    w1 = PandasTransformer(t.graph)
    w1.save(os.path.join(tdir, 'mondo-e.csv'), type='e')
    w1.save(os.path.join(tdir, 'mondo-n.csv'), type='n')
    w3 = JsonTransformer(t.graph)
    w3.save(os.path.join(tdir, "mondo.json"))
