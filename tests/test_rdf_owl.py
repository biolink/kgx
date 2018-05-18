from kgx import RdfOwlTransformer, PandasTransformer, JsonTransformer
import gzip

def test_load():
    """
    load tests
    """
    t = RdfOwlTransformer()
    fn = "tests/resources/mody.ttl.gz"
    f = gzip.open(fn, 'rb')
    t.parse(f, input_format='ttl')
    t.report()
    w1 = PandasTransformer(t.graph)
    w1.save('target/mondo-e.csv', type='e')
    w1.save('target/mondo-n.csv', type='n')
    w3 = JsonTransformer(t.graph)
    w3.save("target/mondo.json")
