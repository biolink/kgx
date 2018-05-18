from kgx import ObanRdfTransformer, PandasTransformer, GraphMLTransformer, JsonTransformer
from rdflib import Namespace
from rdflib.namespace import RDF
import rdflib


def test_load():
    """
    load and save tests
    """
    src_path = "tests/resources/monarch/biogrid_test.ttl"
    tg_path = "target/test_output.ttl"

    # execute ObanRdfTransformer's parse and save function
    t = ObanRdfTransformer()
    t.parse(src_path, input_format="turtle")
    t.save(tg_path, output_format="turtle")
    t.report()

    w1 = PandasTransformer(t.graph)
    w1.save('target/biogrid-e.csv', type='e')
    w1.save('target/biogrid-n.csv', type='n')
    
    # read again the source, test graph
    src_graph = rdflib.Graph()
    src_graph.parse(src_path, format="turtle")

    # read again the dumped target graph
    tg_graph = rdflib.Graph()
    tg_graph.parse(tg_path, format="turtle")

    # compare subgraphs from the source and the target graph.
    OBAN = Namespace('http://purl.org/oban/')
    for a in src_graph.subjects(RDF.type, OBAN.association):
        oban_src_graph = rdflib.Graph()
        oban_src_graph += src_graph.triples((a, None, None))
        oban_tg_graph = rdflib.Graph()
        oban_tg_graph += tg_graph.triples((a, None, None))
        # see they are indeed identical (isomorphic)
        if not oban_src_graph.isomorphic(oban_tg_graph):
            raise RuntimeError('The subgraphs whose subject is ' + str(a) + ' are not isomorphic ones.')

    w2 = GraphMLTransformer(t.graph)
    w2.save("target/x1n.graphml")
    w3 = JsonTransformer(t.graph)
    w3.save("target/x1n.json")
