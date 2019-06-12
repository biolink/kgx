import os

from kgx import ObanRdfTransformer, RdfOwlTransformer, PandasTransformer, JsonTransformer
from rdflib import Namespace
from rdflib.namespace import RDF
import rdflib

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

def test_load():
    """
    load TTL and save as CSV
    """
    input_file = os.path.join(resource_dir, 'monarch/biogrid_test.ttl')
    output_file = os.path.join(target_dir, 'test_output.ttl')

    t = ObanRdfTransformer()
    t.parse(input_file, input_format="turtle")
    t.report()
    t.save(output_file, output_format="turtle")

    output_archive_file = os.path.join(target_dir, 'biogrid_test')
    pt = PandasTransformer(t.graph)
    pt.save(output_archive_file)

    # read again the source, test graph
    src_graph = rdflib.Graph()
    src_graph.parse(input_file, format="turtle")

    # read again the dumped target graph
    target_graph = rdflib.Graph()
    target_graph.parse(output_file, format="turtle")

    # compare subgraphs from the source and the target graph.
    OBAN = Namespace('http://purl.org/oban/')
    for a in src_graph.subjects(RDF.type, OBAN.association):
        oban_src_graph = rdflib.Graph()
        oban_src_graph += src_graph.triples((a, None, None))
        oban_tg_graph = rdflib.Graph()
        oban_tg_graph += target_graph.triples((a, None, None))
        # see they are indeed identical (isomorphic)
        if not oban_src_graph.isomorphic(oban_tg_graph):
            print('The subgraphs whose subject is {} are not isomorphic'.format(a))

    # w2 = GraphMLTransformer(t.graph)
    # w2.save(os.path.join(tpath, "x1n.graphml"))
    w3 = JsonTransformer(t.graph)
    w3.save(os.path.join(target_dir, "biogrid_test.json"))

def test_owl_load():
    """
    Load a test OWL and export as JSON
    """
    input_file = os.path.join(resource_dir, 'mody.ttl')
    output_archive_file = os.path.join(target_dir, 'mondo_test')
    output_json_file = os.path.join(target_dir, 'mondo_test.json')

    t = RdfOwlTransformer()
    t.parse(input_file, input_format='ttl')
    t.report()

    pt = PandasTransformer(t.graph)
    pt.save(output_archive_file)

    jt = JsonTransformer(t.graph)
    jt.save(output_json_file)

def test_ontology_load():
    """
    Load an ontology OWL and export as JSON
    """
    # TODO
    pass
