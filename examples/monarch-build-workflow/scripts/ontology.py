import click
import bmt

from functools import lru_cache

from ontobio.ontol_factory import OntologyFactory
from kgx.cli.utils import get_type, get_transformer
from kgx.utils.rdf_utils import make_curie as _make_curie

ontology_factory = OntologyFactory()

@lru_cache()
def get_ontology(curie:str):
    prefix, _ = curie.lower().rsplit(':', 1)
    return ontology_factory.create(prefix)

@lru_cache()
def get_term(curie:str) -> str:
    ontology = get_ontology(curie)
    terms = [ontology.label(curie)] + ontology.ancestors(curie)
    # ont.label('GENO:0000845')
    # ont.ancestors('GENO:0000845')
    for term in terms:
        if bmt.get_element(term) is not None:
            return term
    return terms[-1]

@lru_cache()
def make_curie(s:str) -> str:
    return _make_curie(s)

@click.command()
@click.argument('input_path', type=click.Path(exists=True))
@click.option('-o', '--output_path', required=True, type=click.Path(exists=False))
@click.option('-b', '--biolink-model-only', is_flag=True, help='If true, will only use biolink model terms. Otherwise, will choose the best term available.')
def main(input_path, output_path, biolink_model_only):
    """
    Uses ontobio to load ontologies and choose the best biolink model term
    for a node category or edge label.
    """
    input_transformer = get_transformer(get_type(input_path))()
    output_transformer = get_transformer(get_type(output_path))()
    input_transformer.parse(input_path)
    G = input_transformer.graph

    for n, data in G.nodes(data=True):
        if 'category' in data and ':' in data['category']:
            c = make_curie(data['category'])
            data['category'] = [get_term(c)]

        elif 'category' not in data:
            data['category'] = ['named thing']

    for u, v, data in G.edges(data=True):
        if 'edge_label' in data and ':' in data['edge_label']:
            c = make_curie(data['edge_label'])
            data['edge_label'] = get_term(c)
        elif 'edge_label' not in data:
            data['edge_label'] = 'related_to'

    output_transformer.graph = G
    output_transformer.save(output_path)

if __name__ == '__main__':
    main()
