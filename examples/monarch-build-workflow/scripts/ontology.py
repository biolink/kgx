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
def get_term(curie:str, biolink_model_only=False) -> str:
    ontology = get_ontology(curie)
    terms = [ontology.label(c) for c in ontology.ancestors(curie, reflexive=True)]
    for term in terms:
        if term is not None and bmt.get_element(term) is not None:
            return term
    terms.sort(key=lambda s: len(s) if isinstance(s, str) else float('inf'))
    if biolink_model_only:
        return curie
    else:
        return terms[0]

@lru_cache()
def make_curie(s:str) -> str:
    return _make_curie(s)

ontologies = {}

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
        if 'category' in data and isinstance(data['category'], (tuple, list, set)):
            for category in data['category']:
                if ':' in category:
                    curie = make_curie(category)
                    prefix, _ = curie.lower().rsplit(':', 1)
                    ontologies[prefix] = None
    for u, v, data in G.edges(data=True):
        if 'edge_label' in data and ':' in data['edge_label']:
            curie = make_curie(data['edge_label'])
            prefix, _ = curie.lower().rsplit(':', 1)
            ontologies[prefix] = None

    print(ontologies)

    for key in ontologies.keys():
        print(key)
        ontologies[key] = get_ontology(key)

    import pudb; pu.db

    with click.progressbar(G.nodes(data=True)) as bar:
        for n, data in bar:
            if 'category' in data and isinstance(data['category'], (list, set, tuple)):
                l = [get_term(make_curie(c), biolink_model_only) for c in data['category'] if ':' in c]
                l += [c for c in data['category'] if ':' not in c]
                data['category'] = l
            elif 'category' not in data:
                data['category'] = ['named thing']

    with click.progressbar(G.edges(data=True)) as bar:
        for u, v, data in bar:
            if 'edge_label' in data and ':' in data['edge_label']:
                c = make_curie(data['edge_label'])
                data['edge_label'] = get_term(c, biolink_model_only)
            elif 'edge_label' not in data:
                data['edge_label'] = 'related_to'

    output_transformer.graph = G
    output_transformer.save(output_path)

if __name__ == '__main__':
    main()
