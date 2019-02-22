import click, bmt
import logging
from kgx import JsonTransformer

@click.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('-o', '--output', required=True, type=click.Path(exists=False))
def main(path, output):
    G = JsonTransformer(path).graph

    for u, v, attr_dict in G.edges(data=True):
        edge_label = attr_dict['edge_label']

        try:
            predicate, category = edge_label.replace(' ', '_').rsplit('_', 1)
        except ValueError:
            continue

        is_predicate = bmt.get_predicate(predicate) is not None
        is_category = bmt.get_class(category) is not None

        if is_predicate and is_category:
            if 'category' not in G.node[v]:
                G.node[v]['category'] = [category]
            elif category not in G.node[v]['category']:
                G.node[v]['category'].append(category)
                logging.info('from {u} {p} {v} found {v} is a {c}'.format(u=u, p=p, v=v, c=category))

if __name__ == '__main__':
    main()
