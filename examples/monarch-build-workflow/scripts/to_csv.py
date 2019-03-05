"""
This script prepares the clique_merged.json file for uploading to Neo4j
- Removes nodes that cannot be categorized into the biolink model
- Renames edge labels that don't matche the biolink model to "related_to"
- Transforms into CSV format
"""

from kgx import JsonTransformer, PandasTransformer
import bmt

t = JsonTransformer()
t.parse('results/clique_merged.json')
t = PandasTransformer(t)

G = t.graph

size = len(G)

nodes = []

for n, data in G.nodes(data=True):
    data['category'] = [c for c in data.get('category', []) if bmt.get_class(c) is not None]
    if data['category'] == []:
        if 'name' in data:
            data['category'] = ['named thing']
        else:
            nodes.append(n)

G.remove_nodes_from(nodes)

for u, v, data in G.edges(data=True):
    if bmt.get_predicate(data['edge_label']) is None:
        data['edge_label'] = 'related_to'

print('Removed {} many nodes'.format(size - len(G)))

t.graph = G

t.save('results/clique_merged.csv')
