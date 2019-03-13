"""
Loads all the turtle files with their required ontologies and transforms them to
json. Then loads all these json files, along with the semmeddb edges.csv and
nodes.csv files, into a single NetworkX graph, and performs `clique_merge` on it.
Finally, saves the resulting NetworkX graph as `clique_merged.csv`
"""

from kgx import ObanRdfTransformer, JsonTransformer, HgncRdfTransformer, RdfOwlTransformer
from kgx import clique_merge, make_valid_types

owl = ['hp', 'mondo', 'go', 'so', 'ro']
ttl = ['hgnc', 'orphanet', 'hpoa', 'omim', 'clinvar']

for filename in owl:
    t = RdfOwlTransformer()
    t.parse('data/{}.owl'.format(filename))
    t = PandasTransformer(t)
    t.save('results/{}.csv'.format(filename))

for filename in ttl:
    t = RdfOwlTransformer()
    t.parse('data/{}.ttl'.format(filename))
    t = PandasTransformer(t)
    t.save('results/{}.csv'.format(filename))

t = PandasTransformer()
t.parse('results/hp.csv.tar')
t.parse('results/mondo.csv.tar')
t.parse('results/hgnc.csv.tar')
t.parse('results/clinvar.csv.tar')
t.parse('results/omim.csv.tar')
t.parse('results/hpoa.csv.tar')
t.parse('results/orphanet.csv.tar')

# Uncomment to add in SemMedDb. Produce the file with scripts/transformed_semmeddb.py
# t.parse('results/transformed_semmeddb.csv.tar')

t.graph = clique_merge(t.graph)
make_valid_types(t.graph)

t.save('results/clique_merged.csv')
