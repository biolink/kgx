"""
Loads all the turtle files with their required ontologies and transforms them to
json. Then loads all these json files, along with the semmeddb edges.csv and
nodes.csv files, into a single NetworkX graph, and performs `clique_merge` on it.
Finally, saves the resulting NetworkX graph as `clique_merged.csv`
"""
import os

from kgx import ObanRdfTransformer, JsonTransformer, HgncRdfTransformer, RdfOwlTransformer
from kgx import clique_merge, make_valid_types

data = {
    'data/hp.owl' : RdfOwlTransformer,
    'data/mondo.owl' : RdfOwlTransformer,
    'data/go.owl' : RdfOwlTransformer,
    'data/so.owl' : RdfOwlTransformer,
    'data/ro.owl' : RdfOwlTransformer,

    'data/hgnc.ttl' : HgncRdfTransformer,

    'data/orphanet.ttl' : ObanRdfTransformer,
    'data/hpoa.ttl' : ObanRdfTransformer,
    'data/omim.ttl' : ObanRdfTransformer,
    'data/clinvar.ttl' : ObanRdfTransformer,
}

def change_extention(filename, extention):
    while extention.startswith('.'):
        extention = extention[1:]
    return '{}.{}'.format(filename.split('.', 1)[0], extention)

for filename, constructor in data.items():
    t = constructor()
    t.parse(filename)
    t.save(change_extention(filename, 'csv'))

t = PandasTransformer()

for filename in data.keys():
    filename = change_extention(filename, 'csv.tar')
    t.parse(filename)

t.merge_cliques()
t.categorize()
make_valid_types(t.graph)

t.save('results/clique_merged.csv')

quit()


# owl = ['hp', 'mondo', 'go', 'so', 'ro']
# ttl = ['hgnc', 'orphanet', 'hpoa', 'omim', 'clinvar']
# 
# for filename in owl:
#     i, o = 'data/{}.owl'.format(filename), 'results/{}.csv.tar'.format(filename)
#     if os.path.isfile(o):
#         continue
#     t = RdfOwlTransformer()
#     t.parse(i)
#     t = PandasTransformer(t)
#     t.save(o)
#
# for filename in ttl:
#     i, o = 'data/{}.ttl'.format(filename), 'results/{}.csv.tar'.format(filename)
#     if os.path.isfile(o):
#         continue
#     t = ObanRdfTransformer()
#     t.parse(i)
#     t = PandasTransformer(t)
#     t.save(o)
#
# t = PandasTransformer()
# for filename in ttl + owl:
#     t.parse('results/{}.csv.tar')
#
# t.merge_cliques()
# t.categorize()
# make_valid_types(t.graph)
#
# t.save('results/clique_merged.csv')
#
# quit()

# t = PandasTransformer()
# t.parse('results/hp.csv.tar')
# t.parse('results/mondo.csv.tar')
# t.parse('results/hgnc.csv.tar')
# t.parse('results/clinvar.csv.tar')
# t.parse('results/omim.csv.tar')
# t.parse('results/hpoa.csv.tar')
# t.parse('results/orphanet.csv.tar')
#
# # Uncomment to add in SemMedDb. Produce the file with scripts/transformed_semmeddb.py
# # t.parse('results/transformed_semmeddb.csv.tar')
#
# t.graph = clique_merge(t.graph)
# make_valid_types(t.graph)
#
# t.save('results/clique_merged.csv')
