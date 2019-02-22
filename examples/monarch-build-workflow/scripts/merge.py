from kgx import JsonTransformer, clique_merge

t = JsonTransformer()
t.parse('results/hgnc.json')
t.parse('results/clinvar.json')
t.parse('results/omim.json')
t.parse('results/hpoa.json')
t.parse('results/orphanet.json')

t.graph = clique_merge(t.graph)
t.save('results/clique_merged.csv')
