from kgx import JsonTransformer, clique_merge

t = JsonTransformer()
t.parse('results/hpo.owl')
t.parse('results/hp.owl')
t.parse('results/mondo.json')
t.parse('results/hgnc.json')
t.parse('results/clinvar.json')
t.parse('results/omim.json')
t.parse('results/hpoa.json')
t.parse('results/orphanet.json')

#t = PandasTransformer(t.graph)
#t.parse('data/semmeddb_edges.csv')
#t.parse('data/semmeddb_nodes.csv')

t.graph = clique_merge(t.graph)
t.save('results/clique_merged.json')
