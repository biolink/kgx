import networkx as nx

def map_graph(G, mapping, preserve=True):
    if preserve:
        for nid in G.nodes_iter():
            if nid in mapping:
                # add_node will append attributes
                G.add_node(nid, source_curie=nid)
        for oid,sid in G.edges_iter():
            if oid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_object=oid)
            if sid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_subject=oid)
    nx.relabel_nodes(G, mapping, copy=False)


