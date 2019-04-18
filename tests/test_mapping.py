from kgx import PandasTransformer
from kgx import ObanRdfTransformer
import kgx.mapper as mapper
import networkx as nx
from random import random
import logging

def test_mapping():
    """
    create a random graph and save it in different formats
    """
    G = nx.MultiDiGraph()

    N = 100
    E = N * 3
    mapping = {}
    for i in range(0,N+1):
        nid = curie(i)
        mapping[nid] = mapped_curie(i)
        G.add_node(nid, label="node {}".format(i))
    for i in range(1,E):
        s = random_curie(N)
        o = random_curie(N)
        G.add_edge(o,s,predicate='related_to',relation='related_to')
    print('Nodes={}'.format(len(G.nodes())))
    mapper.map_graph(G, mapping)
    print("Mapped..")

    count = 0
    for nid in G.nodes():
        src = G.node[nid]['source_curie']
        assert nid.startswith("Y:")
        assert src.startswith("X:")
        count += 1
        if count > 5:
            break

    print("Saving tsv")
    w = PandasTransformer(G)
    w.save("target/maptest")
    w = ObanRdfTransformer(G)
    w.save("target/maptest.ttl")


def random_curie(N):
    return curie(int(random()*N))

def curie(n):
    return "X:{}".format(n)
def mapped_curie(n):
    return "Y:{}".format(n)
