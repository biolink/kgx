from kgx import PandasTransformer
from kgx import ObanRdfTransformer
import networkx as nx
from random import random
import logging

def test_load():
    """
    create a random graph and save it in different formats
    """
    G = nx.MultiDiGraph()

    # Adjust this to test for scalability
    N = 1000
    E = N * 3
    for i in range(1,N):
        G.add_node(curie(i), label="node {}".format(i))
    for i in range(1,E):
        s = random_curie(N)
        o = random_curie(N)
        G.add_edge(o,s, predicate='related_to', relation='related_to')
    print('Nodes={}'.format(len(G.nodes())))
    rename_all(G)

    w = PandasTransformer(G)
    w.save("target/random")

    w = ObanRdfTransformer(G)
    w.save("target/random.ttl")

def rename_all(G):
    m = {}
    for nid in G.nodes():
        tid = nid.replace("X:","FOO:")
        m[nid] = tid
    print("Renaming...")
    nx.relabel_nodes(G,m, copy=False)
    print("Renamed!")


def random_curie(N):
    return curie(int(random()*N))

def curie(n):
    return "X:{}".format(n)
