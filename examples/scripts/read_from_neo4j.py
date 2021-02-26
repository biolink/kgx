import os
import argparse
from kgx.transformer import Transformer

"""
A script that demonstrates how to read edges and nodes from Neo4j.
"""


def usage():
    print("""
usage: read_from_neo4j.py --edge_filter subject_category=biolink:Gene
                          --edge_filter filter object_category=biolink:Disease
                          --edge_filter filter edge_label=biolink:involved_in
    """)


parser = argparse.ArgumentParser(description='Read graph (or subgraph) from Neo4j')
parser.add_argument('--uri', help='URI/URL for Neo4j (including port)', default='localhost:7474')
parser.add_argument('--username', help='username', default='neo4j')
parser.add_argument('--password', help='password', default='demo')
args = parser.parse_args()


input_args = {
    'uri': args.uri,
    'username': args.username,
    'password': args.password,
    'format': 'neo4j'
}

# Initialize Transformer
t = Transformer()
t.transform(input_args)
print(f"Number of nodes from Neo4j: {t.store.graph.number_of_nodes()}")
print(f"Number of edges from Neo4j: {t.store.graph.number_of_edges()}")