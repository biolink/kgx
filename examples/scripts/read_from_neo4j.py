import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer

"""
A script that demonstrates how to read edges and nodes from Neo4j
"""

def usage():
    print("""
usage: read_from_neo4j.py --edge_filter subject_category=biolink:Gene
                          --edge_filter filter object_category=biolink:Disease
                          --edge_filter filter edge_label=biolink:involved_in
    """)

parser = argparse.ArgumentParser(description='Read graph (or subgraph) from Neo4j')
parser.add_argument('--node_filter', action='append', help='A filter that can be applied to nodes')
parser.add_argument('--edge_filter', action='append', help='A filter that can be applied to edges')
parser.add_argument('--uri', help='URI/URL for Neo4j (including port)', default='localhost:7474')
parser.add_argument('--username', help='username', default='neo4j')
parser.add_argument('--password', help='password', default='demo')
args = parser.parse_args()

# Initialize NeoTransformer
nt = NeoTransformer(None, uri=args.uri, username=args.username, password=args.password)

if args.node_filter:
    for f in args.node_filter:
        k, v = f.split('=')
        nt.set_node_filter(k, set(v))

if args.edge_filter:
    for f in args.edge_filter:
        k, v = f.split('=')
        nt.set_edge_filter(k, set(v))

# Read from Neo4j with the given filter constraints (if any)
nt.load()
nt.report()
