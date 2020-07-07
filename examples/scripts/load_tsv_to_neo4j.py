import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer

"""
A loader script that demonstrates how to load edges and nodes into Neo4j
from TSV
"""

def usage():
    print("""
usage: load_tsv_to_neo4j.py --nodes nodes.tsv --edges edges.tsv
    """)

parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
parser.add_argument('--nodes', help='file with nodes in TSV format')
parser.add_argument('--edges', help='file with edges in TSV format')
parser.add_argument('--uri', help='URI/URL for Neo4j (including port)', default='localhost:7474')
parser.add_argument('--username', help='username', default='neo4j')
parser.add_argument('--password', help='password', default='demo')
args = parser.parse_args()

if args.nodes is None and args.edges is None:
    usage()
    exit()

# Initialize PandasTransformer
t = PandasTransformer()

# Load nodes and edges into graph
if args.nodes:
    t.parse(args.nodes, input_format='tsv')
if args.edges:
    t.parse(args.edges, input_format='tsv')

# Initialize NeoTransformer
n = NeoTransformer(t.graph, uri=args.uri, username=args.username, password=args.password)

# Save graph into Neo4j
n.save()
n.neo4j_report()
