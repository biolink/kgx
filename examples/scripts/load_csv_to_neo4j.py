import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer

"""
A loader script that demonstrates how to load edges and nodes into Neo4j
"""

def usage():
    print("""
usage: load_csv_to_neo4j.py --nodes nodes.csv --edges edges.csv
    """)

parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
parser.add_argument('nodes', help='file with nodes in CSV format')
parser.add_argument('edges', help='file with edges in CSV format')
parser.add_argument('--host', help='host to connect with Neo4j', default='localhost')
parser.add_argument('--http_port', help='HTTP port to connect with Neo4j', default='7474')
parser.add_argument('--username', help='username (default: neo4j)', default='neo4j')
parser.add_argument('--password', help='password (default: demo)', default='demo')
args = parser.parse_args()

if args.nodes is None or args.edges is None:
    usage()
    exit()

print(args)
# Initialize PandasTransformer
t = PandasTransformer()

# Load nodes and edges into graph
t.parse(args.nodes, error_bad_lines=False)
t.parse(args.edges, error_bad_lines=False)

# Initialize NeoTransformer
# TODO: eliminate bolt
n = NeoTransformer(t.graph, args.host, {'http': args.http_port}, args.username, args.password)

# Save graph into Neo4j
n.save_with_unwind()

n.neo4j_report()
