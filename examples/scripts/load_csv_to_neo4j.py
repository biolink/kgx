import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer
from neo4j.util import watch

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
parser.add_argument('--uri', help='BOLT URI to connect with Neo4j (default: bolt://localhost:7687', default = 'bolt://localhost:7687')
parser.add_argument('--username', help='username (default: neo4j)', default = 'neo4j')
parser.add_argument('--password', help='password (default: demo)', default = 'demo')
args = parser.parse_args()

if args.nodes is None or args.edges is None:
    usage()
    exit()

#watch("neo4j.bolt", logging.INFO, sys.stdout)

print(args)
# Initialize PandasTransformer
t = PandasTransformer()

# Load nodes and edges into graph
t.parse(args.nodes, error_bad_lines=False)
t.parse(args.edges, error_bad_lines=False)

# Initialize NeoTransformer
n = NeoTransformer(t.graph, args.uri, args.username, args.password)

# Save graph into Neo4j
n.save_with_unwind()

n.neo4j_report()
