import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer

"""
A script that demonstrates how to read edges and nodes from Neo4j
"""

def usage():
    print("""
usage: read_from_neo4j.py --filter subject_category=molecular_entity
                          --filter object_category=molecular_entity
                          --filter edge_label=part_of
    """)

parser = argparse.ArgumentParser(description='Read graph (or subgraph) from Neo4j')
parser.add_argument('--filter', action='append', help='A filter that can be applied to node and/or edges')
parser.add_argument('--host', help='host to connect with Neo4j', default='localhost')
parser.add_argument('--http_port', help='HTTP port to connect with Neo4j', default='7474')
parser.add_argument('--username', help='username (default: neo4j)', default='neo4j')
parser.add_argument('--password', help='password (default: demo)', default='demo')
args = parser.parse_args()

# Initialize NeoTransformer
n = NeoTransformer(None, args.host, { 'http': args.http_port}, args.username, args.password)

if args.filter is not None:
    if len(args.filter) > 0:
        for filter in args.filter:
            k,v = filter.split('=')
            # Set filters
            n.set_filter(k, v)

# Read from Neo4j with the given filter constraints (if any)
# TODO the test data from \nneo-1 is not that great for testing pagination; the dataset is very small!
n.load()
n.report()
