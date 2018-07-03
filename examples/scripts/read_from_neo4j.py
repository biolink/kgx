import os, re, sys, logging, argparse
from kgx import NeoTransformer, PandasTransformer
from neo4j.util import watch

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
parser.add_argument('--bolt_port', help='Bolt port to connect with Neo4j', default='7687')
parser.add_argument('--username', help='username (default: neo4j)', default='neo4j')
parser.add_argument('--password', help='password (default: demo)', default='demo')
args = parser.parse_args()

#watch("neo4j.bolt", logging.INFO, sys.stdout)

# Initialize NeoTransformer
n = NeoTransformer(None, args.host, {'bolt': args.bolt_port}, args.username, args.password)

if args.filter is not None:
    if len(args.filter) > 0:
        for filter in args.filter:
            k,v = filter.split('=')
            # Set filters
            n.set_filter(k, v)

# Read from Neo4j with the given filter constraints (if any)
n.load()
n.report()
