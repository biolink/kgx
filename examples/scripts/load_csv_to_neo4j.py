import os
import argparse
from kgx.transformer import Transformer

"""
A loader script that demonstrates how to load edges and nodes into Neo4j.
"""


def usage():
    print("""
usage: load_csv_to_neo4j.py --nodes nodes.csv --edges edges.csv
    """)


parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
parser.add_argument('--nodes', help='file with nodes in CSV format')
parser.add_argument('--edges', help='file with edges in CSV format')
parser.add_argument('--uri', help='URI/URL for Neo4j (including port)', default='localhost:7474')
parser.add_argument('--username', help='username', default='neo4j')
parser.add_argument('--password', help='password', default='demo')
args = parser.parse_args()

if args.nodes is None and args.edges is None:
    usage()
    exit()

filename = []
if args.nodes:
    filename.append(args.nodes)
if args.edges:
    filename.append(args.edges)

input_args = {
    'filename': filename,
    'format': 'csv'
}
output_args = {
    'uri': args.uri,
    'username': args.username,
    'password': args.password,
    'format': 'neo4j'
}
# Initialize Transformer
t = Transformer()
t.transform(input_args, output_args)
