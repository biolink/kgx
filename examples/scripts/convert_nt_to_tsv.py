import sys

import argparse

from kgx import NtTransformer, PandasTransformer

"""
A loader script that demonstrates how to convert an RDF N-Triple (*.nt) 
to TSV format.
"""

def usage():
    print("""
usage: convert_nt_to_tsv.py --input triples.nt --output output 
    """)

parser = argparse.ArgumentParser(description='Load edges and nodes into Neo4j')
parser.add_argument('--input', help='RDF N-Triple file')
parser.add_argument('--output', help='Output file name')
args = parser.parse_args()

if args.input is None or args.output is None:
    usage()
    exit()

# Initialize NtTransformer
t = NtTransformer()

# Load the nt
t.parse(args.input)

# Initialize PandasTransformer
pt = PandasTransformer(t.graph)
pt.save(args.output, output_format='tsv')
