import sys
import argparse

from kgx.transformer import Transformer

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

input_args = {
    'filename': [args.input],
    'format': 'nt'
}

output_args = {
    'filename': args.output,
    'format': 'tsv'
}

# Initialize NtTransformer
t = Transformer()

# Transform NT
t.transform(input_args, output_args)
