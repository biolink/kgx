import os
import pprint

RESOURCE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "resources")
TARGET_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "target")


def print_graph(g):
    pprint.pprint([x for x in g.nodes(data=True)])
    pprint.pprint([x for x in g.edges(data=True)])
