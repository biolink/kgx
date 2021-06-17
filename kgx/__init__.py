__version__ = '1.2.0'

from enum import Enum

class GraphEntityType(Enum):
    GRAPH = "graph"
    NODE = "node"
    EDGE = "edge"