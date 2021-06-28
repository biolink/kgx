__version__ = '1.3.0'

from enum import Enum


class GraphEntityType(Enum):
    GRAPH = "graph"
    NODE = "node"
    EDGE = "edge"


# Biolink 2.0 "Knowledge Source" association slots,
# including the deprecated 'provided_by' slot
KS_SLOTS = [
    'knowledge_source',
    'primary_knowledge_source',
    'original_knowledge_source',
    'aggregator_knowledge_source',
    'supporting_data_source',
    'provided_by'
]