"""
The classes in this file are for aiding the building of filtered cypher queries
in NeoTransformer.
"""

from enum import Enum
from typing import Union, Tuple

class FilterLocation(Enum):
    SUBJECT='subject'
    OBJECT='object'
    EDGE='edge'
    NODE='node'

    @staticmethod
    def values():
        return [x.value for x in FilterLocation]

class FilterType(Enum):
    LABEL='label'
    CATEGORY='category'
    PROPERTY='property'

    @staticmethod
    def values():
        return [x.value for x in FilterType]


class Filter(object):
    """
    Represents a filter for a cypher query in :py:class:kgx.NeoTransformer
    """
    def __init__(self, target:str, value:Union[str, Tuple[str, str]]):
        filter_local, filter_type = target.split('_')
        self.target = target
        self.filter_local = FilterLocation(filter_local)
        self.filter_type = FilterType(filter_type)
        self.value = value

        if self.filter_type is FilterType.PROPERTY:
            assert isinstance(value, tuple) and len(value) == 2, 'Property filter values must be a tuple of length 2'

    def __str__(self):
        """
        A human readable string representation of a Filter object
        types:
            subject_category
            object_category
            node_category
            edge_label

            subject_property
            object_property
            node_property
            edge_property
        """
        return 'Filter[target={}, value={}]'.format(self.target, self.value)

    @staticmethod
    def build(filter_local:FilterLocation, filter_type:FilterType, value):
        """
        A factory method for building a Filter using the given enums

        Only edges should have the target "edge_label", and edges should not be
        combined with the "category" location.
        """
        assert not (filter_type is FilterType.LABEL and filter_local is not FilterLocation.EDGE)
        assert not (filter_local is FilterLocation.EDGE and filter_type is FilterType.CATEGORY)

        return Filter('{}_{}'.format(filter_local.value, filter_type.value), value)

    @staticmethod
    def targets():
        targets = []
        for filter_type in FilterType:
            for filter_local in FilterLocation:
                try:
                    targets.append(
                        Filter.build(filter_type=filter_type, filter_local=filter_local, value=(None, None)).target
                    )
                except:
                    continue
        return targets

if __name__ == '__main__':
    print(Filter('subject_label', 'gene'))
    print(Filter.build(FilterLocation.EDGE, FilterType.PROPERTY, ('property_name', 'property_value')))
    print(Filter.targets())
