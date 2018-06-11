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
        """
        return 'Filter[target={}, value={}]'.format(self.target, self.value)

    @staticmethod
    def build(filter_local:FilterLocation, filter_type:FilterType, value):
        """
        A factory method for building a Filter using the given enums
        """
        return Filter('{}_{}'.format(filter_local.value, filter_type.value), value)

    @staticmethod
    def targets():
        return [Filter.build(a, b, (None, None)).target for a in FilterLocation for b in FilterType]

if __name__ == '__main__':
    print(Filter('subject_label', 'gene'))
    print(Filter.build(FilterLocation.EDGE, FilterType.PROPERTY, ('property_name', 'property_value')))
    print(Filter.targets())
