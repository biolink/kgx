"""
The classes in this file are for aiding the building of filtered cypher queries
in NeoTransformer.
"""

from enum import Enum

class FilterType(Enum):
    """
    This enum represents the four different places that one can apply a filter:
    the subject, object, and edge of a relationship query, and the node of a
    node query.
    """

    SUBJECT='subject'
    OBJECT='object'
    EDGE='edge'
    NODE='node'

    def lookup(name:str) -> 'FilterType':
        """
        Gets the enum object that matches the given name
        """
        for t in FilterType:
            if t.name.lower() == name.lower():
                return t

    def get_cypher_args() -> list:
        """
        Builds and returns each kind of key word argument passed into the cypher
        queries. For example: subject_label, subject_property, edge_label, etc.
        """
        args = []
        for t in FilterType:
            for f in [PropertyFilter, LabelFilter]:
                args.append('{type}_{suffix}'.format(
                    type=t.name.lower(),
                    suffix=f.get_suffix()
                ))
        return args

    def get_types() -> list:
        return [f.name.lower() for f in FilterType]

class Filter(object):
    def __init__(self, filter_type:FilterType):
        self.filter_type = filter_type

class PropertyFilter(Filter):
    """
    An object representing a property filter. Adding it to a tranformer will
    will result a cypher query that looks something like: (n {key: "value"}).
    The filter_type will determine where this filter is applied.
    """
    def __init__(self, filter_type:FilterType, key:str, value:str):
        super().__init__(filter_type=filter_type)
        self.key = key
        self.value = value

    @staticmethod
    def get_suffix():
        return 'property'

class LabelFilter(Filter):
    """
    An object representing a label filter. Adding it to a tranformer will
    will result a cypher query that looks something like: (n:value).
    The filter_type will determine where this filter is applied.
    """
    def __init__(self, filter_type:FilterType, value:str):
        super().__init__(filter_type=filter_type)
        self.value = value

    @staticmethod
    def get_suffix():
        return 'label'
