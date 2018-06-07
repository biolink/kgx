from enum import Enum

class FilterType(Enum):
    SUBJECT=1
    OBJECT=2
    EDGE=3
    NODE=4

    def lookup(name:str) -> 'FilterType':
        for t in FilterType:
            if t.name.lower() == name.lower():
                return t

    def get_all_args() -> list:
        args = []
        for t in FilterType:
            for f in [PropertyFilter, LabelFilter]:
                args.append('{type}_{suffix}'.format(
                    type=t.name.lower(),
                    suffix=f.get_suffix()
                ))
        return args

    def get_all_types() -> list:
        return [f.name.lower() for f in FilterType]

class Filter(object):
    def __init__(self, filter_type:FilterType):
        self.filter_type = filter_type

class PropertyFilter(Filter):
    def __init__(self, filter_type:FilterType, key:str, value:str):
        super().__init__(filter_type=filter_type)
        self.key = key
        self.value = value

    @staticmethod
    def get_suffix():
        return 'property'

class LabelFilter(Filter):
    def __init__(self, filter_type:FilterType, value:str):
        super().__init__(filter_type=filter_type)
        self.value = value

    @staticmethod
    def get_suffix():
        return 'label'
