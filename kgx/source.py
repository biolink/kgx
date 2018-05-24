from typing import Union, List, Dict

SimpleValue = Union[List[str], str]

class Source(object):
    """
    Base class for a data source.  This can be

     TODO: will these still be true?
     - from a source to an in-memory property graph (networkx)
     - from an in-memory property graph to a target format or database

    """

    def __init__(self, sink):
        """
        Create a new Source. This should be called directly on a subclass.
        """
        self.filter = {} # type: Dict[str, SimpleValue]
        self.sink = sink

    def node_count(self):
        return 'uncounted'

    def edge_count(self):
        return 'uncounted'

    def report(self) -> None:
        print('|Nodes|={}'.format(self.node_count()))
        print('|Edges|={}'.format(self.edge_count()))

    def set_filter(self, p: str, v: SimpleValue) -> None:
        """
        For pulling from a database endpoint, allow filtering
        for sets of interest

        Parameters:

         - predicate
         - subject_category
         - object_category
         - source
        """
        self.filter[p] = v
