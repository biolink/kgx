from typing import Any
from kgx.sink import Sink


class NullSink(Sink):
    """
     A NullSink just ignores any date written to it,
     effectively a /dev/null device for Transformer
     data flows, in which the inspection of the input
     knowledge graph is the important operation, but
     the graph itself is not persisted in the output
     (in particular, not in memory, where the huge
     memory footprint may be problematics, e.g. when
     stream processing huge graphs).

    Parameters
    ----------
    n/a (**kwargs allowed, but ignored)
    """

    def __init__(self, **kwargs: Any):
        super().__init__()

    def write_node(self, record) -> None:
        """
        Write a node record to the underlying store.

        Parameters
        ----------
        record: Any
            A node record

        """
        pass

    def write_edge(self, record) -> None:
        """
        Write an edge record to the underlying store.

        Parameters
        ----------
        record: Any
            An edge record

        """
        pass

    def finalize(self) -> None:
        """
        Operations that ought to be done after
        writing all the incoming data should be called
        by this method.

        """
        pass
