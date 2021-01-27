
class Sink(object):
    """
    A Sink is responsible for writing data as records
    to a store where the store is a file or a database.
    """
    def __init__(self):
        pass

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
