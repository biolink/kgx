class Sink(object):
    """
    Base class for a data sink.
    """
    def add_node(self, node_id, attributes):
        raise NotImplementedError('add_node')

    def add_edge(self, subject_id, object_id, attributes):
        raise NotImplementedError('add_edge')

    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        pass
