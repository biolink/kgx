from kgx.sink.tsv_sink import TsvSink


class TrapiSink(TsvSink):
    """
    Translator reasoner API Sink

    Parameters:
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs

    """
    # TO be implemented for TRAPI 1.0 spec
    def __init__(self, owner):

        super().__init__(owner)
