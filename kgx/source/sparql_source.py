from kgx.source import RdfSource


class SparqlSource(RdfSource):

    # TODO: Implement a SparqlSource that is capable of reading from
    #  a local/remote SPARQL endpoint

    def __init__(self, owner):
        super().__init__(owner)
