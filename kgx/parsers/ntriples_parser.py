import codecs
from typing import Generator

from rdflib.plugins.parsers.ntriples import W3CNTriplesParser, ParseError
from rdflib.plugins.parsers.ntriples import r_wspace, r_wspaces, r_tail


class CustomNTriplesParser(W3CNTriplesParser):
    """
    This class is an extension to ``rdflib.plugins.parsers.ntriples.W3CNTriplesParser``
    that parses N-Triples and yields triples.
    """

    def __init__(self, sink=None):
        W3CNTriplesParser.__init__(sink)
        self.file = None
        self.buffer = ""
        self.line = ""
    
    def parse(self, f) -> Generator:
        """
        Parses an N-Triples file and yields triples.

        Parameters
        ----------
        f:
            The file-like object to parse

        Returns
        -------
        Generator
            A generator for triples

        """
        if not hasattr(f, "read"):
            raise ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        f = codecs.getreader("utf-8")(f)

        self.file = f
        self.buffer = ""
        while True:
            self.line = self.readline()
            if self.line is None:
                break
            if self.line == "":
                raise ParseError(f"Empty line encountered in {str(f)}. "
                                 f"Ensure that no leading or trailing empty lines persist "
                                 f"in the N-Triples file.")
            try:
                yield from self.parseline()
            except ParseError:
                raise ParseError("Invalid line: %r" % self.line)

    def parseline(self) -> Generator:
        """
        Parse each line and yield triples.

        Parameters
        ----------
        Generator
            A generator

        """
        if not hasattr(self, 'sink'):
            raise ParseError("CustomNTriplesParser is missing a sink?")
        
        self.eat(r_wspace)
        
        if self.line or not self.line.startswith("#"):
            
            subject = self.subject()
            
            self.eat(r_wspaces)

            predicate = self.predicate()
            self.eat(r_wspaces)

            object = self.object()
            self.eat(r_tail)

            if self.line:
                raise ParseError("Trailing garbage")
            
            return self.sink.triple(subject, predicate, object)
