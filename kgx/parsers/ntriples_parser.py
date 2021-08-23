import codecs
from typing import Generator

from rdflib.plugins.parsers.ntriples import NTriplesParser, ParseError
from rdflib.plugins.parsers.ntriples import r_wspace, r_wspaces, r_tail


class CustomNTriplesParser(NTriplesParser):
    """
    This class is an extension to ``rdflib.plugins.parsers.ntriples.NTriplesParser``
    that parses N-Triples and yields triples.
    """

    def parse(self, filename: str) -> Generator:
        """
        Parses an N-Triples file and yields triples.

        Parameters
        ----------
        filename: str
            The filename to parse

        Returns
        -------
        Generator
            A generator for triples

        """
        if not hasattr(filename, "read"):
            raise ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        f = codecs.getreader("utf-8")(filename)

        self.file = f
        self.buffer = ""
        while True:
            self.line = self.readline()
            if self.line is None:
                break
            if self.line == "":
                break
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
        print(self.line)
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
