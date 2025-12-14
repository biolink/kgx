"""
Jelly parser for KGX.

This class provides streaming parsing of Jelly format files,
similar to CustomNTriplesParser but for Jelly format.
"""

import gzip
from typing import Generator, Optional, IO
from contextlib import contextmanager

from rdflib import URIRef


class JellyParser:
    """
    Streaming parser for Jelly format files.

    Similar to CustomNTriplesParser interface but for Jelly format.
    """

    def __init__(self, sink):
        self.sink = sink

    def parse(
            self,
            filename: str,
            compression: Optional[str] = None
    ) -> Generator:
        file_obj = self._open_file(filename, compression)

        try:
            yield from self._parse_jelly_stream(file_obj)
        finally:
            file_obj.close()

    def _open_file(self, filename: str, compression: Optional[str]) -> IO[bytes]:
        if compression == "gz":
            return gzip.open(filename, "rb")
        else:
            return open(filename, "rb")

    def _parse_jelly_stream(self, file_obj: IO[bytes]) -> Generator:
        from pyjelly.integrations.rdflib.parse import parse_jelly_flat, Triple, Quad

        for item in parse_jelly_flat(file_obj):
            if isinstance(item, (Triple, Quad)):
                s, p, o = item[:3]

                yield from self.sink.triple(s, p, o)
