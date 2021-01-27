import gzip
import re

import jsonlines

from kgx.source.json_source import JsonSource


class JsonlSource(JsonSource):
    def __init__(self):
        super().__init__()
        self._node_properties = set()
        self._edge_properties = set()

    def parse(self, filename, input_format, compression = None, provided_by = None, **kwargs):
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if re.search(f'nodes.{input_format}', filename):
            m = self.load_node # type: ignore
        elif re.search(f'edges.{input_format}', filename):
            m = self.load_edge # type: ignore
        else:
            raise TypeError(f"Unrecognized file: {filename}")

        if compression == 'gz':
            with gzip.open(filename, 'rb') as FH:
                reader = jsonlines.Reader(FH)
                for obj in reader:
                    yield m(obj)
        else:
            with jsonlines.open(filename) as FH:
                for obj in FH:
                    yield m(obj)

