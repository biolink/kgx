import gzip
import os

import jsonlines

from kgx.sink.sink import Sink


class JsonlSink(Sink):
    def __init__(self, filename, output_format, compression = None, **kwargs):
        super().__init__()

        self.dirname = os.path.abspath(os.path.dirname(filename))
        self.basename = os.path.basename(filename)
        self.extension = output_format.split(':')[0]

        self.nodes_filename = f"{self.basename}_nodes.{self.extension}"
        self.edges_filename = f"{self.basename}_edges.{self.extension}"
        if self.dirname:
            os.makedirs(self.dirname, exist_ok=True)

        if compression == 'gz':
            self.nodes_filename += f".{compression}"
            self.edges_filename += f".{compression}"
            NFH = gzip.open(self.nodes_filename, 'wb')
            self.NFH = jsonlines.Writer(NFH)
            EFH = gzip.open(self.edges_filename, 'wb')
            self.EFH = jsonlines.Writer(EFH)
        else:
            self.NFH = jsonlines.open(self.nodes_filename, 'w')
            self.EFH = jsonlines.open(self.edges_filename, 'w')

    def write_node(self, record):
        self.NFH.write(record)

    def write_edge(self, record):
        self.EFH.write(record)

    def finalize(self):
        pass
