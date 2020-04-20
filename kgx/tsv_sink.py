import json
from .file_sink import FileSink

class TsvSink(FileSink):
    def __init__(self, file_name_prefix, extension='tsv'):
        super(TsvSink, self).__init__(file_name_prefix, extension)

    def _writer(self, out, fields):
        return TsvWriter(out, fields)


class TsvWriter:
    def __init__(self, out, fields):
        self.out = out
        self.fields = fields

    def _writeline(self, cols):
        self.out.write('\t'.join(cols))
        self.out.write('\n')

    def writeheader(self):
        self._writeline(self.fields)

    def writerow(self, row):
        self._writeline(tuple(row[f] for f in self.fields))
