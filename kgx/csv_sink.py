import json
from csv import DictWriter
from .file_sink import FileSink

class CsvSink(FileSink):
    def __init__(self, file_name_prefix, extension='csv'):
        super(CsvSink, self).__init__(file_name_prefix, extension)

    def _writer(self, out, fields):
        return DictWriter(out, fields)
