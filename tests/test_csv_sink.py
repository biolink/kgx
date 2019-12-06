import logging
from kgx import CsvSink, ProgressSink, NeoSource

logging.basicConfig(level=logging.INFO)

# TODO: uncomment when travis CI supports neo4j.
#with CsvSink('neo_src-output') as csv_sink:
    #psink = ProgressSink(csv_sink)

    #neo_src = NeoSource(psink)

    #neo_src.load()
