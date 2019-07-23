import logging, yaml
from kgx import CsvSink, ProgressSink, SparqlSource

logging.basicConfig(level=logging.INFO)

with open ('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    outputname = cfg['sparql']['outputname']

with CsvSink(outputname) as csv_sink:
    psink = ProgressSink(csv_sink)
    src = SparqlSource(psink)
    src.load()
