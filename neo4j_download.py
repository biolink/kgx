import logging, yaml
from kgx import CsvSink, ProgressSink, NeoSource

logging.basicConfig(level=logging.INFO)

with open ('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    outputname = cfg['neo4j']['outputname']

with CsvSink(outputname) as csv_sink:
    psink = ProgressSink(csv_sink)
    neo_src = NeoSource(psink)
    neo_src.load()
