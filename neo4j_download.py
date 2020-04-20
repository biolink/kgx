import logging, yaml
from kgx import CsvSink, TsvSink, ProgressSink, NeoSource

logging.basicConfig(level=logging.INFO)

with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    outputname = cfg['neo4j']['outputname']

with TsvSink(outputname) as sink:
    psink = ProgressSink(sink)
    neo_src = NeoSource(psink)
    neo_src.load()
