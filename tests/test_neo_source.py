import logging
from kgx import DebugSink, ProgressSink, NeoSource

logging.basicConfig(level=logging.INFO)

dsink = DebugSink(None)
psink = ProgressSink(dsink)

# TODO: uncomment when travis CI supports neo4j.
#neo_src = NeoSource(psink)

#neo_src.load()
