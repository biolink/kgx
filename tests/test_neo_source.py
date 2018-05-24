import logging
from kgx import DebugSink, ProgressSink, NeoSource

logging.basicConfig(level=logging.INFO)

dsink = DebugSink(None)
psink = ProgressSink(dsink)

neo_src = NeoSource(psink)

neo_src.load()
