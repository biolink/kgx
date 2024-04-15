# use importlib.metadata to read the version provided
# by the package during installation. Do not hardcode
# the version in the code
try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata
__version__ = importlib_metadata.version(__name__)