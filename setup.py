from setuptools import setup

requires = [
    "prefixcommons>=0.1.4",
    "networkx==1.11",
    "SPARQLWrapper==1.8.0",
    "pandas<0.21",
    "pytest>=0.0",
    "mypy>=0.0",
    "pystache>=0.0",
    "pytest_logging>=0.0",
    "rdflib>=0.0",
    "click==6.7",
    "neo4j-driver>=1.5.3",
    "pyyaml>=0.0",
    "Click"]

setup(
    name='Knowledge Graph Exchange',
    version='0.0.1',
    packages=['kgx'],
    install_requires=requires,
    scripts=['bin/translator_kgx.py'],
    entry_points="""
        [console_scripts]
        kgx=translator_kgx:cli
    """
)
