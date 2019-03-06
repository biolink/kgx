from setuptools import setup

requires = [
    "prefixcommons>=0.1.4",
    "networkx>=2.2",
    "SPARQLWrapper>=1.8.0",
    "pandas>0.21",
    "pytest>=0.0",
    "mypy>=0.0",
    "pystache>=0.0",
    "rdflib>=0.0",
    "click>=7.0",
    "neo4j-driver>=1.5.3",
    "pyyaml>=0.0",
    "neo4jrestclient",
    "prologterms",
    "BiolinkMG",
    "bmt",
]

setup(
    name='Knowledge Graph Exchange',
    version='0.0.1',
    packages=['kgx', 'kgx.cli'],
    install_requires=requires,
    scripts=['bin/translator_kgx.py'],
    entry_points="""
        [console_scripts]
        kgx=translator_kgx:cli
    """
)
