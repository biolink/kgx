from setuptools import setup, find_packages

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
    #
    # Official Neo4j driver seems to be from the 'neo4j' package?
    #
    # "neo4j-driver>=1.5.3",
    "neo4j",
    "pyyaml>=0.0",
    "jupyter>=0",
    "neo4jrestclient",
    "BiolinkMG>=0.0",
    "prologterms>=0.0.5",
    "bmt",
    "shexjsg>=0.6.5",
]

setup(
    name='Knowledge Graph Exchange',
    version='0.0.1',
    packages=find_packages(),
    install_requires=requires,
    scripts=['bin/translator_kgx.py'],
    entry_points="""
        [console_scripts]
        kgx=translator_kgx:cli
    """
)
