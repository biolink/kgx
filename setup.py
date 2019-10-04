from setuptools import setup, find_packages


NAME = 'Knowledge Graph Exchange'
DESCRIPTION = 'Knowledge Graph Exchange tools for BioLink model-compliant graphs.'
URL = 'https://github.com/NCATS-Tangerine/kgx'
AUTHOR = 'Deepak Unni'
EMAIL = 'deepak.unni3@gmail.com'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.0.1'
LICENSE = 'BSD'

REQUIRED = [
    "prefixcommons>=0.1.4",
    "pip>=9.0.1",
    "networkx>=2.2",
    "SPARQLWrapper>=1.8.2",
    "pandas>=0.24.2",
    "pytest>=0.0",
    "mypy>=0.0",
    "pystache>=0.0",
    "rdflib>=0.0",
    "Click>=7.0",
    "neo4j>=1.7.4",
    "neo4jrestclient>=0.0",
    "pyyaml>=0.0",
    #"BiolinkMG>=0.0",
    "biolinkml>=0.0",
    "bmt>=0.1.0",
    "prologterms>=0.0.5",
    "shexjsg>=0.6.5",
    "terminaltables>=3.1.0",
    "stringcase>=1.2.0",
    "validators>=0.13.0"
]

EXTRAS = {}


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    description=DESCRIPTION,
    long_description=open('README.md').read(),
    license=LICENSE,
    packages=find_packages(),
    keywords='knowledge-graph Neo4j RDF NCATS NCATS-Translator',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bioinformatics',
        'Topic :: Scientific/Engineering :: Knowledge Graphs',
        'Topic :: Scientific/Engineering :: Translational Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3'
    ],
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    entry_points="""
        [console_scripts]
        kgx=translator_kgx:cli
    """,
    scripts=['bin/translator_kgx.py']
)
