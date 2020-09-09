from setuptools import setup, find_packages

NAME = 'Knowledge Graph Exchange'
DESCRIPTION = 'Knowledge Graph Exchange tools for BioLink model-compliant graphs.'
URL = 'https://github.com/NCATS-Tangerine/kgx'
AUTHOR = 'Deepak Unni'
EMAIL = 'deepak.unni3@gmail.com'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.0.1'
LICENSE = 'BSD'

with open("requirements.txt", "r") as FH:
    REQUIREMENTS = FH.readlines()

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
    package_data={'kgx': ["config.yml"]},
    keywords='knowledge-graph Neo4j RDF NCATS NCATS-Translator',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bioinformatics',
        'Topic :: Scientific/Engineering :: Knowledge Graphs',
        'Topic :: Scientific/Engineering :: Translational Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3'
    ],
    install_requires=[r for r in REQUIREMENTS if not r.startswith("#")],
    extras_require=EXTRAS,
    include_package_data=True,
    entry_points={
        'console_scripts': ['kgx=kgx.cli:cli']
    }
)
