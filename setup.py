from setuptools import setup, find_packages
import re

NAME = 'kgx'
DESCRIPTION = 'A Python library and set of command line utilities for exchanging Knowledge Graphs (KGs) that conform to or are aligned to the Biolink Model.'
URL = 'https://github.com/NCATS-Tangerine/kgx'
AUTHOR = 'Deepak Unni, Sierra Moxon, Richard Bruskiewich'
EMAIL = 'deepak.unni3@gmail.com, smoxon@lbl.gov, richard.bruskiewich@delphinai.com'
REQUIRES_PYTHON = '>=3.7.0'
VERSION = '1.5.6'
LICENSE = 'BSD'

with open("requirements.txt", "r") as FH:
    REQUIREMENTS = FH.readlines()

EXTRAS = {}


INSTALL_REQUIRES = list()
for r in REQUIREMENTS:
    if not r.strip() or r.startswith("#"):
        continue
    # Leaving this here just for the moment in case developers
    # need to use git package overrides in the requirements.txt file
    # m = re.search(r"#egg=(.+)$", r)
    # if m:
    #    INSTALL_REQUIRES.append(m.group(1))
    # else:
    INSTALL_REQUIRES.append(r)

setup(
    name=NAME,
    author=AUTHOR,
    version=VERSION,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    description=DESCRIPTION,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    license=LICENSE,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_data={'kgx': ["config.yml"]},
    keywords='knowledge-graph Neo4j RDF NCATS NCATS-Translator Biolink-Model',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3'
    ],
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS,
    include_package_data=True,
    entry_points={
        'console_scripts': ['kgx=kgx.cli:cli']
    }
)
