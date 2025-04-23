# Installation

The installation for KGX requires Python 3.9 or greater.

## Installation for users

### Installing from PyPI

KGX is available on PyPI and can be installed using [pip](https://pip.pypa.io/en/stable/installing/) as follows:

```bash
pip install kgx
```

To install a particular version of KGX, specify the version number:

```bash
pip install kgx==2.4.2
```

### Installing from GitHub

Clone the GitHub repository and install using pip:

```bash
git clone https://github.com/biolink/kgx
cd kgx
pip install .
```

## Installation for developers

### Setting up a development environment with Poetry

KGX uses [Poetry](https://python-poetry.org/) for dependency management. First, install Poetry using [pipx](https://pypa.github.io/pipx/):

```bash
pip install pipx
pipx ensurepath
pipx install poetry
```

Next, clone the GitHub repository:

```bash
git clone https://github.com/biolink/kgx
cd kgx
```

Install all dependencies and create a virtual environment with Poetry:

```bash
poetry install
```

To activate the poetry virtual environment:

```bash
poetry shell
```

### Using a traditional virtual environment

Alternatively, you can use a traditional virtual environment:

```bash
git clone https://github.com/biolink/kgx
cd kgx
python3 -m venv env
source env/bin/activate
pip install .
```

### Setting up a testing environment

KGX has a suite of tests that rely on Docker containers to run Neo4j specific tests.

To set up the required containers, first install [Docker](https://docs.docker.com/get-docker/)
on your local machine.

Once Docker is up and running, run the following commands:

```bash
docker run -d --name kgx-neo4j-integration-test \
            -p 7474:7474 -p 7687:7687 \
            --env NEO4J_AUTH=neo4j/test \
            neo4j:3.5.25
```

```bash
docker run -d --name kgx-neo4j-unit-test \
            -p 8484:7474 -p 8888:7687 \
            --env NEO4J_AUTH=neo4j/test \
            neo4j:3.5.25
```

**Note:** Setting up the Neo4j container is optional. If there is no container set up then the tests that rely on them are skipped.

KGX tests are run using `make`:

```bash
# The Makefile already handles running commands through Poetry
make test
```
