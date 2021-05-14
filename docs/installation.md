# Installation

The installation for KGX requires Python 3.7 or greater.


## Installation for users


### Installing from PyPI

KGX is available on PyPI and can be installed using
[pip](https://pip.pypa.io/en/stable/installing/) as follows,

```bash
pip install kgx
```

To install a particular version of KGX, be sure to specify the version number,

```bash
pip install kgx==0.5.0
```


### Installing from GitHub

Clone the GitHub repository and then install,

```bash
git clone https://github.com/biolink/kgx
cd kgx
python setup.py install
```


## Installation for developers

### Setting up a development environment

To build directly from source, first clone the GitHub repository,

```bash
git clone https://github.com/biolink/kgx
cd kgx
```

Then install the necessary dependencies listed in ``requirements.txt``,

```bash
pip3 install -r requirements.txt
```


For convenience, make use of the `venv` module in Python3 to create a
lightweight virtual environment,

```
python3 -m venv env
source env/bin/activate

pip install -r requirements.txt
```

To install KGX you can do one of the following,

```bash
pip install .

# OR 

python setup.py install
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

KGX tests are simply run using `make`:

```bash
make tests
```
