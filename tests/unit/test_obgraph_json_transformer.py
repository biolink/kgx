import os

import pytest

from kgx import ObographJsonTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_json_load():
    t = ObographJsonTransformer()
    t.parse(os.path.join(resource_dir, 'goslim_generic.json'))
    assert t.graph.number_of_nodes() == 176
    assert t.graph.number_of_edges() == 206

    n1 = t.graph.nodes(data=True)['GO:0003677']
    assert n1['id'] == 'GO:0003677'
    assert n1['name'] == 'DNA binding'
    assert n1['description'] == 'Any molecular function by which a gene product interacts selectively and non-covalently with DNA (deoxyribonucleic acid).'
    assert n1['category'] == ['biolink:MolecularActivity']
    assert 'structure-specific DNA binding' in n1['synonym']
    assert 'structure specific DNA binding' in n1['synonym']
    assert 'microtubule/chromatin interaction' in n1['synonym']
    assert 'plasmid binding' in n1['synonym']

    n2 = t.graph.nodes(data=True)['GO:0005575']
    assert n2['id'] == 'GO:0005575'
    assert n2['name'] == 'cellular_component'
    assert n2['description'] == 'A location, relative to cellular compartments and structures, occupied by a macromolecular machine when it carries out a molecular function. There are two ways in which the gene ontology describes locations of gene products: (1) relative to cellular structures (e.g., cytoplasmic side of plasma membrane) or compartments (e.g., mitochondrion), and (2) the stable macromolecular complexes of which they are parts (e.g., the ribosome).'
    assert n2['category'] == ['biolink:CellularComponent']
    assert n2['xref'] == ['NIF_Subcellular:sao1337158144']
    assert 'goslim_chembl' in n2['subsets']
    assert 'goslim_generic' in n2['subsets']


def test_load_node():
    node = {
        "id": "http://purl.obolibrary.org/obo/GO_0005615",
        "meta": {
            "basicPropertyValues": [{
                "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                "val": "cellular_component"
            }]
        },
        "type": "CLASS",
        "lbl": "extracellular space"
    }
    t = ObographJsonTransformer()
    t.load_node(node)

    n = next(iter(t.graph.nodes(data=True)))
    assert n[1]['id'] == 'GO:0005615'
    assert n[1]['name'] == 'extracellular space'
    assert n[1]['category'] == ['biolink:CellularComponent']


def test_load_edge():
    edge = {
        "sub": "http://purl.obolibrary.org/obo/GO_0003677",
        "pred": "is_a",
        "obj": "http://purl.obolibrary.org/obo/GO_0003674"
    }
    t = ObographJsonTransformer()
    t.load_edge(edge)

    e = next(iter(t.graph.edges(data=True)))
    assert e[2]['subject'] == 'GO:0003677'
    assert e[2]['edge_label'] == 'biolink:subclass_of'
    assert e[2]['relation'] == 'rdfs:subClassOf'
    assert e[2]['object'] == 'GO:0003674'


@pytest.mark.parametrize('query', [
    ({
        "id": "http://purl.obolibrary.org/obo/GO_0005615",
        "meta": {
            "basicPropertyValues": [{
                "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                "val": "cellular_component"
            }]
        },
        "type": "CLASS",
        "lbl": "extracellular space"
    }, 'biolink:CellularComponent'),
    ({
        "id": "http://purl.obolibrary.org/obo/GO_0008168",
        "meta": {
            "definition": {
              "val": "Catalysis of the transfer of a methyl group to an acceptor molecule."
            },
            "basicPropertyValues": [{
              "pred": "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId",
              "val": "GO:0004480"
            }, {
              "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
              "val": "molecular_function"
            }]
        }
     }, 'biolink:MolecularActivity'),
    ({
        "id": "http://purl.obolibrary.org/obo/GO_0065003",
        "meta": {
            "definition": {
                "val": "The aggregation, arrangement and bonding together of a set of macromolecules to form a protein-containing complex."
            },
            "basicPropertyValues": [{
                "pred": "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId",
                "val": "GO:0006461"
            }, {
                "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                "val": "biological_process"
            }]
        },
    }, 'biolink:BiologicalProcess')
])
def test_get_category(query):
    node = query[0]
    t = ObographJsonTransformer()
    c = t.get_category(node['id'], node)
    assert c == query[1]



