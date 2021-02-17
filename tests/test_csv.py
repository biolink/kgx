import os

from kgx import PandasTransformer
from tests import TARGET_DIR, RESOURCE_DIR


def test_load():
    """
    Test for loading data into PandasTransformer
    """
    t = PandasTransformer()
    os.makedirs(TARGET_DIR, exist_ok=True)
    t.parse(os.path.join(RESOURCE_DIR, "x1_nodes.csv"), input_format='csv')
    t.parse(os.path.join(RESOURCE_DIR, "x1_edges.csv"), input_format='csv')
    t.report()
    t.save(os.path.join(TARGET_DIR, 'x1copy'))
    # w = GraphMLTransformer(t.graph)
    # w.save(os.path.join(TARGET_DIR, "x1n.graphml"))

def test_semmeddb_csv():
    """
    Read nodes and edges from CSV and export the resulting graph as an archive
    """
    t = PandasTransformer()
    nodes_file = os.path.join(RESOURCE_DIR, "semmed/semmeddb_test_nodes.csv")
    edges_file = os.path.join(RESOURCE_DIR, "semmed/semmeddb_test_edges.csv")
    output = os.path.join(TARGET_DIR, "semmeddb_test_export")

    t.parse(nodes_file, input_format='csv')
    t.parse(edges_file, input_format='csv')

    # save output as *.tar
    t.save(output, output_format='csv', compression='tar')

    # save output as *.tar.gz
    t.save(output, output_format='csv', compression='tar.gz')

    # save output as *tar.bz2
    t.save(output, output_format='csv', compression='tar.bz2')

def test_semmeddb_csv_to_tsv():
    """
    Read nodes and edges from CSV and export the resulting graph as an archive
    """
    t = PandasTransformer()
    nodes_file = os.path.join(RESOURCE_DIR, "semmed/semmeddb_test_nodes.csv")
    edges_file = os.path.join(RESOURCE_DIR, "semmed/semmeddb_test_edges.csv")
    output = os.path.join(TARGET_DIR, "semmeddb_test_tsv_export")

    t.parse(nodes_file, input_format='csv')
    t.parse(edges_file, input_format='csv')

    # save output as TSV in a tar archive
    t.save(output, output_format='tsv', compression='tar')

def test_read_achive():
    """
    Test reading of tar, tar.gz and tar.bz2 archives
    """

    tar_file = os.path.join(TARGET_DIR, "semmeddb_test_export.tar")
    tar_gz_file = os.path.join(TARGET_DIR, "semmeddb_test_export.tar.gz")
    tar_bz_file = os.path.join(TARGET_DIR, "semmeddb_test_export.tar.bz2")

    pt = PandasTransformer()
    pt.parse(tar_file, input_format='csv', compression='tar')
    assert not pt.is_empty()

    pt2 = PandasTransformer()
    pt2.parse(tar_gz_file, input_format='csv', compression='tar.gz')
    assert not pt2.is_empty()

    pt3 = PandasTransformer()
    pt3.parse(tar_bz_file, input_format='csv', compression='tar.bz2')
    assert not pt3.is_empty()
