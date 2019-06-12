import os

from kgx import PandasTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

def test_load():
    """
    Test for loading data into PandasTransformer
    """
    t = PandasTransformer()
    os.makedirs(target_dir, exist_ok=True)
    t.parse(os.path.join(resource_dir, "x1n.csv"))
    t.parse(os.path.join(resource_dir, "x1e.csv"))
    t.report()
    t.save(os.path.join(target_dir, 'x1copy.csv'))
    # w = GraphMLTransformer(t.graph)
    # w.save(os.path.join(target_dir, "x1n.graphml"))

def test_semmeddb_csv():
    """
    Read nodes and edges from CSV and export the resulting graph as an archive
    """
    t = PandasTransformer()
    nodes_file = os.path.join(resource_dir, "semmed/semmeddb_test_nodes.csv")
    edges_file = os.path.join(resource_dir, "semmed/semmeddb_test_edges.csv")
    output = os.path.join(target_dir, "semmeddb_test_export")

    t.parse(nodes_file)
    t.parse(edges_file)

    # save output as *.tar
    t.save(output)

    # save output as *.tar.gz
    t.save(output, mode='w:gz')

    # save output as *tar.bz2
    t.save(output, mode='w:bz2')

def test_semmeddb_csv_to_tsv():
    """
    Read nodes and edges from CSV and export the resulting graph as an archive
    """
    t = PandasTransformer()
    nodes_file = os.path.join(resource_dir, "semmed/semmeddb_test_nodes.csv")
    edges_file = os.path.join(resource_dir, "semmed/semmeddb_test_edges.csv")
    output = os.path.join(target_dir, "semmeddb_test_tsv_export")

    t.parse(nodes_file)
    t.parse(edges_file)

    # save output as TSV in a tar archive
    t.save(output, extension='tsv')

def test_read_achive():
    """
    Test reading of tar, tar.gz and tar.bz2 archives
    """

    tar_file = os.path.join(target_dir, "semmeddb_test_export.tar")
    tar_gz_file = os.path.join(target_dir, "semmeddb_test_export.tar.gz")
    tar_bz_file = os.path.join(target_dir, "semmeddb_test_export.tar.bz2")

    pt = PandasTransformer()
    pt.parse(tar_file)
    assert not pt.is_empty()

    pt2 = PandasTransformer()
    pt2.parse(tar_gz_file)
    assert not pt2.is_empty()

    pt3 = PandasTransformer()
    pt3.parse(tar_bz_file)
    assert not pt3.is_empty()
