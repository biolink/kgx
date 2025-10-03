import os
import pytest
import tempfile
from unittest.mock import patch

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from kgx.source import DuckDbSource
from kgx.transformer import Transformer


@pytest.fixture
def sample_duckdb():
    """Create a temporary DuckDB database with sample data."""
    if not DUCKDB_AVAILABLE:
        pytest.skip("DuckDB not available")
    
    # Create temporary database file - just get a unique name
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
        db_path = tmp.name
    
    # DuckDB will create the file when we connect
    conn = duckdb.connect(db_path)

    # Create nodes table
    conn.execute('''
        CREATE TABLE nodes (
            id VARCHAR PRIMARY KEY,
            category VARCHAR,
            name VARCHAR,
            description VARCHAR
        )
    ''')

    # Create edges table  
    conn.execute('''
        CREATE TABLE edges (
            id VARCHAR,
            subject VARCHAR,
            predicate VARCHAR,
            object VARCHAR,
            relation VARCHAR,
            publications VARCHAR
        )
    ''')

    # Insert sample data
    conn.execute('''
        INSERT INTO nodes VALUES 
        ('CURIE:123', 'biolink:Gene', 'Gene 123', 'Test gene'),
        ('CURIE:456', 'biolink:Disease', 'Disease 456', 'Test disease'),
        ('CURIE:789', 'biolink:ChemicalEntity', 'Chemical 789', 'Test chemical')
    ''')

    conn.execute('''
        INSERT INTO edges VALUES 
        ('edge1', 'CURIE:123', 'biolink:related_to', 'CURIE:456', 'biolink:related_to', 'PMID:1'),
        ('edge2', 'CURIE:789', 'biolink:treats', 'CURIE:456', 'biolink:treats', 'PMID:2')
    ''')

    conn.close()
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_duckdb_source_init():
    """Test DuckDbSource initialization."""
    t = Transformer()
    source = DuckDbSource(t)
    assert source.owner == t
    assert source.connection is None
    assert source.node_count == 0
    assert source.edge_count == 0


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_connect_db_success(sample_duckdb):
    """Test successful database connection."""
    t = Transformer()
    source = DuckDbSource(t)
    
    # Should not raise exception
    source._connect_db(sample_duckdb)
    assert source.connection is not None
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_connect_db_read_only(sample_duckdb):
    """Test that database connection is read-only."""
    t = Transformer()
    source = DuckDbSource(t)
    source._connect_db(sample_duckdb)
    
    # Attempt to insert should fail in read-only mode
    with pytest.raises(duckdb.InvalidInputException, match="Cannot execute statement of type \"CREATE\".*read-only"):
        source.connection.execute("CREATE TABLE test_write (id VARCHAR)")
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_connect_db_missing_tables():
    """Test database connection with missing tables."""
    # Create database without required tables - get unique name
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
        db_path = tmp.name
    
    conn = duckdb.connect(db_path)
    conn.execute('CREATE TABLE other_table (id VARCHAR)')
    conn.close()
    
    try:
        t = Transformer()
        source = DuckDbSource(t)
        
        with pytest.raises(ValueError, match="Required 'nodes' table not found"):
            source._connect_db(db_path)
    finally:
        os.unlink(db_path)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_get_nodes(sample_duckdb):
    """Test reading nodes from database."""
    t = Transformer()
    source = DuckDbSource(t)
    source._connect_db(sample_duckdb)
    
    nodes = source.get_nodes(limit=10, offset=0)
    
    assert len(nodes) == 3
    assert all('id' in node for node in nodes)
    assert any(node['id'] == 'CURIE:123' for node in nodes)
    assert any(node['id'] == 'CURIE:456' for node in nodes)
    assert any(node['id'] == 'CURIE:789' for node in nodes)
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_get_edges(sample_duckdb):
    """Test reading edges from database."""
    t = Transformer()
    source = DuckDbSource(t)
    source._connect_db(sample_duckdb)
    
    edges = source.get_edges(limit=10, offset=0)
    
    assert len(edges) == 2
    assert all('subject' in edge for edge in edges)
    assert all('predicate' in edge for edge in edges)
    assert all('object' in edge for edge in edges)
    
    # Check specific edge
    edge1 = next(edge for edge in edges if edge['id'] == 'edge1')
    assert edge1['subject'] == 'CURIE:123'
    assert edge1['predicate'] == 'biolink:related_to'
    assert edge1['object'] == 'CURIE:456'
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_get_nodes_with_filters(sample_duckdb):
    """Test reading nodes with filters."""
    t = Transformer()
    source = DuckDbSource(t)
    source.node_filters = {'category': 'biolink:Gene'}
    source._connect_db(sample_duckdb)
    
    nodes = source.get_nodes(limit=10, offset=0)
    
    assert len(nodes) == 1
    assert nodes[0]['id'] == 'CURIE:123'
    assert nodes[0]['category'] == 'biolink:Gene'
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_get_edges_with_filters(sample_duckdb):
    """Test reading edges with filters."""
    t = Transformer()
    source = DuckDbSource(t)
    source.edge_filters = {'predicate': 'biolink:treats'}
    source._connect_db(sample_duckdb)
    
    edges = source.get_edges(limit=10, offset=0)
    
    assert len(edges) == 1
    assert edges[0]['predicate'] == 'biolink:treats'
    assert edges[0]['subject'] == 'CURIE:789'
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_pagination(sample_duckdb):
    """Test pagination functionality."""
    t = Transformer()
    source = DuckDbSource(t)
    source._connect_db(sample_duckdb)
    
    # Get first 2 nodes
    page1 = source.get_nodes(limit=2, offset=0)
    assert len(page1) == 2
    
    # Get next node
    page2 = source.get_nodes(limit=2, offset=2)
    assert len(page2) == 1
    
    # Ensure no overlap
    page1_ids = {node['id'] for node in page1}
    page2_ids = {node['id'] for node in page2}
    assert not page1_ids.intersection(page2_ids)
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_parse_full_graph(sample_duckdb):
    """Test parsing the complete graph."""
    t = Transformer()
    source = DuckDbSource(t)
    
    records = list(source.parse(
        filename=sample_duckdb,
        page_size=2  # Small page size to test pagination
    ))
    
    # Should get both nodes and edges
    nodes = [r for r in records if len(r) == 2]  # nodes have format (node_id, node_data)
    edges = [r for r in records if len(r) == 4]  # edges have format (subject, object, edge_key, edge_data)
    
    assert len(nodes) == 3
    assert len(edges) == 2
    
    # Check node structure
    node_id, node_data = nodes[0]
    assert isinstance(node_id, str)
    assert 'id' in node_data
    assert 'category' in node_data
    
    # Check edge structure
    subject, object_node, edge_key, edge_data = edges[0]
    assert isinstance(subject, str)
    assert isinstance(object_node, str)
    assert isinstance(edge_key, str)
    assert 'id' in edge_data
    assert 'subject' in edge_data
    assert 'predicate' in edge_data
    assert 'object' in edge_data
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_parse_with_filters(sample_duckdb):
    """Test parsing with node and edge filters."""
    t = Transformer()
    source = DuckDbSource(t)
    
    records = list(source.parse(
        filename=sample_duckdb,
        node_filters={'category': 'biolink:Gene'},
        edge_filters={'predicate': 'biolink:treats'}
    ))
    
    nodes = [r for r in records if len(r) == 2]  # nodes have format (node_id, node_data)
    edges = [r for r in records if len(r) == 4]  # edges have format (subject, object, edge_key, edge_data)
    
    # Should only get filtered results
    assert len(nodes) == 1
    assert len(edges) == 1
    assert nodes[0][1]['category'] == 'biolink:Gene'
    assert edges[0][3]['predicate'] == 'biolink:treats'
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_parse_with_range(sample_duckdb):
    """Test parsing with start and end parameters."""
    t = Transformer()
    source = DuckDbSource(t)
    
    # Get only first 2 records (nodes come first)
    records = list(source.parse(
        filename=sample_duckdb,
        start=0,
        end=2,
        page_size=1
    ))
    
    assert len(records) == 2
    assert all(len(r) == 2 for r in records)  # Should be all nodes (length 2)
    
    source.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_process_node_edge_missing_required_fields():
    """Test processing nodes and edges with missing required fields."""
    t = Transformer()
    source = DuckDbSource(t)
    
    # Test node without ID
    node_without_id = {'category': 'biolink:Gene', 'name': 'test'}
    processed_nodes = list(source.load_nodes([node_without_id]))
    assert len(processed_nodes) == 0
    
    # Test edge without required fields
    edge_without_subject = {'predicate': 'biolink:treats', 'object': 'CURIE:456'}
    processed_edges = list(source.load_edges([edge_without_subject]))
    assert len(processed_edges) == 0


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_source_registration():
    """Test that DuckDbSource is properly registered in SOURCE_MAP."""
    from kgx.transformer import SOURCE_MAP
    
    assert 'duckdb' in SOURCE_MAP
    assert SOURCE_MAP['duckdb'] == DuckDbSource


def test_import_without_duckdb():
    """Test that import works even without duckdb installed."""
    # Import the module first, then patch duckdb to None
    from kgx.source.duckdb_source import DuckDbSource
    
    with patch('kgx.source.duckdb_source.duckdb', None):
        t = Transformer()
        source = DuckDbSource(t)
        
        # Should raise ImportError when trying to connect
        with pytest.raises(ImportError, match="duckdb package is required"):
            source._connect_db("dummy_path")


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_close_connection(sample_duckdb):
    """Test closing database connection."""
    t = Transformer()
    source = DuckDbSource(t)
    
    source._connect_db(sample_duckdb)
    assert source.connection is not None
    
    source.close()
    assert source.connection is None


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_error_handling_in_queries(sample_duckdb):
    """Test error handling in query methods."""
    t = Transformer()
    source = DuckDbSource(t)
    source._connect_db(sample_duckdb)
    
    # Break the connection to simulate query error
    source.connection.close()
    source.connection = None
    
    # Should return empty list on error
    nodes = source.get_nodes()
    assert nodes == []
    
    edges = source.get_edges()
    assert edges == []