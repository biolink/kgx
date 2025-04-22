import json
import os
import gzip

from kgx.graph.nx_graph import NxGraph
from kgx.sink.trapi_sink import TrapiSink
from tests import TARGET_DIR


def test_write_trapi_json():
    """
    Write a graph as TRAPI JSON using TrapiSink.
    """
    # Create a test graph
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_node("C", id="C", **{"name": "Node C", "category": ["biolink:NamedThing"]})
    graph.add_node("D", id="D", **{"name": "Node D", "category": ["biolink:NamedThing"]})
    graph.add_node("E", id="E", **{"name": "Node E", "category": ["biolink:NamedThing"]})
    graph.add_node("F", id="F", **{"name": "Node F", "category": ["biolink:NamedThing"]})
    
    graph.add_edge("B", "A", **{"subject": "B", "object": "A", "predicate": "biolink:sub_class_of"})
    graph.add_edge("C", "B", **{"subject": "C", "object": "B", "predicate": "biolink:sub_class_of"})
    graph.add_edge("D", "C", **{"subject": "D", "object": "C", "predicate": "biolink:sub_class_of"})
    graph.add_edge("D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"})
    graph.add_edge("E", "D", **{"subject": "E", "object": "D", "predicate": "biolink:sub_class_of"})
    graph.add_edge("F", "D", **{"subject": "F", "object": "D", "predicate": "biolink:sub_class_of"})
    
    filename = os.path.join(TARGET_DIR, "test_trapi_graph.json")
    s = TrapiSink(filename=filename, biolink_version="2.4.8", owner=graph)
    
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, data in graph.edges(data=True):
        s.write_edge(data)
    s.finalize()
    
    assert os.path.exists(filename)
    
    # Verify the output
    with open(filename, 'r') as f:
        content = json.load(f)
    
    # Check overall structure
    assert 'knowledge_graph' in content
    assert 'nodes' in content['knowledge_graph']
    assert 'edges' in content['knowledge_graph']
    
    # Check node count and structure
    assert len(content['knowledge_graph']['nodes']) == 6  # A through F
    assert 'A' in content['knowledge_graph']['nodes']
    assert 'categories' in content['knowledge_graph']['nodes']['A']
    assert 'attributes' in content['knowledge_graph']['nodes']['A']
    assert content['knowledge_graph']['nodes']['A']['name'] == 'Node A'
    
    # Check edge count
    assert len(content['knowledge_graph']['edges']) == 6
    
    # Find an edge by looking for known predicate and check its structure
    found_edge = False
    for edge_id, edge in content['knowledge_graph']['edges'].items():
        if edge['predicate'] == 'biolink:related_to':
            found_edge = True
            assert edge['subject'] == 'D'
            assert edge['object'] == 'A'
            assert 'attributes' in edge
            assert 'sources' in edge
    assert found_edge


def test_write_trapi_jsonl():
    """
    Write a graph as TRAPI JSONLines using TrapiSink.
    """
    # Create a test graph
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_edge("A", "B", **{"subject": "A", "object": "B", "predicate": "biolink:related_to"})
    
    filename = os.path.join(TARGET_DIR, "test_trapi_graph.jsonl")
    s = TrapiSink(filename=filename, format="jsonl", biolink_version="2.4.8", owner=graph)
    
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, data in graph.edges(data=True):
        s.write_edge(data)
    s.finalize()
    
    assert os.path.exists(filename)
    
    # Verify the output by reading the lines
    lines = []
    with open(filename, 'r') as f:
        for line in f:
            lines.append(json.loads(line))
    
    # First line should be the header
    assert lines[0]['type'] == 'knowledge_graph'
    assert lines[0]['biolink_version'] == '2.4.8'
    
    # Count nodes and edges
    node_count = 0
    edge_count = 0
    for line in lines:
        if 'type' in line:
            if line['type'] == 'node':
                node_count += 1
            elif line['type'] == 'edge':
                edge_count += 1
    
    assert node_count == 2  # A and B
    assert edge_count == 1  # A-B


def test_write_trapi_json_with_knowledge_source():
    """
    Write a graph as TRAPI JSON with knowledge source using TrapiSink.
    """
    # Create a test graph
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_edge("A", "B", **{"subject": "A", "object": "B", "predicate": "biolink:related_to"})
    
    filename = os.path.join(TARGET_DIR, "test_trapi_with_ks.json")
    s = TrapiSink(filename=filename, knowledge_source="infores:test-source", owner=graph)
    
    # Add some extra attributes to test complex transformations
    for n, data in graph.nodes(data=True):
        data['provided_by'] = 'test_provider'
        data['description'] = f'Description of {data["name"]}'
        data['xref'] = [f'XREF:{n}1', f'XREF:{n}2']
        s.write_node(data)
    
    for u, v, data in graph.edges(data=True):
        data['knowledge_level'] = 'text_mining_agent'
        data['primary_knowledge_source'] = 'infores:primary-source'
        data['provided_by'] = ['provider1', 'provider2']
        s.write_edge(data)
    
    s.finalize()
    
    assert os.path.exists(filename)
    
    # Verify the output
    with open(filename, 'r') as f:
        content = json.load(f)
    
    # Check node attributes
    node = content['knowledge_graph']['nodes']['A']
    found_description = False
    found_provided_by = False
    found_xref = False
    
    for attr in node['attributes']:
        if attr['attribute_type_id'] == 'biolink:description':
            found_description = True
            assert attr['value'] == 'Description of Node A'
        elif attr['attribute_type_id'] == 'biolink:provided_by':
            found_provided_by = True
            assert 'test_provider' in attr['value']
        elif attr['attribute_type_id'] == 'biolink:xref':
            found_xref = True
            assert 'XREF:A1' in attr['value']
            assert 'XREF:A2' in attr['value']
    
    assert found_description
    assert found_provided_by
    assert found_xref
    
    # Check edge sources
    found_edge = False
    for edge_id, edge in content['knowledge_graph']['edges'].items():
        if 'sources' in edge:
            found_edge = True
            primary_source_found = False
            
            for source in edge['sources']:
                if source['resource_role'] == 'primary_knowledge_source':
                    primary_source_found = True
                    assert source['resource_id'] == 'infores:primary-source'
                    
            assert primary_source_found
            break
            
    assert found_edge


def test_write_trapi_compressed():
    """
    Write a graph as compressed TRAPI JSON using TrapiSink.
    """
    # Create a test graph
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_edge("A", "B", **{"subject": "A", "object": "B", "predicate": "biolink:related_to"})
    
    filename = os.path.join(TARGET_DIR, "test_trapi_compressed.json")
    s = TrapiSink(filename=filename, compression="gz", owner=graph)
    
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, data in graph.edges(data=True):
        s.write_edge(data)
    s.finalize()
    
    compressed_filename = f"{filename}.gz"
    assert os.path.exists(compressed_filename)
    
    # Verify the compressed output can be read
    with gzip.open(compressed_filename, 'rt') as f:
        content = json.load(f)
    
    assert 'knowledge_graph' in content
    assert len(content['knowledge_graph']['nodes']) == 2
    assert len(content['knowledge_graph']['edges']) == 1