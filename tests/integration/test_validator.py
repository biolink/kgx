import os
from sys import stderr
from kgx.utils.kgx_utils import get_toolkit
from kgx.validator import Validator
from kgx.graph.nx_graph import NxGraph
from kgx.transformer import Transformer
from tests import RESOURCE_DIR
from bmt import Toolkit

toolkit = Toolkit()


def test_validator_bad():
    """
    A fake test to establish a fail condition for validation.
    """
    G = NxGraph()
    G.add_node("x", foo=3)
    G.add_node("ZZZ:3", **{"nosuch": 1})
    G.add_edge("x", "y", **{"baz": 6})
    validator = Validator(verbose=True)
    validator.validate(G)
    assert len(validator.get_errors()) > 0


def test_validator_good():
    """
    A fake test to establish a success condition for validation.
    """
    G = NxGraph()
    G.add_node(
        "UniProtKB:P123456", id="UniProtKB:P123456", name="fake", category=["Protein"]
    )
    G.add_node(
        "UBERON:0000001", id="UBERON:0000001", name="fake", category=["NamedThing"]
    )
    G.add_node(
        "UBERON:0000002", id="UBERON:0000002", name="fake", category=["NamedThing"]
    )
    G.add_edge(
        "UBERON:0000001",
        "UBERON:0000002",
        id="UBERON:0000001-part_of-UBERON:0000002",
        relation="RO:1",
        predicate="part_of",
        subject="UBERON:0000001",
        object="UBERON:0000002",
        category=["biolink:Association"],
        knowledge_level="not_provided",
        agent_type="not_provided",
    )
    validator = Validator(verbose=True)
    validator.validate(G)
    print(validator.get_errors())
    assert len(validator.get_errors()) == 0


def test_validate_json():
    """
    Validate against a valid representative Biolink Model compliant JSON.
    """
    input_args = {
        "filename": [os.path.join(RESOURCE_DIR, "valid.json")],
        "format": "json",
    }
    t = Transformer(stream=True)
    t.transform(input_args)
    validator = Validator()
    validator.validate(t.store.graph)
    assert len(validator.get_errors()) == 0


def test_distinct_validator_class_versus_default_toolkit_biolink_version():
    Validator.set_biolink_model(version="1.8.2")
    default_tk = get_toolkit()
    validator_tk = Validator.get_toolkit()
    assert default_tk.get_model_version() != validator_tk.get_model_version()


def test_distinct_class_versus_validator_instance_biolink_version():
    Validator.set_biolink_model(version="1.7.0")
    validator = Validator()
    Validator.set_biolink_model(version="1.8.2")
    validator_class_tk = Validator.get_toolkit()
    validation_instance_version = validator.get_validation_model_version()
    assert validation_instance_version != validator_class_tk.get_model_version()


def test_validator_explicit_biolink_version():
    """
    A fake test to establish a success condition for validation.
    """
    G = NxGraph()
    G.add_node(
        "CHEMBL.COMPOUND:1222250",
        id="CHEMBL.COMPOUND:1222250",
        name="Dextrose",
        category=["NamedThing"]
    )
    G.add_node(
        "UBERON:0000001", id="UBERON:0000001", name="fake", category=["NamedThing"]
    )
    G.add_edge(
        "CHEMBL.COMPOUND:1222250",
        "UBERON:0000001",
        id="CHEMBL.COMPOUND:1222250-part_of-UBERON:0000001",
        relation="RO:1",
        predicate="part_of",
        subject="CHEMBL.COMPOUND:1222250",
        object="UBERON:0000001",
        category=["biolink:Association"],
        knowledge_level="not_provided",
        agent_type="not_provided",
    )
    Validator.set_biolink_model(toolkit.get_model_version())
    validator = Validator(verbose=True)
    validator.validate(G)
    print(validator.get_errors())
    assert len(validator.get_errors()) == 0


def test_validator():
    """
    Test generate the validate function by streaming
    graph data through a graph Transformer.process() Inspector
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
        "aggregator_knowledge_source": True,
    }

    Validator.set_biolink_model(toolkit.get_model_version())

    # Validator assumes the currently set Biolink Release
    validator = Validator()

    transformer = Transformer(stream=True)

    transformer.transform(
        input_args=input_args,
        output_args={
            "format": "null"
        },  # streaming processing throws the graph data away
        # ... Second, we inject the Inspector into the transform() call,
        # for the underlying Transformer.process() to use...
        inspector=validator,
    )

    validator.write_report()

    e = validator.get_errors()
    assert len(e) == 0
