import importlib
from functools import lru_cache
from typing import Dict, Any, Optional
import sys
from os import path

import re

import requests
import yaml
import json
import logging

import curies
from kgx.graph.base_graph import BaseGraph

config: Optional[Dict[str, Any]] = None
logger: Optional[logging.Logger] = None
graph_store_class: Optional[BaseGraph] = None
jsonld_context_map: Dict[str, Dict[str, str]] = {}

CONFIG_FILENAME = path.join(path.dirname(path.abspath(__file__)), "config.yml")


def get_config(filename: str = CONFIG_FILENAME) -> Dict:
    """
    Get config as a Dictionary

    Parameters
    ----------
    filename: str
        The filename with all the configuration

    Returns
    -------
    Dict
        A Dictionary containing all the entries from the config YAML

    """
    global config
    if config is None:
        config = yaml.load(open(filename), Loader=yaml.FullLoader)
    return config


@lru_cache
def get_converter(name: str = "biolink") -> curies.Converter:
    """Get contents of a JSON-LD context."""
    filepath = config["jsonld-context"][name]  # type: ignore
    return curies.load_jsonld_context(filepath)


def get_jsonld_context(name: str = "biolink"):
    """
    Get contents of a JSON-LD context.

    Returns
    -------
    Dict
        the contents of the JSON-LD context

    """
    content = None
    if name in jsonld_context_map:
        content = jsonld_context_map[name]
    else:
        converter = get_converter(name)
        jsonld_context_map[name] = converter.bimap
    return content


def get_logger(name: str = "KGX") -> logging.Logger:
    """
    Get an instance of logger.

    Parameters
    ----------
    name: str
        The name of logger

    Returns
    -------
    logging.Logger
        An instance of logging.Logger

    """
    global logger
    if logger is None:
        config = get_config()
        logger = logging.getLogger(name)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(config["logging"]["format"])
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(config["logging"]["level"])
        logger.propagate = False
    return logger


def get_graph_store_class() -> Any:
    """
    Get a reference to the graph store class, as defined the config.
    Defaults to ``kgx.graph.nx_graph.NxGraph``

    Returns
    -------
    Any
        A reference to the graph store class

    """
    global graph_store_class
    if not graph_store_class:
        config = get_config()
        if "graph_store" in config:
            name = config["graph_store"]
        else:
            name = "kgx.graph.nx_graph.NxGraph"
        module_name = ".".join(name.split(".")[0:-1])
        class_name = name.split(".")[-1]
        graph_store_class = getattr(importlib.import_module(module_name), class_name)
    return graph_store_class


# Biolink Release number should be a well formed Semantic Versioning (patch is optional?)
semver_pattern = re.compile(r"^\d+\.\d+\.\d+$")
semver_pattern_v = re.compile(r"^v\d+\.\d+\.\d+$")


def get_biolink_model_schema(biolink_release: Optional[str] = None) -> Optional[str]:
    """
    Get Biolink Model Schema
    """
    if biolink_release:
        if not semver_pattern.fullmatch(biolink_release) and not semver_pattern_v.fullmatch(biolink_release):
            raise TypeError(
                "The 'biolink_release' argument '"
                + biolink_release
                + "' is not a properly formatted 'major.minor.patch' semantic version?"
            )
        schema = f"https://raw.githubusercontent.com/biolink/biolink-model/{biolink_release}/biolink-model.yaml"
        return schema
    else:
        return None
