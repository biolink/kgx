import importlib
from typing import Dict, Any, Optional
import sys
from os import path

import re

import requests
import yaml
import json
import logging

from kgx.graph.base_graph import BaseGraph

config: Optional[Dict[str, Any]] = None
logger: Optional[logging.Logger] = None
graph_store_class: Optional[BaseGraph] = None
jsonld_context_map: Dict = {}

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
        filepath = config["jsonld-context"][name]  # type: ignore
        if filepath.startswith("http"):
            try:
                content = requests.get(filepath).json()
            except ConnectionError:
                raise Exception(f"Unable to download JSON-LD context from {filepath}")
        else:
            if path.exists(filepath):
                content = json.load(open(filepath))

        if "@context" in content:
            content = content["@context"]
            jsonld_context_map[name] = content
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


def get_biolink_model_schema(biolink_release: Optional[str] = None) -> Optional[str]:
    if biolink_release:
        if not semver_pattern.fullmatch(biolink_release):
            raise TypeError(
                "The 'biolink_release' argument '"
                + biolink_release
                + "' is not a properly formatted 'major.minor.patch' semantic version?"
            )
        schema = f"https://raw.githubusercontent.com/biolink/biolink-model/{biolink_release}/biolink-model.yaml"
        return schema
    else:
        return None
