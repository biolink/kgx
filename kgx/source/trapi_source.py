import gzip
import json
import typing
import ijson
from itertools import chain
from typing import Dict, Tuple, Generator, Optional, Any, List

from kgx.source.json_source import JsonSource
from kgx.config import get_logger

log = get_logger()


class TrapiSource(JsonSource):
    """
    TrapiSource is responsible for reading data as records
    from a TRAPI (Translator Reasoner API) compliant JSON.
    
    This class handles TRAPI 1.5.0 specification.
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.node_properties = set()
        self.edge_properties = set()
        self.biolink_version = None

    def parse(
        self,
        filename: str,
        format: str = "json",
        compression: Optional[str] = None,
        **kwargs: Any
    ) -> typing.Generator:
        """
        This method reads from a TRAPI JSON and yields KGX records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``json`` or ``jsonl``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records
        """
        self.set_provenance_map(kwargs)
            
        # Try to extract biolink version from the file
        self._extract_biolink_version(filename, compression)
            
        if format == 'jsonl':
            # Handle JSONL format
            n = self.read_nodes_jsonl(filename, compression)
            e = self.read_edges_jsonl(filename, compression)
        else:
            # Handle standard JSON format
            n = self.read_nodes(filename, compression)
            e = self.read_edges(filename, compression)
            
        yield from chain(n, e)

    def _extract_biolink_version(self, filename: str, compression: Optional[str] = None) -> None:
        """
        Extract biolink version from the TRAPI response if available.
        
        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type
        """
        try:
            if compression == 'gz':
                with gzip.open(filename, 'rt') as f:
                    data = json.load(f)
            else:
                with open(filename, 'r') as f:
                    data = json.load(f)
                    
            if 'biolink_version' in data:
                self.biolink_version = data['biolink_version']
            elif 'message' in data and 'biolink_version' in data:
                self.biolink_version = data['biolink_version']
                
        except Exception as e:
            log.warning(f"Could not extract biolink version from file {filename}: {e}")

    def read_nodes(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read node records from a TRAPI JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for node records
        """
        if compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        
        # First try message.knowledge_graph.nodes path (TRAPI Response)
        nodes_found = False
        try:
            for node_id, node_data in ijson.kvitems(FH, 'message.knowledge_graph.nodes'):
                nodes_found = True
                node_data['id'] = node_id
                yield self.load_node(node_data)
        except (KeyError, ijson.JSONError):
            if nodes_found:
                # If we found nodes but then got an error, it's a real error
                FH.close()
                log.error(f"Error parsing nodes from {filename}")
                return
                
        # If we get here, try knowledge_graph.nodes directly (standalone KG)
        if not nodes_found:
            FH.close()
            if compression == "gz":
                FH = gzip.open(filename, "rb")
            else:
                FH = open(filename, "rb")
                
            try:
                for node_id, node_data in ijson.kvitems(FH, 'knowledge_graph.nodes'):
                    node_data['id'] = node_id
                    yield self.load_node(node_data)
            except (KeyError, ijson.JSONError) as e:
                log.warning(f"Could not find nodes in TRAPI format in {filename}: {str(e)}")
            finally:
                FH.close()

    def read_edges(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read edge records from a TRAPI JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for edge records
        """
        if compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
            
        # First try message.knowledge_graph.edges path (TRAPI Response)
        edges_found = False
        try:
            for edge_id, edge_data in ijson.kvitems(FH, 'message.knowledge_graph.edges'):
                edges_found = True
                edge_data['id'] = edge_id
                yield self.load_edge(edge_data)
        except (KeyError, ijson.JSONError):
            if edges_found:
                # If we found edges but then got an error, it's a real error
                FH.close()
                log.error(f"Error parsing edges from {filename}")
                return
                
        # If we get here, try knowledge_graph.edges directly (standalone KG)
        if not edges_found:
            FH.close()
            if compression == "gz":
                FH = gzip.open(filename, "rb")
            else:
                FH = open(filename, "rb")
                
            try:
                for edge_id, edge_data in ijson.kvitems(FH, 'knowledge_graph.edges'):
                    edge_data['id'] = edge_id
                    yield self.load_edge(edge_data)
            except (KeyError, ijson.JSONError) as e:
                log.warning(f"Could not find edges in TRAPI format in {filename}: {str(e)}")
            finally:
                FH.close()
                
    def read_nodes_jsonl(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read node records from a TRAPI JSONL file.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for node records
        """
        if compression == "gz":
            FH = gzip.open(filename, 'rt')
        else:
            FH = open(filename, 'r')
            
        for line in FH:
            try:
                record = json.loads(line)
                if 'type' in record and record['type'] == 'node' and 'id' in record:
                    node_id = record['id']
                    # Remove type and id as they're handled differently
                    record_copy = record.copy()
                    if 'type' in record_copy:
                        del record_copy['type']
                    if 'id' in record_copy:
                        del record_copy['id']
                    
                    # Convert to standard KGX format
                    yield self.load_node((node_id, record_copy))
            except json.JSONDecodeError:
                log.warning(f"Error parsing JSONL line in {filename}")
                
        FH.close()
                
    def read_edges_jsonl(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read edge records from a TRAPI JSONL file.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for edge records
        """
        if compression == "gz":
            FH = gzip.open(filename, 'rt')
        else:
            FH = open(filename, 'r')
            
        for line in FH:
            try:
                record = json.loads(line)
                if 'type' in record and record['type'] == 'edge' and 'id' in record:
                    edge_id = record['id']
                    # Remove type and id as they're handled differently
                    record_copy = record.copy()
                    if 'type' in record_copy:
                        del record_copy['type']
                    if 'id' in record_copy:
                        del record_copy['id']
                    
                    # Add ID back as a property
                    record_copy['id'] = edge_id
                    
                    # Convert to standard KGX format
                    yield self.load_edge(record_copy)
            except json.JSONDecodeError:
                log.warning(f"Error parsing JSONL line in {filename}")
                
        FH.close()

    def load_node(self, node: Dict) -> Tuple[str, Dict]:
        """
        Load a TRAPI node into KGX format

        Parameters
        ----------
        node : Dict
            A TRAPI node

        Returns
        -------
        Tuple[str, Dict]
            A tuple containing (node_id, node_data) in KGX format
        """
        # Handle legacy field name
        if "type" in node and "category" not in node and "categories" not in node:
            node["category"] = node["type"]
            del node["type"]
            
        # Handle TRAPI format
        if "categories" in node and "category" not in node:
            node["category"] = node["categories"]
            
        # Process attributes if present
        if "attributes" in node and node["attributes"]:
            self._process_node_attributes(node["attributes"], node)
            
        return super().read_node(node)
        
    def _process_node_attributes(self, attributes: List[Dict], kgx_node: Dict) -> None:
        """
        Process TRAPI node attributes into KGX properties
        
        Parameters
        ----------
        attributes : List[Dict]
            List of TRAPI attribute objects
        kgx_node : Dict
            KGX node object to update
        """
        for attr in attributes:
            if 'attribute_type_id' not in attr or 'value' not in attr:
                continue
                
            attr_type = attr['attribute_type_id']
            attr_value = attr['value']
            
            # Strip biolink: prefix for standard property names
            if attr_type.startswith('biolink:'):
                prop_name = attr_type[8:]  # Remove 'biolink:' prefix
            else:
                prop_name = attr_type
                
            # Handle special attributes
            if prop_name == 'provided_by':
                kgx_node['provided_by'] = attr_value
            elif prop_name == 'synonym':
                kgx_node['synonym'] = attr_value
            elif prop_name == 'xref':
                kgx_node['xref'] = attr_value
            elif prop_name == 'description':
                kgx_node['description'] = attr_value
            else:
                # For all other attributes, use the attribute type as property name
                kgx_node[prop_name] = attr_value

    def load_edge(self, edge: Dict) -> Tuple[str, str, str, Dict]:
        """
        Load a TRAPI edge into KGX format

        Parameters
        ----------
        edge : Dict
            A TRAPI edge

        Returns
        -------
        Tuple[str, str, str, Dict]
            A tuple containing (subject_id, object_id, edge_id, edge_data) in KGX format
        """
        # Handle legacy field names
        if "source_id" in edge and "subject" not in edge:
            edge["subject"] = edge["source_id"]
        if "target_id" in edge and "object" not in edge:
            edge["object"] = edge["target_id"]
        if "relation_label" in edge and "predicate" not in edge:
            if isinstance(edge["relation_label"], list):
                edge["predicate"] = edge["relation_label"][0]
            else:
                edge["predicate"] = edge["relation_label"]
        
        # Process edge attributes
        if "attributes" in edge and edge["attributes"]:
            self._process_edge_attributes(edge["attributes"], edge)
            
        # Process sources
        if "sources" in edge:
            self._process_sources(edge["sources"], edge)
            
        # Track qualifiers
        if "qualifiers" in edge:
            # Keep qualifiers as is
            pass
            
        return super().read_edge(edge)
        
    def _process_edge_attributes(self, attributes: List[Dict], kgx_edge: Dict) -> None:
        """
        Process TRAPI edge attributes into KGX properties
        
        Parameters
        ----------
        attributes : List[Dict]
            List of TRAPI attribute objects
        kgx_edge : Dict
            KGX edge object to update
        """
        for attr in attributes:
            if 'attribute_type_id' not in attr or 'value' not in attr:
                continue
                
            attr_type = attr['attribute_type_id']
            attr_value = attr['value']
            
            # Strip biolink: prefix for standard property names
            if attr_type.startswith('biolink:'):
                prop_name = attr_type[8:]  # Remove 'biolink:' prefix
            else:
                prop_name = attr_type
                
            # Handle special attributes
            if prop_name == 'provided_by':
                kgx_edge['provided_by'] = attr_value
            elif prop_name == 'relation':
                kgx_edge['relation'] = attr_value
            else:
                # For all other attributes, use the attribute type as property name
                kgx_edge[prop_name] = attr_value
                
    def _process_sources(self, sources: List[Dict], kgx_edge: Dict) -> None:
        """
        Process TRAPI retrieval sources into KGX properties
        
        Parameters
        ----------
        sources : List[Dict]
            List of TRAPI retrieval source objects
        kgx_edge : Dict
            KGX edge object to update
        """
        # Find primary knowledge source
        for source in sources:
            if 'resource_role' in source and source['resource_role'] == 'primary_knowledge_source':
                if 'resource_id' in source:
                    kgx_edge['primary_knowledge_source'] = source['resource_id']
                    
                    # Add record URLs if available
                    if 'source_record_urls' in source:
                        kgx_edge['pks_record_urls'] = source['source_record_urls']
                    break
                    
        # Collect all provided_by sources
        provided_by = []
        for source in sources:
            if 'resource_id' in source:
                provided_by.append(source['resource_id'])
                
        if provided_by:
            kgx_edge['provided_by'] = provided_by
