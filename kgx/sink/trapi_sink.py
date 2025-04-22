import os
import json
import gzip
from typing import Dict, Optional, Any, List
import jsonlines

from kgx.sink.sink import Sink
from kgx.config import get_logger

log = get_logger()

class TrapiSink(Sink):
    """
    TrapiSink is responsible for writing data in TRAPI (Translator Reasoner API) format.
    This sink converts KGX nodes and edges to TRAPI KnowledgeGraph format.

    Parameters
    ----------
    filename: str
        The filename to write to
    format: str
        The file format (``json`` or ``jsonl``)
    compression: Optional[str]
        The compression type (``gz``)
    biolink_version: Optional[str]
        The Biolink Model version to use
    knowledge_source: Optional[str]
        The default knowledge source to use when not provided in records
    kwargs: Any
        Any additional arguments

    """
    def __init__(
        self, 
        filename: str, 
        format: str = 'json', 
        compression: Optional[str] = None, 
        biolink_version: Optional[str] = None,
        knowledge_source: Optional[str] = None,
        owner = None,
        **kwargs: Any
    ):
        super().__init__(owner)
        self.filename = filename
        self.format = format
        self.compression = compression
        self.biolink_version = biolink_version
        self.knowledge_source = knowledge_source

        # Initialize TRAPI Knowledge Graph structure
        self.knowledge_graph = {
            "nodes": {},
            "edges": {}
        }

        # Track node and edge properties for TRAPI
        self.node_properties = set()
        self.edge_properties = set()
        
        # Ensure output directory exists
        dirname = os.path.abspath(os.path.dirname(filename))
        if dirname:
            os.makedirs(dirname, exist_ok=True)

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to the TRAPI Knowledge Graph.
        Converts KGX node format to TRAPI node format.

        Parameters
        ----------
        record: Dict
            A KGX node record
        """
        # Extract node ID
        node_id = record.get('id')
        if not node_id:
            log.warning(f"Node record is missing required 'id' field: {record}")
            return

        # Create TRAPI node structure
        trapi_node = {
            "name": record.get('name', ""),
            "categories": self._get_categories(record),
            "attributes": self._create_node_attributes(record),
        }

        # Add is_set if present
        if 'is_set' in record:
            trapi_node['is_set'] = record['is_set']

        # Add to knowledge graph
        self.knowledge_graph["nodes"][node_id] = trapi_node

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to the TRAPI Knowledge Graph.
        Converts KGX edge format to TRAPI edge format.

        Parameters
        ----------
        record: Dict
            A KGX edge record
        """
        # Extract required fields
        subject = record.get('subject')
        predicate = record.get('predicate')
        object = record.get('object')
        edge_id = record.get('id')

        # Validate required fields
        if not subject or not predicate or not object:
            log.warning(f"Edge record is missing required fields (subject, predicate, object): {record}")
            return

        # Create edge ID if not provided
        if not edge_id:
            edge_id = f"{subject}-{predicate}-{object}"

        # Create TRAPI edge structure
        trapi_edge = {
            "predicate": predicate,
            "subject": subject,
            "object": object,
            "attributes": self._create_edge_attributes(record),
            "sources": self._create_sources(record)
        }

        # Add qualifiers if present
        qualifiers = self._create_qualifiers(record)
        if qualifiers:
            trapi_edge["qualifiers"] = qualifiers

        # Add to knowledge graph
        self.knowledge_graph["edges"][edge_id] = trapi_edge

    def finalize(self) -> None:
        """
        Write the complete TRAPI Knowledge Graph to file.
        """
        output_path = self.filename
        
        # Prepare for compression if needed
        if self.compression == 'gz':
            if not output_path.endswith('.gz'):
                output_path = f"{output_path}.gz"
            
            with gzip.open(output_path, 'wt') as f:
                json.dump({"knowledge_graph": self.knowledge_graph}, f, indent=2)
        elif self.format == 'jsonl':
            # For JSONL, write each node and edge as separate lines
            with jsonlines.open(output_path, 'w') as writer:
                # Write header with metadata
                writer.write({
                    "type": "knowledge_graph",
                    "biolink_version": self.biolink_version
                })
                
                # Write nodes
                for node_id, node_data in self.knowledge_graph["nodes"].items():
                    writer.write({
                        "type": "node",
                        "id": node_id,
                        **node_data
                    })
                
                # Write edges
                for edge_id, edge_data in self.knowledge_graph["edges"].items():
                    writer.write({
                        "type": "edge",
                        "id": edge_id,
                        **edge_data
                    })
        else:
            # Default JSON format
            with open(output_path, 'w') as f:
                json.dump({"knowledge_graph": self.knowledge_graph}, f, indent=2)

        log.info(f"Wrote TRAPI Knowledge Graph to {output_path}")

    def _get_categories(self, record: Dict) -> List[str]:
        """
        Extract and format node categories from a KGX record.
        
        Parameters
        ----------
        record: Dict
            A KGX node record
            
        Returns
        -------
        List[str]
            List of Biolink categories for the node
        """
        categories = record.get('category', [])
        
        # Handle string or list categories
        if isinstance(categories, str):
            categories = [categories]
        elif not isinstance(categories, list):
            categories = []
            
        # Ensure all categories are properly prefixed with biolink:
        prefixed_categories = []
        for category in categories:
            if category and not category.startswith('biolink:'):
                category = f"biolink:{category}"
            prefixed_categories.append(category)
            
        return prefixed_categories if prefixed_categories else ["biolink:NamedThing"]

    def _create_node_attributes(self, record: Dict) -> List[Dict]:
        """
        Convert KGX node properties to TRAPI attributes.
        
        Parameters
        ----------
        record: Dict
            A KGX node record
            
        Returns
        -------
        List[Dict]
            List of TRAPI attribute objects
        """
        attributes = []
        
        # Skip these properties as they're handled directly in the TRAPI node structure
        skip_properties = {'id', 'name', 'category', 'is_set'}
        
        for key, value in record.items():
            if key in skip_properties or value is None or (isinstance(value, (list, set)) and len(value) == 0):
                continue
                
            # Convert KGX property to TRAPI attribute
            if key == 'provided_by':
                # Handle provided_by specially
                attribute = {
                    "attribute_type_id": "biolink:provided_by",
                    "value": value if isinstance(value, list) else [value],
                    "value_type_id": "biolink:Agent"
                }
                attributes.append(attribute)
            elif key == 'xref':
                # Handle xrefs specially
                attribute = {
                    "attribute_type_id": "biolink:xref",
                    "value": value if isinstance(value, list) else [value],
                    "value_type_id": "EDAM:data_0896"  # CURIE
                }
                attributes.append(attribute)
            elif key == 'synonym':
                # Handle synonyms specially
                attribute = {
                    "attribute_type_id": "biolink:synonym",
                    "value": value if isinstance(value, list) else [value],
                    "value_type_id": "biolink:Phenomenon"
                }
                attributes.append(attribute)
            elif key == 'description':
                attribute = {
                    "attribute_type_id": "biolink:description",
                    "value": value,
                    "value_type_id": "biolink:Phenomenon" 
                }
                attributes.append(attribute)
            elif not key.startswith('_'):  # Skip internal properties
                # Create a generic attribute for other properties
                attr_type_id = f"biolink:{key}" if not key.startswith('biolink:') else key
                
                attribute = {
                    "attribute_type_id": attr_type_id,
                    "value": value
                }
                attributes.append(attribute)
                
        return attributes

    def _create_edge_attributes(self, record: Dict) -> List[Dict]:
        """
        Convert KGX edge properties to TRAPI attributes.
        
        Parameters
        ----------
        record: Dict
            A KGX edge record
            
        Returns
        -------
        List[Dict]
            List of TRAPI attribute objects
        """
        attributes = []
        
        # Skip these properties as they're handled directly in the TRAPI edge structure
        skip_properties = {'id', 'subject', 'predicate', 'object', 'relation', 'sources', 
                          'primary_knowledge_source', 'knowledge_level', 'agent_type',
                          'pks_record_urls', 'qualifiers'}
        
        for key, value in record.items():
            if key in skip_properties or value is None or (isinstance(value, (list, set)) and len(value) == 0):
                continue
                
            # Convert KGX property to TRAPI attribute
            if key == 'provided_by':
                # Handle provided_by specially
                attribute = {
                    "attribute_type_id": "biolink:provided_by",
                    "value": value if isinstance(value, list) else [value],
                    "value_type_id": "biolink:Agent"
                }
                attributes.append(attribute)
            elif key == 'relation':
                attribute = {
                    "attribute_type_id": "biolink:relation",
                    "value": value
                }
                attributes.append(attribute)
            elif not key.startswith('_'):  # Skip internal properties
                # Create a generic attribute for other properties
                attr_type_id = f"biolink:{key}" if not key.startswith('biolink:') else key
                
                attribute = {
                    "attribute_type_id": attr_type_id,
                    "value": value
                }
                attributes.append(attribute)
                
        return attributes
        
    def _create_sources(self, record: Dict) -> List[Dict]:
        """
        Create TRAPI sources from KGX edge record.
        
        Parameters
        ----------
        record: Dict
            A KGX edge record
            
        Returns
        -------
        List[Dict]
            List of TRAPI retrieval source objects
        """
        sources = []
        
        # Handle primary_knowledge_source
        primary_ks = record.get('primary_knowledge_source')
        if primary_ks:
            source = {
                "resource_id": primary_ks if primary_ks.startswith('infores:') else f"infores:{primary_ks}",
                "resource_role": "primary_knowledge_source"
            }
            
            # Add URLs if available
            pks_record_urls = record.get('pks_record_urls')
            if pks_record_urls:
                if isinstance(pks_record_urls, str):
                    source["source_record_urls"] = [pks_record_urls]
                else:
                    source["source_record_urls"] = pks_record_urls
                    
            sources.append(source)
        elif self.knowledge_source:
            # Use default knowledge source if specified
            source = {
                "resource_id": self.knowledge_source if self.knowledge_source.startswith('infores:') else f"infores:{self.knowledge_source}",
                "resource_role": "primary_knowledge_source"
            }
            sources.append(source)
        else:
            # Create a minimal source with "not_provided"
            source = {
                "resource_id": "infores:unknown",
                "resource_role": "primary_knowledge_source"
            }
            sources.append(source)
            
        # Handle additional sources from provided_by if necessary
        provided_by = record.get('provided_by')
        if provided_by and provided_by != primary_ks:
            if isinstance(provided_by, str):
                provided_by = [provided_by]
                
            for provider in provided_by:
                if provider != primary_ks:
                    source = {
                        "resource_id": provider if provider.startswith('infores:') else f"infores:{provider}",
                        "resource_role": "aggregator_knowledge_source"
                    }
                    sources.append(source)
                    
        return sources
        
    def _create_qualifiers(self, record: Dict) -> List[Dict]:
        """
        Create TRAPI qualifiers from KGX edge record, if present.
        
        Parameters
        ----------
        record: Dict
            A KGX edge record
            
        Returns
        -------
        List[Dict]
            List of TRAPI qualifier objects, or None if no qualifiers present
        """
        qualifiers = []
        
        # Extract qualifiers if present in the record
        if 'qualifiers' in record and record['qualifiers']:
            for q in record['qualifiers']:
                if isinstance(q, dict) and 'qualifier_type_id' in q and 'qualifier_value' in q:
                    qualifiers.append(q)
                    
        # Check for flattened qualifiers in the record
        qualifier_types = [k for k in record.keys() if k.endswith('_qualifier')]
        for qt in qualifier_types:
            if record[qt]:
                qualifier = {
                    "qualifier_type_id": f"biolink:{qt}" if not qt.startswith('biolink:') else qt,
                    "qualifier_value": record[qt]
                }
                qualifiers.append(qualifier)
                
        return qualifiers if qualifiers else None