import typing
from typing import Any, Dict, List, Optional, Iterator, Tuple, Generator

try:
    import duckdb
except ImportError:
    duckdb = None

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    sanitize_import,
    knowledge_provenance_properties,
)

log = get_logger()


class DuckDbSource(Source):
    """
    DuckDbSource is responsible for reading data as records
    from a DuckDB database with nodes and edges tables.
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.node_count = 0
        self.edge_count = 0
        self.seen_nodes = set()

    def _connect_db(self, database_path: str):
        """Connect to DuckDB database."""
        if duckdb is None:
            raise ImportError("duckdb package is required for DuckDbSource")
        
        self.connection = duckdb.connect(database_path, read_only=True)
        
        # Verify tables exist
        tables = self.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name IN ('nodes', 'edges')"
        ).fetchall()
        
        table_names = [table[0] for table in tables]
        if 'nodes' not in table_names:
            raise ValueError("Required 'nodes' table not found in DuckDB database")
        if 'edges' not in table_names:
            raise ValueError("Required 'edges' table not found in DuckDB database")

    def parse(
        self,
        filename: str,
        node_filters: Dict = None,
        edge_filters: Dict = None,
        start: int = 0,
        end: int = None,
        is_directed: bool = True,
        page_size: int = 50000,
        **kwargs: Any,
    ) -> typing.Generator:
        """
        This method reads from DuckDB instance and yields records

        Parameters
        ----------
        filename: str
            The path to the DuckDB database file
        node_filters: Dict
            Node filters
        edge_filters: Dict
            Edge filters
        start: int
            Number of records to skip before streaming
        end: int
            Total number of records to fetch
        is_directed: bool
            Whether or not the edges should be treated as directed
        page_size: int
            The size of each page/batch fetched from DuckDB (``50000``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """
        self._connect_db(filename)

        self.set_provenance_map(kwargs)

        kwargs["is_directed"] = is_directed
        self.node_filters = node_filters
        self.edge_filters = edge_filters
        
        record_count = 0
        
        # Process nodes first
        for page in self.get_pages(
            self.get_nodes, start, end, page_size=page_size, **kwargs
        ):
            for record in self.load_nodes(page):
                if end is not None and record_count >= end:
                    return
                if record_count >= start:
                    yield record
                record_count += 1
        
        # Process edges
        for page in self.get_pages(
            self.get_edges, 
            max(0, start - record_count), 
            end - record_count if end is not None else None, 
            page_size=page_size, 
            **kwargs
        ):
            for record in self.load_edges(page):
                if end is not None and record_count >= end:
                    return
                if record_count >= start:
                    yield record
                record_count += 1

    def get_pages(
        self,
        query_function,
        start: int = 0,
        end: int = None,
        page_size: int = 50000,
        **kwargs: Any,
    ) -> Generator:
        """
        Get pages of data from query function.
        """
        offset = start
        while True:
            if end and offset >= end:
                break
                
            current_page_size = page_size
            if end and (offset + page_size) > end:
                current_page_size = end - offset
                
            page_data = query_function(
                offset=offset, limit=current_page_size, **kwargs
            )
            
            if not page_data:
                break
                
            yield page_data
            offset += len(page_data)
            
            if len(page_data) < current_page_size:
                break

    def get_nodes(self, offset: int = 0, limit: int = 50000, **kwargs) -> List[Dict]:
        """
        Get nodes from the nodes table.
        """
        query = "SELECT * FROM nodes"
        params = []
        
        # Apply node filters if provided
        if self.node_filters:
            conditions = []
            for key, values in self.node_filters.items():
                if isinstance(values, list):
                    placeholders = ','.join(['?' for _ in values])
                    conditions.append(f"{key} IN ({placeholders})")
                    params.extend(values)
                else:
                    conditions.append(f"{key} = ?")
                    params.append(values)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        query += f" LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        try:
            result = self.connection.execute(query, params).fetchall()
            columns = [desc[0] for desc in self.connection.description]
            
            # Convert rows to dictionaries
            nodes = []
            for row in result:
                node_dict = dict(zip(columns, row))
                nodes.append(node_dict)
                
            return nodes
        except Exception as e:
            log.error(f"Error executing node query: {e}")
            return []

    def get_edges(self, offset: int = 0, limit: int = 50000, **kwargs) -> List[Dict]:
        """
        Get edges from the edges table.
        """
        query = "SELECT * FROM edges"
        params = []
        
        # Apply edge filters if provided
        if self.edge_filters:
            conditions = []
            for key, values in self.edge_filters.items():
                if isinstance(values, list):
                    placeholders = ','.join(['?' for _ in values])
                    conditions.append(f"{key} IN ({placeholders})")
                    params.extend(values)
                else:
                    conditions.append(f"{key} = ?")
                    params.append(values)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        query += f" LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        try:
            result = self.connection.execute(query, params).fetchall()
            columns = [desc[0] for desc in self.connection.description]
            
            # Convert rows to dictionaries
            edges = []
            for row in result:
                edge_dict = dict(zip(columns, row))
                edges.append(edge_dict)
                
            return edges
        except Exception as e:
            log.error(f"Error executing edge query: {e}")
            return []

    def load_nodes(self, nodes: List[Dict]) -> Generator:
        """
        Load nodes from a list of node dictionaries.
        """
        for node_data in nodes:
            if 'id' not in node_data:
                log.warning("Node missing required 'id' field, skipping")
                continue
                
            node_id = node_data['id']
            
            if node_id in self.seen_nodes:
                continue
                
            self.seen_nodes.add(node_id)
            
            # Apply node filters
            if not self.check_node_filter(node_data):
                continue
            
            # Process node data
            processed_node = self.process_node(node_data)
            if processed_node:
                self.node_count += 1
                yield processed_node

    def load_edges(self, edges: List[Dict]) -> Generator:
        """
        Load edges from a list of edge dictionaries.
        """
        for edge_data in edges:
            if not all(key in edge_data for key in ['subject', 'predicate', 'object']):
                log.warning("Edge missing required fields (subject, predicate, object), skipping")
                continue
            
            # Apply edge filters
            if not self.check_edge_filter(edge_data):
                continue
            
            # Process edge data
            processed_edge = self.process_edge(edge_data)
            if processed_edge:
                self.edge_count += 1
                yield processed_edge

    def process_node(self, node_data: Dict) -> Optional[Tuple]:
        """
        Process a node dictionary into KGX format.
        """
        try:
            node = {
                'id': node_data['id'],
                'category': node_data.get('category', 'biolink:NamedThing')
            }
            
            # Add other properties
            for key, value in node_data.items():
                if key not in ['id', 'category'] and value is not None:
                    node[key] = value
            
            # Return (node_id, node_data) format expected by transformer
            return (node_data['id'], node)
        except Exception as e:
            log.error(f"Error processing node: {e}")
            return None

    def process_edge(self, edge_data: Dict) -> Optional[Tuple]:
        """
        Process an edge dictionary into KGX format.
        """
        try:
            edge = {
                'subject': edge_data['subject'],
                'predicate': edge_data['predicate'],
                'object': edge_data['object']
            }
            
            # Generate edge key if not provided
            if 'id' in edge_data and edge_data['id']:
                edge_key = edge_data['id']
            else:
                edge_key = generate_edge_key(
                    edge_data['subject'],
                    edge_data['predicate'],
                    edge_data['object']
                )
            edge['id'] = edge_key
            
            # Add other properties
            for key, value in edge_data.items():
                if key not in ['id', 'subject', 'predicate', 'object'] and value is not None:
                    edge[key] = value
            
            # Return (subject, object, edge_key, edge_data) format expected by transformer
            return (edge_data['subject'], edge_data['object'], edge_key, edge)
        except Exception as e:
            log.error(f"Error processing edge: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None