"""
Data schemas and models for DSX file parsing and ASG representation.

This module defines all data structures used throughout the ETL migration system,
providing type safety and validation.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime


class StageType(str, Enum):
    """Enumeration of supported DataStage stage types."""
    SEQUENTIAL_FILE = "SequentialFile"
    TRANSFORMER = "Transformer"
    JOIN = "Join"
    LOOKUP = "Lookup"
    SORT = "Sort"
    AGGREGATOR = "Aggregator"
    FILTER = "Filter"
    PIVOT = "Pivot"
    DATABASE = "Database"
    GENERIC = "Generic"
    # Add more stage types as needed
    UNKNOWN = "Unknown"


class DataType(str, Enum):
    """Enumeration of supported data types."""
    INTEGER = "Integer"
    VARCHAR = "Varchar"
    CHAR = "Char"
    DECIMAL = "Decimal"
    DATE = "Date"
    TIMESTAMP = "Timestamp"
    DOUBLE = "Double"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    UNKNOWN = "Unknown"


class EdgeType(str, Enum):
    """Enumeration of edge types in the ASG."""
    DATA_FLOW = "DataFlow"
    CONTROL_FLOW = "ControlFlow"
    DEPENDENCY = "Dependency"


@dataclass
class FieldDefinition:
    """Schema definition for a single field in a stage."""
    name: str
    data_type: DataType
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: Optional[bool] = True
    default_value: Optional[str] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        """Validate field definition after initialization."""
        if self.name is None or not self.name.strip():
            raise ValueError("Field name cannot be empty")
        if not isinstance(self.data_type, DataType):
            try:
                self.data_type = DataType(self.data_type)
            except ValueError:
                self.data_type = DataType.UNKNOWN


@dataclass
class Transformation:
    """Represents a data transformation rule."""
    output_field: str
    expression: str
    transformation_type: Optional[str] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        """Validate transformation after initialization."""
        if not self.output_field or not self.output_field.strip():
            raise ValueError("Output field name cannot be empty")
        if not self.expression or not self.expression.strip():
            raise ValueError("Transformation expression cannot be empty")


@dataclass
class StageProperties:
    """Properties specific to a DataStage stage."""
    file_path: Optional[str] = None
    field_definitions: List[FieldDefinition] = field(default_factory=list)
    input_fields: List[str] = field(default_factory=list)
    output_fields: List[str] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    connection_string: Optional[str] = None
    table_name: Optional[str] = None
    query: Optional[str] = None
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    
    def add_custom_property(self, key: str, value: Any) -> None:
        """Add a custom property to the stage."""
        self.custom_properties[key] = value
    
    def get_field_definition(self, field_name: str) -> Optional[FieldDefinition]:
        """Get field definition by name."""
        for field_def in self.field_definitions:
            if field_def.name == field_name:
                return field_def
        return None


@dataclass
class Stage:
    """Represents a DataStage stage node."""
    name: str
    stage_type: StageType
    properties: StageProperties
    id: Optional[str] = None  # Optional unique identifier
    
    def __post_init__(self):
        """Validate stage after initialization."""
        if not self.name or not self.name.strip():
            raise ValueError("Stage name cannot be empty")
        if not isinstance(self.stage_type, StageType):
            try:
                self.stage_type = StageType(self.stage_type)
            except ValueError:
                self.stage_type = StageType.UNKNOWN
        if self.id is None:
            self.id = self.name


@dataclass
class Link:
    """Represents a connection/link between stages."""
    source_stage: str
    target_stage: str
    edge_type: EdgeType = EdgeType.DATA_FLOW
    link_properties: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate link after initialization."""
        if not self.source_stage or not self.source_stage.strip():
            raise ValueError("Source stage name cannot be empty")
        if not self.target_stage or not self.target_stage.strip():
            raise ValueError("Target stage name cannot be empty")
        if not isinstance(self.edge_type, EdgeType):
            try:
                self.edge_type = EdgeType(self.edge_type)
            except ValueError:
                self.edge_type = EdgeType.DATA_FLOW


@dataclass
class Job:
    """Represents a complete DataStage job."""
    job_name: str
    stages: List[Stage] = field(default_factory=list)
    links: List[Link] = field(default_factory=list)
    description: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate job after initialization."""
        if not self.job_name or not self.job_name.strip():
            raise ValueError("Job name cannot be empty")
    
    def get_stage(self, stage_name: str) -> Optional[Stage]:
        """Get a stage by name."""
        for stage in self.stages:
            if stage.name == stage_name:
                return stage
        return None
    
    def get_stages_by_type(self, stage_type: StageType) -> List[Stage]:
        """Get all stages of a specific type."""
        return [stage for stage in self.stages if stage.stage_type == stage_type]
    
    def get_outgoing_links(self, stage_name: str) -> List[Link]:
        """Get all links originating from a stage."""
        return [link for link in self.links if link.source_stage == stage_name]
    
    def get_incoming_links(self, stage_name: str) -> List[Link]:
        """Get all links targeting a stage."""
        return [link for link in self.links if link.target_stage == stage_name]


@dataclass
class ASGNode:
    """Represents a node in the Abstract Syntax Graph."""
    node_id: str
    node_type: str
    stage: Stage
    properties: Dict[str, Any] = field(default_factory=dict)
    level: Optional[int] = None  # Hierarchy level in the graph
    
    def __post_init__(self):
        """Validate ASG node after initialization."""
        if not self.node_id or not self.node_id.strip():
            raise ValueError("Node ID cannot be empty")
        if not self.node_type or not self.node_type.strip():
            raise ValueError("Node type cannot be empty")


@dataclass
class ASGEdge:
    """Represents an edge in the Abstract Syntax Graph."""
    source_id: str
    target_id: str
    edge_type: EdgeType
    properties: Dict[str, Any] = field(default_factory=dict)
    weight: Optional[float] = None
    
    def __post_init__(self):
        """Validate ASG edge after initialization."""
        if not self.source_id or not self.source_id.strip():
            raise ValueError("Source node ID cannot be empty")
        if not self.target_id or not self.target_id.strip():
            raise ValueError("Target node ID cannot be empty")
        if not isinstance(self.edge_type, EdgeType):
            try:
                self.edge_type = EdgeType(self.edge_type)
            except ValueError:
                self.edge_type = EdgeType.DATA_FLOW


@dataclass
class AbstractSyntaxGraph:
    """Complete Abstract Syntax Graph representation."""
    nodes: Dict[str, ASGNode] = field(default_factory=dict)
    edges: List[ASGEdge] = field(default_factory=list)
    adjacency_list: Dict[str, List[str]] = field(default_factory=dict)
    reverse_adjacency_list: Dict[str, List[str]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_node(self, node: ASGNode) -> None:
        """Add a node to the graph."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node with ID '{node.node_id}' already exists")
        self.nodes[node.node_id] = node
        if node.node_id not in self.adjacency_list:
            self.adjacency_list[node.node_id] = []
        if node.node_id not in self.reverse_adjacency_list:
            self.reverse_adjacency_list[node.node_id] = []
    
    def add_edge(self, edge: ASGEdge) -> None:
        """Add an edge to the graph."""
        if edge.source_id not in self.nodes:
            raise ValueError(f"Source node '{edge.source_id}' does not exist")
        if edge.target_id not in self.nodes:
            raise ValueError(f"Target node '{edge.target_id}' does not exist")
        
        # Check for duplicate edges
        for existing_edge in self.edges:
            if (existing_edge.source_id == edge.source_id and 
                existing_edge.target_id == edge.target_id):
                return  # Edge already exists
        
        self.edges.append(edge)
        self.adjacency_list[edge.source_id].append(edge.target_id)
        self.reverse_adjacency_list[edge.target_id].append(edge.source_id)
    
    def get_node(self, node_id: str) -> Optional[ASGNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)
    
    def get_neighbors(self, node_id: str, direction: str = "outgoing") -> List[str]:
        """
        Get neighbors of a node.
        
        Args:
            node_id: The node ID
            direction: 'outgoing' or 'incoming'
        
        Returns:
            List of neighbor node IDs
        """
        if direction == "outgoing":
            return self.adjacency_list.get(node_id, [])
        elif direction == "incoming":
            return self.reverse_adjacency_list.get(node_id, [])
        else:
            raise ValueError("Direction must be 'outgoing' or 'incoming'")
    
    def get_edges_from_node(self, node_id: str, direction: str = "outgoing") -> List[ASGEdge]:
        """
        Get all edges connected to a node.
        
        Args:
            node_id: The node ID
            direction: 'outgoing' or 'incoming'
        
        Returns:
            List of edges
        """
        if direction == "outgoing":
            return [edge for edge in self.edges if edge.source_id == node_id]
        elif direction == "incoming":
            return [edge for edge in self.edges if edge.target_id == node_id]
        else:
            raise ValueError("Direction must be 'outgoing' or 'incoming'")
    
    def validate_graph(self) -> List[str]:
        """
        Validate the graph structure and return list of issues.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        issues = []
        
        # Check for orphaned edges
        for edge in self.edges:
            if edge.source_id not in self.nodes:
                issues.append(f"Edge references non-existent source node: {edge.source_id}")
            if edge.target_id not in self.nodes:
                issues.append(f"Edge references non-existent target node: {edge.target_id}")
        
        # Check for nodes with no connections
        isolated_nodes = [
            node_id for node_id in self.nodes.keys()
            if (not self.adjacency_list.get(node_id) and 
                not self.reverse_adjacency_list.get(node_id))
        ]
        if isolated_nodes:
            issues.append(f"Isolated nodes found: {', '.join(isolated_nodes)}")
        
        return issues

