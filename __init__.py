"""
ETL Migrator - Production-level DSX file parser and ASG generator.

This package provides tools for parsing DataStage DSX files and converting
them into Abstract Syntax Graphs (ASG) for migration and analysis purposes.
"""

from .parser import DSXParser, parse_dsx_file, collect_tags
from .asg import ASG
from .schemas import (
    Job,
    Stage,
    StageProperties,
    StageType,
    FieldDefinition,
    DataType,
    Transformation,
    Link,
    EdgeType,
    ASGNode,
    ASGEdge,
    AbstractSyntaxGraph
)
from .exceptions import (
    DSXParserError,
    DSXValidationError,
    DSXParseError,
    ASGBuildError,
    SchemaValidationError,
    StageNotFoundError,
    InvalidGraphStructureError
)

__version__ = "0.1.0"

__all__ = [
    # Parser
    "DSXParser",
    "parse_dsx_file",
    "collect_tags",
    
    # ASG
    "ASG",
    
    # Schemas
    "Job",
    "Stage",
    "StageProperties",
    "StageType",
    "FieldDefinition",
    "DataType",
    "Transformation",
    "Link",
    "EdgeType",
    "ASGNode",
    "ASGEdge",
    "AbstractSyntaxGraph",
    
    # Exceptions
    "DSXParserError",
    "DSXValidationError",
    "DSXParseError",
    "ASGBuildError",
    "SchemaValidationError",
    "StageNotFoundError",
    "InvalidGraphStructureError",
]

