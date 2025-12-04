"""
Custom exceptions for the ETL Migrator module.
"""


class DSXParserError(Exception):
    """Base exception for DSX parsing errors."""
    pass


class DSXValidationError(DSXParserError):
    """Raised when DSX file validation fails."""
    pass


class DSXParseError(DSXParserError):
    """Raised when DSX file cannot be parsed."""
    pass


class ASGBuildError(DSXParserError):
    """Raised when ASG construction fails."""
    pass


class SchemaValidationError(DSXParserError):
    """Raised when schema validation fails."""
    pass


class StageNotFoundError(DSXParserError):
    """Raised when a referenced stage is not found."""
    pass


class InvalidGraphStructureError(ASGBuildError):
    """Raised when the graph structure is invalid."""
    pass

