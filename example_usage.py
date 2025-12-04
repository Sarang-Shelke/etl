"""
Example usage of the ETL Migrator DSX parser.

This file demonstrates how to use the production-level parser
to convert DSX files into Job and ASG representations.
"""
import logging
from pathlib import Path

from parser import DSXParser, parse_dsx_file
from asg import ASG
from exceptions import DSXParseError, DSXValidationError
from schemas import StageType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def example_basic_parsing():
    """Example: Basic parsing of a DSX file."""
    print("=" * 60)
    print("Example 1: Basic Parsing")
    print("=" * 60)
    
    try:
        # Parse DSX file
        job = parse_dsx_file("datastage_sample 2.dsx", strict_validation=True)
        
        print(f"\nJob Name: {job.job_name}")
        print(f"Description: {job.description}")
        print(f"Number of Stages: {len(job.stages)}")
        print(f"Number of Links: {len(job.links)}")
        
        # Print stage details
        print("\nStages:")
        for stage in job.stages:
            print(f"  - {stage.name} ({stage.stage_type.value})")
            if stage.properties.file_path:
                print(f"    File Path: {stage.properties.file_path}")
            if stage.properties.field_definitions:
                print(f"    Fields: {len(stage.properties.field_definitions)}")
            if stage.properties.transformations:
                print(f"    Transformations: {len(stage.properties.transformations)}")
        
        # Print links
        print("\nLinks:")
        for link in job.links:
            print(f"  - {link.source_stage} -> {link.target_stage}")
        
    except DSXParseError as e:
        logger.error(f"Parse error: {e}")
    except DSXValidationError as e:
        logger.error(f"Validation error: {e}")


def example_asg_creation():
    """Example: Creating and analyzing an ASG."""
    print("\n" + "=" * 60)
    print("Example 2: ASG Creation and Analysis")
    print("=" * 60)
    
    try:
        # Parse and build ASG
        job = parse_dsx_file("datastage_sample 2.dsx", strict_validation=True)
        asg = ASG.from_job(job)
        
        print(f"\nASG contains {len(asg.nodes)} nodes and {len(asg.edges)} edges")
        
        # Get topological order
        try:
            topo_order = asg.get_topological_order()
            print(f"\nTopological Order:")
            for i, node_id in enumerate(topo_order, 1):
                print(f"  {i}. {node_id}")
        except Exception as e:
            logger.warning(f"Could not determine topological order: {e}")
        
        # Get root and leaf nodes
        roots = asg.get_roots()
        leaves = asg.get_leaves()
        print(f"\nRoot nodes: {', '.join(roots)}")
        print(f"Leaf nodes: {', '.join(leaves)}")
        
        # Check for cycles
        cycles = asg.detect_cycles()
        if cycles:
            print(f"\nCycles detected: {len(cycles)}")
            for cycle in cycles:
                print(f"  Cycle: {' -> '.join(cycle)}")
        else:
            print("\nNo cycles detected")
        
        # Get node levels
        levels = asg.get_levels()
        print("\nNode Levels:")
        for node_id, level in sorted(levels.items()):
            print(f"  {node_id}: Level {level}")
        
        # Validate graph
        issues = asg.validate_graph()
        if issues:
            print("\nGraph Validation Issues:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("\nGraph validation passed")
        
    except DSXParseError as e:
        logger.error(f"Parse error: {e}")
    except DSXValidationError as e:
        logger.error(f"Validation error: {e}")


def example_detailed_inspection():
    """Example: Detailed inspection of job components."""
    print("\n" + "=" * 60)
    print("Example 3: Detailed Inspection")
    print("=" * 60)
    
    try:
        job = parse_dsx_file("datastage_sample 2.dsx", strict_validation=True)
        
        # Inspect each stage in detail
        for stage in job.stages:
            print(f"\n{'=' * 60}")
            print(f"Stage: {stage.name}")
            print(f"Type: {stage.stage_type.value}")
            print(f"{'=' * 60}")
            
            # Field definitions
            if stage.properties.field_definitions:
                print("\nField Definitions:")
                for field_def in stage.properties.field_definitions:
                    print(f"  - {field_def.name}: {field_def.data_type.value}", end="")
                    if field_def.length:
                        print(f"({field_def.length})", end="")
                    print()
            
            # Input/Output fields
            if stage.properties.input_fields:
                print(f"\nInput Fields: {', '.join(stage.properties.input_fields)}")
            if stage.properties.output_fields:
                print(f"Output Fields: {', '.join(stage.properties.output_fields)}")
            
            # Transformations
            if stage.properties.transformations:
                print("\nTransformations:")
                for trans in stage.properties.transformations:
                    print(f"  {trans.output_field} = {trans.expression}")
            
            # Connections
            incoming = job.get_incoming_links(stage.name)
            outgoing = job.get_outgoing_links(stage.name)
            
            if incoming:
                print(f"\nIncoming Links:")
                for link in incoming:
                    print(f"  <- {link.source_stage}")
            
            if outgoing:
                print(f"\nOutgoing Links:")
                for link in outgoing:
                    print(f"  -> {link.target_stage}")
        
    except DSXParseError as e:
        logger.error(f"Parse error: {e}")
    except DSXValidationError as e:
        logger.error(f"Validation error: {e}")


def example_parser_class():
    """Example: Using the DSXParser class directly."""
    print("\n" + "=" * 60)
    print("Example 4: Using DSXParser Class")
    print("=" * 60)
    
    parser = DSXParser(strict_validation=False)  # Non-strict mode
    
    try:
        job = parser.parse("datastage_sample 2.dsx")
        
        print(f"\nParsed job: {job.job_name}")
        print(f"Metadata: {job.metadata}")
        
        # Query stages by type
        transformer_stages = job.get_stages_by_type(StageType.TRANSFORMER)
        print(f"\nTransformer stages: {[s.name for s in transformer_stages]}")
        
        sequential_stages = job.get_stages_by_type(StageType.SEQUENTIAL_FILE)
        print(f"Sequential file stages: {[s.name for s in sequential_stages]}")
        
    except DSXParseError as e:
        logger.error(f"Parse error: {e}")


if __name__ == "__main__":
    # Run all examples
    example_basic_parsing()
    example_asg_creation()
    example_detailed_inspection()
    example_parser_class()

