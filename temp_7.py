#!/usr/bin/env python3
"""
Enhanced ASG to IR Converter with Complete Transformation Tracking
Converts DataStage ASG (Abstract Syntax Graph) to IR (Intermediate Representation)

ENHANCEMENTS:
✅ Complete transformation tracking from DSX to IR
✅ Preserves TrxGenCode transformation logic
✅ Column-level transformation details
✅ Complexity scoring and classification
✅ Full data lineage tracking
✅ Expression preservation
"""

import json
import re
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import uuid
import os

# Database connection for type mappings
from sqlalchemy import create_engine, text

# Sync DB URL for script execution (converted from async)
SYNC_DB_URL = "postgresql://postgres:Edgematics2025@axoma-dev-postgres.cd4keaaye6mk.eu-west-1.rds.amazonaws.com:5432/axoma-etl-migration-tool"

class IRNodeType(Enum):
    """IR Node Types for Migration"""
    SOURCE = "source"
    SINK = "sink"
    TRANSFORM = "transform"
    LOOKUP = "lookup"
    JOIN = "join"
    MERGE = "merge"
    DEDUPLICATE = "deduplicate"
    AGGREGATE = "aggregate"

class TransformationType(Enum):
    """Transformation Types in IR"""
    SIMPLE_COLUMN = "simple_column"
    CONDITIONAL = "conditional"
    AGGREGATION = "aggregation"
    WINDOW_FUNCTION = "window_function"
    CONSTANT = "constant"
    EXPRESSION = "expression"

@dataclass
class IRASTNode:
    """Abstract Syntax Tree Node for Transformations"""
    node_type: str
    value: str
    children: List['IRASTNode'] = field(default_factory=list)
    operator: Optional[str] = None
    conditions: List['IRASTNode'] = field(default_factory=list)

@dataclass
class IRColumn:
    """IR Column Definition with Full Transformation Tracking"""
    name: str
    data_type: str
    nullable: bool = True
    length: int = 0
    scale: int = 0
    
    # Transformation info
    source_columns: List[str] = field(default_factory=list)
    transformation_type: TransformationType = TransformationType.SIMPLE_COLUMN
    complexity_score: float = 0.0
    expression: str = ""
    ast: Optional[IRASTNode] = None
    
    # 🔧 NEW: Enhanced transformation tracking
    transformation_logic: Dict[str, Any] = field(default_factory=dict)
    has_transformation: bool = False
    transformation_classification: str = "none"
    functions: List[str] = field(default_factory=list)
    
    # Lineage tracking
    source_stage: Optional[str] = None
    lineage_path: List[str] = field(default_factory=list)

@dataclass
class IRNode:
    """IR Node Definition with Enhanced Transformation Tracking"""
    node_id: str
    node_type: IRNodeType
    name: str
    description: str = ""
    
    # Schema
    input_columns: List[IRColumn] = field(default_factory=list)
    output_columns: List[IRColumn] = field(default_factory=list)
    
    # 🔧 NEW: Enhanced business logic tracking
    transformation_logic: Dict[str, Any] = field(default_factory=dict)
    trxgen_code: Optional[str] = None  # 🔧 NEW: Preserve TrxGenCode
    trx_class_name: Optional[str] = None  # 🔧 NEW: Transformation class name
    complexity_score: float = 0.0
    
    # Performance metrics
    processing_time_estimate: float = 0.0
    memory_usage_estimate: int = 0
    
    # Dependencies
    dependencies: List[str] = field(default_factory=list)

@dataclass
class IREdge:
    """IR Edge Definition"""
    from_node: str
    to_node: str
    from_pin: str = ""
    to_pin: str = ""
    join_type: str = "unknown"
    data_flow_type: str = "sequential"

@dataclass
class IRJob:
    """Complete IR Job Definition with Transformation Tracking"""
    job_name: str
    description: str = ""
    
    # Pipeline structure
    nodes: List[IRNode] = field(default_factory=list)
    edges: List[IREdge] = field(default_factory=list)
    
    # Metadata
    total_stages: int = 0
    total_columns: int = 0
    total_transformations: int = 0  # 🔧 NEW: Track transformation count
    complexity_metrics: Dict[str, float] = field(default_factory=dict)
    
    # Schema evolution tracking
    schema_lineage: Dict[str, Any] = field(default_factory=dict)

class EnhancedASGToIRConverter:
    """Enhanced converter with complete transformation tracking"""
    
    def __init__(self):
        self.node_counter = 0
        self.asg_data = None
        self.ir_job = None
        self.node_mappings = {
            'PxChangeCapture': IRNodeType.SOURCE,
            'CTransformerStage': IRNodeType.TRANSFORM,
            'PxPeek': IRNodeType.SOURCE,
            'PxLookup': IRNodeType.LOOKUP,
            'PxJoin': IRNodeType.JOIN,
            'PxFunnel': IRNodeType.MERGE,
            'PxRemDup': IRNodeType.DEDUPLICATE,
            'CCustomStage': IRNodeType.TRANSFORM
        }
        
        # 🔧 FIX: Consistent ID tracking
        self.asg_to_ir_node_id_map = {}  # Maps ASG node IDs to IR node IDs
        self.schema_mappings = {}  # Maps schema references
        self.provenance_map = {}  # Maps ASG node IDs to provenance info
        self.transformation_stats = {  # Track transformation statistics
            'simple_columns': 0,
            'aggregations': 0,
            'conditionals': 0,
            'expressions': 0,
            'constants': 0
        }
        
        # 🔧 NEW: DB-based type mappings cache
        self.db_type_mappings = {}  # component -> (ir_type, ir_subtype)
        # self._load_type_mappings_from_db()
    
    def _load_type_mappings_from_db(self):
        """Load component → (ir_type, ir_subtype) mappings from database."""
        try:
            print("creating engine")
            engine = create_engine(SYNC_DB_URL)
            print("created engine")
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT component, ir_type, ir_subtype
                    FROM ir_property_mappings
                    WHERE ir_type IS NOT NULL AND ir_subtype IS NOT NULL
                """))
                for row in result:
                    component = row[0]
                    if component not in self.db_type_mappings:
                        self.db_type_mappings[component] = (row[1], row[2])
            print(f"✅ Loaded {len(self.db_type_mappings)} type mappings from DB")
        except Exception as e:
            print(f"⚠️ Failed to load type mappings from DB: {e}")
        
    def load_asg(self, asg_file_path: str) -> Dict[str, Any]:
        """Load ASG data from file with improved error handling"""
        try:
            with open(asg_file_path, 'r', encoding='utf-8') as f:
                self.asg_data = json.load(f)
            print(f"✅ Loaded ASG data from: {asg_file_path}")
            return self.asg_data
        except FileNotFoundError:
            print(f"ASG file not found: {asg_file_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in ASG file: {e}")
            return None
        except Exception as e:
            print(f"Error loading ASG file: {e}")
            return None
    
    def convert(self) -> Dict[str, Any]:
        """Main conversion: ASG → IR with complete transformation tracking."""
        print("\n🔄 Starting ASG → IR conversion...")
        
        if not self.asg_data:
            print("❌ No ASG data loaded. Call load_asg() first.")
            return None
        
        # Initialize IR structure
        self.ir_data = {
            "irVersion": "1.0",
            "generatedAt": datetime.now().isoformat() + "Z",
            "job": {
                "id": self._generate_deterministic_id(),
                "name": self.asg_data.get('job_name', 'Unknown_Job')
            },
            "nodes": [],
            "links": [],
            "schemas": {},
            "transformationTracking": {
                "totalTransformations": 0,
                "complexityDistribution": {},
                "transformationTypes": {}
            }
        }
        
        print("  [1/6] Extracting provenance...")
        self._extract_all_provenance()
        
        print("  [2/6] Converting nodes with transformation tracking...")
        self._convert_nodes_with_transformations()
        
        print("  [3/6] Converting edges...")
        self._convert_edges()
        
        print("  [4/6] Building schemas...")
        self._build_schemas()
        
        print("  [5/6] Adding provenance...")
        self._add_provenance()
        
        print("  [6/6] Computing transformation statistics...")
        self._compute_transformation_statistics()
        
        total_transformations = sum(self.transformation_stats.values())
        print(f"Enhanced conversion complete: {len(self.ir_data['nodes'])} nodes, {len(self.ir_data['links'])} links, {total_transformations} transformations")
        return self.ir_data
    
    def _extract_all_provenance(self):
        """Extract provenance information from all ASG nodes"""
        for node in self.asg_data.get('nodes', []):
            node_id = node.get('id', '')
            provenance = node.get('provenance', {})
            
            # Extract provenance with defaults
            self.provenance_map[node_id] = {
                'source': provenance.get('source', 'dsx'),
                'location': provenance.get('location', f"{self.asg_data.get('job_name', 'Unknown')}.dsx"),
                'lineStart': provenance.get('lineStart', '--'),
                'lineEnd': provenance.get('lineEnd', '--'),
                'filePath': provenance.get('filePath', f"{self.asg_data.get('job_name', 'Unknown')}.dsx")
            }
    
    def _convert_nodes_with_transformations(self):
        """Convert ASG nodes to IR nodes with complete transformation tracking."""
        for asg_node in self.asg_data.get('nodes', []):
            try:
                ir_node = self._convert_single_node_with_transformations(asg_node)
                if ir_node:
                    self.ir_data['nodes'].append(ir_node)
            except Exception as e:
                print(f"❌ Error converting node {asg_node.get('id', 'Unknown')}: {e}")
                continue
    
    def _convert_single_node_with_transformations(self, asg_node: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single ASG node to IR node with full transformation data."""
        asg_node_id = asg_node.get('id', '')
        node_name = asg_node.get('name', 'Unknown')
        node_type = asg_node.get('type', 'Unknown')
        enhanced_type = asg_node.get('enhanced_type', node_type)
        
        # Generate consistent IR node ID using enumeration
        ir_node_id = f"n{self.node_counter}"
        self.asg_to_ir_node_id_map[asg_node_id] = ir_node_id
        
        # Map DataStage stage types to IR node types
        ir_type, ir_subtype = self._map_stage_type_to_ir(enhanced_type, asg_node)
        
        ir_node = {
            "id": ir_node_id,
            "type": ir_type,
            "subtype": ir_subtype,
            "name": node_name,
            "props": {},
            "transformationDetails": {
                "hasTransformations": False,
                "transformationType": "none",
                "complexityScore": 0.0,
                "transformationCount": 0
            }
        }
        
        # Map properties based on stage type
        self._map_node_properties(ir_node, asg_node)
        
        # 🔧 ENHANCED: Handle schemas with transformation data
        if asg_node.get('pins'):
            schema_ref = self._create_schema_from_pins_with_transformations(asg_node, ir_node_id, ir_node)
            ir_node['schemaRef'] = schema_ref
        elif self._should_have_schema(asg_node):
            schema_ref = self._create_target_schema(asg_node, ir_node_id)
            ir_node['schemaRef'] = schema_ref
        
        # 🔧 NEW: Preserve TrxGenCode and transformation class
        self._preserve_trxgen_code(ir_node, asg_node)
        
        self.node_counter += 1
        return ir_node
    
    def _preserve_trxgen_code(self, ir_node: Dict[str, Any], asg_node: Dict[str, Any]):
        """🔧 NEW: Preserve TrxGenCode transformation logic"""
        enhanced_props = asg_node.get('enhanced_properties', {})
        apt_props = enhanced_props.get('apt_properties', {})
        
        # Preserve TrxGenCode (located in apt_properties)
        if 'TrxGenCode' in apt_props:
            ir_node['trxgenCode'] = apt_props['TrxGenCode']
        
        # Preserve TrxClassName
        if 'TrxClassName' in apt_props:
            ir_node['trxClassName'] = apt_props['TrxClassName']
        
        # Preserve job parameters if any
        if 'JobParameterNames' in apt_props:
            ir_node['jobParameterNames'] = apt_props['JobParameterNames']
    
    def _create_schema_from_pins_with_transformations(self, asg_node: Dict[str, Any], ir_node_id: str, ir_node: Dict[str, Any]) -> str:
        """🔧 ENHANCED: Create IR schema with full transformation tracking"""
        asg_node_id = asg_node.get('id', '')
        schema_id = f"s_{asg_node_id}"
        
        # Extract columns with transformation data
        schema_columns = []
        has_any_transformations = False
        total_complexity = 0.0
        transformation_count = 0
        
        for pin in asg_node.get('pins', []):
            # 🔧 FIXED: Use enhanced_schema for transformation data, fallback to schema
            columns_to_process = pin.get('enhanced_schema', pin.get('schema', []))
            for column in columns_to_process:
                # Create enhanced IR column with transformation data
                ir_column = self._create_enhanced_ir_column(column, pin.get('name', ''))
                schema_columns.append(ir_column)
                
                # Track transformation statistics
                if ir_column.get('hasTransformation', False):
                    has_any_transformations = True
                    transformation_count += 1
                    
                    # Update transformation type tracking
                    trans_type = ir_column.get('transformationClassification', 'none')
                    column_name = ir_column.get('name', 'unknown')
                    
                    # Map ASG transformation types to our statistics keys
                    type_mapping = {
                        'simple_column': 'simple_columns',
                        'aggregation': 'aggregations',
                        'conditional': 'conditionals',
                        'expression': 'expressions',
                        'constant': 'constants'
                    }
                    
                    stat_key = type_mapping.get(trans_type, trans_type)
                    
                    if stat_key in self.transformation_stats:
                        self.transformation_stats[stat_key] += 1
                    else:
                        # Handle any unmapped transformation types
                        print(f"⚠️  Unknown transformation type: {trans_type} (mapped to {stat_key})")
                
                # Sum complexity scores
                complexity = ir_column.get('complexityScore', 0.0)
                total_complexity += complexity
        
        # Store schema with transformation data
        self.ir_data['schemas'][schema_id] = schema_columns
        self.schema_mappings[asg_node_id] = schema_id
        
        # Update node transformation summary
        if has_any_transformations:
            ir_node['transformationDetails'] = {
                "hasTransformations": True,
                "transformationType": "mixed",  # Could be more specific
                "complexityScore": total_complexity / transformation_count if transformation_count > 0 else 0.0,
                "transformationCount": transformation_count
            }
        
        return schema_id
    
    def _create_enhanced_ir_column(self, column_data: Dict[str, Any], pin_name: str) -> Dict[str, Any]:
        """🔧 NEW: Create IR column with complete transformation data"""
        # Base column data
        ir_column = {
            "name": column_data.get('name', 'unknown'),
            "type": self._map_sql_type_to_ir(column_data.get('type', 'string')),
            "nullable": column_data.get('nullable', True),
            "transformationLogic": None,  # Will be populated if transformation exists
            "transformationClassification": "none",
            "complexityScore": 0.0,
            "sourceColumns": [],
            "expression": ""
        }
        
        # 🔧 FIXED: Check both transformation_logic field and has_transformation flag
        has_transformation = column_data.get('has_transformation', False)
        
        if has_transformation:
            # Get transformation logic from the dedicated field
            transformation_logic = column_data.get('transformation_logic', {})
            
            # Also get classification from the column directly
            classification = column_data.get('transformation_classification', 'simple_column')
            complexity = column_data.get('complexity_score', 0.0)
            
            ir_column.update({
                "transformationLogic": transformation_logic,
                "transformationClassification": classification,
                "complexityScore": complexity,
                "sourceColumns": transformation_logic.get('source_columns', []),
                "expression": transformation_logic.get('expression', ''),
                "functions": transformation_logic.get('functions', []),
                "hasTransformation": True
            })
        else:
            ir_column["hasTransformation"] = False
        
        return ir_column
    
    def _map_stage_type_to_ir(self, enhanced_type: str, asg_node: Dict[str, Any]) -> tuple:
        """Map DataStage stage types to IR node types using DB mappings."""
        
        # Get the actual stage_type from properties (e.g., PxSequentialFile)
        properties = asg_node.get('properties', {})
        stage_type = properties.get('stage_type', enhanced_type)
        
        # PRIORITY 1: Handle PxSequentialFile explicitly (can be Source or Sink)
        if stage_type == 'PxSequentialFile':
            ir_type = self._determine_source_or_sink(asg_node)
            return ir_type, "File"
        
        # PRIORITY 2: Check DB mappings for the actual stage_type
        if stage_type in self.db_type_mappings:
            ir_type, ir_subtype = self.db_type_mappings[stage_type]
            
            # For components that can be Source OR Sink (like PxSequentialFile),
            # determine direction based on pins or node name
            if stage_type == 'PxSequentialFile':
                ir_type = self._determine_source_or_sink(asg_node)
            
            return ir_type, ir_subtype
        
        # PRIORITY 3: Check DB mappings for enhanced_type as fallback
        if enhanced_type in self.db_type_mappings:
            ir_type, ir_subtype = self.db_type_mappings[enhanced_type]
            # Still check if it's a file stage that needs direction detection
            if enhanced_type == 'PxSequentialFile':
                ir_type = self._determine_source_or_sink(asg_node)
            return ir_type, ir_subtype
        
        # PRIORITY 4: Final fallback to legacy heuristics (also checks stage_type)
        return self._legacy_type_mapping(stage_type, asg_node, enhanced_type)
    
    def _determine_source_or_sink(self, asg_node: Dict[str, Any]) -> str:
        """Determine if a node is Source or Sink based on pins direction."""
        pins = asg_node.get('pins', [])
        
        has_input = any(p.get('direction') == 'input' for p in pins)
        has_output = any(p.get('direction') == 'output' for p in pins)
        
        if has_output and not has_input:
            return "Source"
        elif has_input and not has_output:
            return "Sink"
        
        # Fallback to name heuristics
        node_name = asg_node.get('name', '').upper()
        if any(kw in node_name for kw in ['TGT', 'OUT', 'TARGET', 'SINK']):
            return "Sink"
        return "Source"
    
    def _legacy_type_mapping(self, stage_type: str, asg_node: Dict[str, Any], enhanced_type: str = None) -> tuple:
        """Legacy fallback for type mapping when DB mapping not found."""
        
        # Use stage_type first, then enhanced_type as fallback
        type_to_check = stage_type if stage_type else (enhanced_type or '')
        
        # Database connector patterns
        if any(db in type_to_check.upper() for db in ['DB2', 'ORACLE', 'SQL', 'CONNECTOR']):
            return "Source", self._detect_db_type(asg_node)
        
        # File-based stages - PxSequentialFile or any Sequential/File stage
        elif 'Sequential' in type_to_check or (type_to_check == 'PxSequentialFile'):
            # Determine Source or Sink based on pins
            pins = asg_node.get('pins', [])
            has_input = any(p.get('direction') == 'input' for p in pins)
            has_output = any(p.get('direction') == 'output' for p in pins)
            
            if has_output and not has_input:
                return "Source", "File"
            elif has_input and not has_output:
                return "Sink", "File"
            else:
                # Fallback to name heuristics
                node_name = asg_node.get('name', '').upper()
                if any(keyword in node_name for keyword in ['TGT', 'OUT', 'TARGET', 'SINK', 'OUTPUT']):
                    return "Sink", "File"
                else:
                    return "Source", "File"
        
        # Transformation stages
        elif type_to_check in ['CTransformerStage', 'PxJoin', 'PxLookup', 'PxChangeCapture']:
            return "Transform", "Map"
        
        # Custom stages - check if it's actually a file stage based on properties
        elif enhanced_type == 'CCustomStage' or type_to_check == 'CCustomStage':
            # Check if it has file-related properties
            enhanced_props = asg_node.get('enhanced_properties', {})
            config = enhanced_props.get('configuration', {})
            has_file_path = 'FilePath' in config or any('file' in str(k).lower() for k in config.keys())
            
            if has_file_path:
                # Determine Source or Sink based on pins
                pins = asg_node.get('pins', [])
                has_input = any(p.get('direction') == 'input' for p in pins)
                has_output = any(p.get('direction') == 'output' for p in pins)
                
                if has_output and not has_input:
                    return "Source", "File"
                elif has_input and not has_output:
                    return "Sink", "File"
                else:
                    # Fallback to name heuristics
                    node_name = asg_node.get('name', '').upper()
                    if any(keyword in node_name for keyword in ['TGT', 'OUT', 'TARGET', 'SINK', 'OUTPUT']):
                        return "Sink", "File"
                    elif any(keyword in node_name for keyword in ['SRC', 'IN', 'SOURCE', 'INPUT']):
                        return "Source", "File"
            
            # Default for CCustomStage without file properties
            return "Transform", "Custom"
        
        # Default fallback
        else:
            return "Transform", "Generic"
    
    def _detect_db_type(self, asg_node: Dict[str, Any]) -> str:
        """Detect database type from node properties."""
        node_type = asg_node.get('type', '').upper()
        enhanced_props = asg_node.get('enhanced_properties', {})
        
        if 'configuration' in enhanced_props:
            config = enhanced_props['configuration']
            if 'databaseType' in config:
                return config['databaseType']
        
        if 'DB2' in node_type:
            return 'DB2'
        elif 'ORACLE' in node_type:
            return 'Oracle'
        elif 'SQL' in node_type or 'MSSQL' in node_type:
            return 'SQLServer'
        elif 'MYSQL' in node_type:
            return 'MySQL'
        elif 'POSTGRES' in node_type:
            return 'PostgreSQL'
        else:
            return 'GenericDB'
    
    def _map_node_properties(self, ir_node: Dict[str, Any], asg_node: Dict[str, Any]):
        """Map ASG node properties to IR node properties."""
        
        enhanced_props = asg_node.get('enhanced_properties', {})
        
        # File path detection (SequentialFile or generic File)
        if ir_node['subtype'] in ('SequentialFile', 'File'):
            ir_node['props'] = {
                "path": self._extract_file_path(asg_node),
                "delimiter": enhanced_props.get('delimiter', ','),
                "encoding": "UTF-8",
                "firstLineColumnNames": enhanced_props.get('firstLineColumnNames', True)
            }
        
        # Database properties
        elif ir_node['subtype'] in ['DB2', 'Oracle', 'SQLServer', 'MySQL', 'PostgreSQL']:
            ir_node['props'] = {
                "table": self._extract_table_name(asg_node),
                "commit": "1000",
                "schema": self._extract_schema_name(asg_node)
            }
        
        # Transformation properties (for joins, lookups, etc.)
        elif ir_node['subtype'] == 'Map':
            join_props = self._extract_join_properties(asg_node)
            if join_props:
                ir_node['props'].update(join_props)
        
        # Custom stages
        elif ir_node['subtype'] == 'Custom':
            custom_props = {
                "customType": asg_node.get('enhanced_type', 'Unknown'),
                "description": f"DataStage {asg_node.get('enhanced_type', 'Custom')} component"
            }
            
            if enhanced_props:
                custom_props.update(enhanced_props)
            
            ir_node['props'] = custom_props
    
    def _extract_file_path(self, asg_node: Dict[str, Any]) -> str:
        """Extract actual file path from ASG node."""
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        file_path = None
        if config and 'FilePath' in config:
            file_path = config['FilePath']
        elif config and 'file' in config:
            file_path = config['file']

        # Fallback: look for file on pin properties (SequentialFile stores path on pin)
        if not file_path:
            for pin in asg_node.get('pins', []):
                pin_props = pin.get('properties', {})
                if 'file' in pin_props and pin_props['file']:
                    file_path = pin_props['file']
                    break
        
        # Clean up the path: remove "0file" prefix but preserve directory structure
        if file_path:
            # Remove "0file" prefix if present (decoding artifact)
            cleaned_path = file_path.replace('0file/', '').replace('0file\\', '')
            
            # Normalize path separators to forward slashes
            cleaned_path = cleaned_path.replace('\\', '/')
            
            # If it's an absolute Windows path (e.g., D:/ETL_Migrator/inputfile.csv),
            # convert to relative path by removing drive letter and making it relative
            # if re.match(r'^[A-Za-z]:/', cleaned_path):
            #     # Remove drive letter (e.g., "D:/" -> "")
            #     cleaned_path = re.sub(r'^[A-Za-z]:/', '', cleaned_path)
            
            # Return the cleaned path (preserves directory structure)
            return cleaned_path
        
        # Fallback: generate path from node name
        node_name = asg_node.get('name', 'file')
        node_name_lower = node_name.lower()
        
        if 'src' in node_name.lower() or 'source' in node_name.lower() or 'input' in node_name.lower():
            return f"inputfile.csv"  # Use actual filename
        elif any(keyword in node_name.lower() for keyword in ['tgt', 'target', 'out', 'output']):
            return f"outputfile.csv"
        else:
            return f"datafile.csv"
    
    def _extract_table_name(self, asg_node: Dict[str, Any]) -> str:
        """Extract table name from ASG node."""
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        if config and 'table' in config:
            return config['table']
        
        for key, value in enhanced_props.items():
            if 'table' in key.lower() or 'target' in key.lower():
                return str(value)
        
        return asg_node.get('name', 'unknown_table')
    
    def _extract_schema_name(self, asg_node: Dict[str, Any]) -> str:
        """Extract schema name from ASG node."""
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        if config and 'schema' in config:
            return config['schema']
        
        for key, value in enhanced_props.items():
            if 'schema' in key.lower():
                return str(value)
        
        return "dbo"
    
    def _extract_join_properties(self, asg_node: Dict[str, Any]) -> Dict[str, Any]:
        """Extract join properties for transformation nodes."""
        props = {}
        
        if 'join_key' in asg_node.get('properties', {}):
            join_key_info = asg_node['properties']['join_key']
            if isinstance(join_key_info, dict) and 'parsed_keys' in join_key_info:
                props['joinKeys'] = join_key_info['parsed_keys']
        
        if 'operator' in asg_node.get('properties', {}):
            operator = asg_node['properties']['operator']
            props['joinType'] = operator.lower()
        
        enhanced_props = asg_node.get('enhanced_properties', {})
        if 'aggregations' in enhanced_props:
            props['aggregations'] = enhanced_props['aggregations']
        
        if 'keys' in enhanced_props:
            props['keys'] = enhanced_props['keys']
        
        return props
    
    def _create_schema_from_pins(self, asg_node: Dict[str, Any], ir_node_id: str) -> str:
        """Create IR schema from ASG node pins - wrapper for enhanced version"""
        ir_node = {}  # Temporary node for transformation tracking
        return self._create_schema_from_pins_with_transformations(asg_node, ir_node_id, ir_node)
    
    def _map_sql_type_to_ir(self, sql_type: str) -> str:
        """Map SQL types to IR type hints."""
        type_mapping = {
            'VARCHAR': 'string',
            'CHAR': 'string',
            'INTEGER': 'integer',
            'INT': 'integer',
            'BIGINT': 'long',
            'DECIMAL': 'decimal',
            'NUMERIC': 'decimal',
            'FLOAT': 'float',
            'REAL': 'float',
            'DOUBLE': 'double',
            'DATE': 'date',
            'TIME': 'time',
            'TIMESTAMP': 'timestamp',
            'BOOLEAN': 'boolean',
            'BIT': 'boolean'
        }
        
        return type_mapping.get(sql_type.upper(), 'string')
    
    def _should_have_schema(self, asg_node: Dict[str, Any]) -> bool:
        """Check if node should have a schema even without pins"""
        enhanced_type = asg_node.get('enhanced_type', '')
        node_name = asg_node.get('name', '').upper()
        
        return any(keyword in enhanced_type.upper() for keyword in ['TARGET', 'SINK', 'OUTPUT']) or \
               any(keyword in node_name for keyword in ['TGT', 'OUT', 'TARGET', 'SINK'])
    
    def _create_target_schema(self, asg_node: Dict[str, Any], ir_node_id: str) -> str:
        """Create schema for target nodes based on expected output"""
        asg_node_id = asg_node.get('id', '')
        schema_id = f"s_{asg_node_id}"
        
        schema_columns = []
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        if config and 'schema' in config:
            schema_data = config['schema']
            if schema_data and isinstance(schema_data, list):
                for column in schema_data:
                    if isinstance(column, dict):
                        schema_columns.append({
                            "name": column.get('name', 'unknown'),
                            "type": self._map_sql_type_to_ir(column.get('type', 'string')),
                            "nullable": column.get('nullable', True)
                        })
        
        if not schema_columns:
            for pin in asg_node.get('pins', []):
                if pin.get('direction') == 'input':
                    for column in pin.get('schema', []):
                        schema_columns.append({
                            "name": column.get('name', 'unknown'),
                            "type": self._map_sql_type_to_ir(column.get('type', 'string')),
                            "nullable": column.get('nullable', True)
                        })
                    break
        
        self.ir_data['schemas'][schema_id] = schema_columns
        self.schema_mappings[asg_node_id] = schema_id
        
        return schema_id
    
    def _convert_edges(self):
        """Convert ASG edges to IR links."""
        for asg_edge in self.asg_data.get('edges', []):
            try:
                ir_link = self._convert_single_edge(asg_edge)
                if ir_link:
                    self.ir_data['links'].append(ir_link)
            except Exception as e:
                print(f"❌ Error converting edge {asg_edge}: {e}")
                continue
    
    def _convert_single_edge(self, asg_edge: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single ASG edge to IR link."""
        from_node_id = self._get_consistent_ir_node_id(asg_edge.get('from_node', ''))
        to_node_id = self._get_consistent_ir_node_id(asg_edge.get('to_node', ''))
        schema_ref = self._get_consistent_schema_ref(asg_edge)
        
        return {
            "id": f"l{len(self.ir_data['links']) + 1}",
            "from": {
                "nodeId": from_node_id,
                "port": "out"
            },
            "to": {
                "nodeId": to_node_id,
                "port": "in"
            },
            "schemaRef": schema_ref
        }
    
    def _get_consistent_ir_node_id(self, asg_node_id: str) -> str:
        """Get consistent IR node ID for ASG node."""
        if asg_node_id in self.asg_to_ir_node_id_map:
            return self.asg_to_ir_node_id_map[asg_node_id]
        else:
            return f"n{hash(asg_node_id) % 10000}"
    
    def _get_consistent_schema_ref(self, asg_edge: Dict[str, Any]) -> str:
        """Get consistent schema reference for edge."""
        from_node_id = asg_edge.get('from_node', '')
        
        if from_node_id in self.schema_mappings:
            return self.schema_mappings[from_node_id]
        else:
            schema_id = f"s_{from_node_id}"
            if schema_id not in self.ir_data['schemas']:
                self.ir_data['schemas'][schema_id] = []
            return schema_id
    
    def _build_schemas(self):
        """Build comprehensive schemas from all nodes."""
        pass
    
    def _add_provenance(self):
        """Add provenance information to nodes with actual DSX info."""
        job_name = self.ir_data['job']['name']
        
        for node in self.ir_data['nodes']:
            asg_node_id = None
            for asg_id, ir_id in self.asg_to_ir_node_id_map.items():
                if ir_id == node['id']:
                    asg_node_id = asg_id
                    break
            
            if asg_node_id:
                provenance = self.provenance_map.get(asg_node_id, {})
            else:
                provenance = {
                    'source': 'dsx',
                    'location': f"{job_name}.dsx",
                    'lineStart': '--',
                    'lineEnd': '--'
                }
            
            node['provenance'] = {
                "source": provenance.get('source', 'dsx'),
                "location": provenance.get('location', f"{job_name}.dsx"),
                "lineStart": provenance.get('lineStart', '--'),
                "lineEnd": provenance.get('lineEnd', '--'),
                "filePath": provenance.get('filePath', f"{job_name}.dsx")
            }
    
    def _compute_transformation_statistics(self):
        """🔧 NEW: Compute global transformation statistics"""
        total_transformations = sum(self.transformation_stats.values())
        
        self.ir_data['transformationTracking'] = {
            "totalTransformations": total_transformations,
            "transformationTypes": dict(self.transformation_stats),
            "complexityDistribution": {
                "simple": self.transformation_stats['simple_columns'],
                "aggregations": self.transformation_stats['aggregations'],
                "complex": self.transformation_stats['conditionals'] + self.transformation_stats['expressions']
            }
        }
    
    def _generate_deterministic_id(self) -> str:
        """Generate deterministic ID for job to ensure reproducibility"""
        job_name = self.asg_data.get('job_name', 'Unknown_Job')
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        return f"job-{job_name.replace(' ', '_')}-{timestamp}"
    
    def save_ir(self, filepath: str) -> bool:
        """Save IR JSON file."""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.ir_data, f, indent=2, ensure_ascii=False)
            print(f"✅ Enhanced IR saved to: {filepath}")
            return True
        except Exception as e:
            print(f"❌ Error saving IR: {e}")
            return False
    
    def validate_ir(self) -> bool:
        """Validate IR consistency."""
        print("\n🔍 Validating Enhanced IR consistency...")
        
        # Check node ID consistency
        node_ids = {node['id'] for node in self.ir_data['nodes']}
        link_node_ids = set()
        
        for link in self.ir_data['links']:
            link_node_ids.add(link['from']['nodeId'])
            link_node_ids.add(link['to']['nodeId'])
        
        missing_nodes = link_node_ids - node_ids
        if missing_nodes:
            print(f"❌ Link references missing nodes: {missing_nodes}")
            return False
        
        # Check schema consistency
        schema_refs = set(self.ir_data['schemas'].keys())
        link_schema_refs = {link['schemaRef'] for link in self.ir_data['links']}
        missing_schemas = link_schema_refs - schema_refs
        if missing_schemas:
            print(f"❌ Link references missing schemas: {missing_schemas}")
            return False
        
        # Check transformation data integrity
        nodes_with_transformations = sum(1 for node in self.ir_data['nodes'] 
                                       if node.get('transformationDetails', {}).get('hasTransformations', False))
        
        print(f"✅ Enhanced IR validation passed!")
        print(f"   {nodes_with_transformations} nodes have transformations")
        print(f"   {self.ir_data['transformationTracking']['totalTransformations']} total transformations tracked")
        
        return True
    
    def print_summary(self):
        """Print enhanced conversion summary."""
        print(f"\n📊 ENHANCED CONVERSION SUMMARY")
        print(f"=" * 60)
        print(f"Job Name: {self.ir_data['job']['name']}")
        print(f"Job ID: {self.ir_data['job']['id']}")
        print(f"IR Version: {self.ir_data['irVersion']}")
        print(f"Nodes: {len(self.ir_data['nodes'])}")
        print(f"Links: {len(self.ir_data['links'])}")
        print(f"Schemas: {len(self.ir_data['schemas'])}")
        
        # Transformation tracking summary
        trans_tracking = self.ir_data['transformationTracking']
        print(f"\n🔄 TRANSFORMATION TRACKING:")
        print(f"Total Transformations: {trans_tracking['totalTransformations']}")
        print(f"  Simple Columns: {trans_tracking['transformationTypes'].get('simple_columns', 0)}")
        print(f"  Aggregations: {trans_tracking['transformationTypes'].get('aggregations', 0)}")
        print(f"  Expressions: {trans_tracking['transformationTypes'].get('expressions', 0)}")
        print(f"  Conditionals: {trans_tracking['transformationTypes'].get('conditionals', 0)}")
        
        print(f"\nNode Types:")
        type_counts = {}
        for node in self.ir_data['nodes']:
            node_type = f"{node['type']}/{node['subtype']}"
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
        
        for node_type, count in sorted(type_counts.items()):
            print(f"  {node_type}: {count}")
        
        # Show nodes with TrxGenCode
        trxgen_nodes = [node for node in self.ir_data['nodes'] if node.get('trxgenCode')]
        if trxgen_nodes:
            print(f"\n🔧 NODES WITH TRXGEN CODE:")
            for node in trxgen_nodes:
                code_length = len(node.get('trxgenCode', ''))
                print(f"  {node['name']}: {code_length} characters")
        
        print(f"\nNode ID Mappings:")
        for asg_id, ir_id in sorted(self.asg_to_ir_node_id_map.items()):
            print(f"  {asg_id} → {ir_id}")


def main():
    """Main execution."""
    print("🚀 Enhanced ASG to IR Converter with Transformation Tracking")
    print("=" * 70)
    
    converter = EnhancedASGToIRConverter()
    
    asg_file = 'simple_user_job.json'
    if not os.path.exists(asg_file):
        print(f"❌ ASG file not found: {asg_file}")
        return
    
    if converter.load_asg(asg_file):
        ir_data = converter.convert()
        
        if ir_data:
            if converter.validate_ir():
                converter.save_ir('simple_user_job_ir.json')
                converter.print_summary()
            else:
                print("❌ Enhanced IR validation failed - not saving invalid output")
        else:
            print("❌ Enhanced conversion failed")


if __name__ == "__main__":
    main()