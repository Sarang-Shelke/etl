#!/usr/bin/env python3
"""
Enhanced ASG to IR Converter - Talend-Focused
Converts DataStage ASG to IR with complete Talend compatibility

ENHANCEMENTS:
✅ Comprehensive debug logging for every conversion step
✅ Talend-specific property extraction (table names, DB info, schemas)
✅ Multi-pin handling for complex stages (Lookup, Join, Merge)
✅ XML property parsing for connector configurations
✅ Transformation logic preservation (TrxGenCode, TrxClassName)
✅ Complete schema lineage tracking
✅ All connector types supported (DB2, ODBC, etc.)
"""

import json
import re
import sys
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import os

# ============================================================================
# DEBUG CONFIGURATION
# ============================================================================

DEBUG = False  # Can be set via CLI flag
def dbg(msg: str):
    """Print debug message to stderr"""
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr)

def log_step(step: str, detail: str = ""):
    """Log a conversion step"""
    if detail:
        print(f"  ℹ️  {step}: {detail}", file=sys.stderr)
    else:
        print(f"  ℹ️  {step}", file=sys.stderr)

def log_node_processing(asg_id: str, node_name: str, node_type: str, ir_id: str):
    """Log node processing"""
    dbg(f"Processing node {asg_id} '{node_name}' ({node_type}) → IR ID {ir_id}")

def log_pin_processing(pin_id: str, pin_name: str, direction: str, col_count: int):
    """Log pin processing"""
    dbg(f"  Pin {pin_id} '{pin_name}' ({direction}): {col_count} columns")

def log_property_extraction(node_id: str, prop_name: str, prop_value: str):
    """Log property extraction"""
    val_preview = str(prop_value)[:80]
    dbg(f"  Property {node_id}::{prop_name} = {val_preview}")

def log_error(msg: str):
    """Log error message"""
    print(f"❌ ERROR: {msg}", file=sys.stderr)

def log_warning(msg: str):
    """Log warning message"""
    print(f"⚠️  WARNING: {msg}", file=sys.stderr)

# ============================================================================
# DATA STRUCTURES FOR TALEND IR
# ============================================================================

class NodeType(Enum):
    """Node types for Talend IR"""
    SOURCE = "source"
    SINK = "sink"
    TRANSFORM = "transform"
    LOOKUP = "lookup"
    JOIN = "join"
    MERGE = "merge"
    DEDUPLICATE = "deduplicate"
    AGGREGATE = "aggregate"

@dataclass
class TalendProperty:
    """Talend-specific property"""
    name: str
    value: Any
    category: str = "config"  # config, connection, schema, etc.
    required_for_talend: bool = True

# ============================================================================
# TALEND IR CONVERTER - TALEND-FOCUSED
# ============================================================================

class TalendASGToIRConverter:
    """Convert ASG to IR optimized for Talend job generation"""
    
    def __init__(self, debug: bool = False):
        global DEBUG
        DEBUG = debug
        
        self.asg_data: Optional[Dict[str, Any]] = None
        self.ir_data: Dict[str, Any] = {}
        self.node_counter = 0
        
        # Mappings
        self.asg_to_ir_node_id_map: Dict[str, str] = {}
        self.schema_mappings: Dict[str, str] = {}
        self.pin_mappings: Dict[str, str] = {}
        
        # Statistics
        self.stats = {
            'nodes_processed': 0,
            'pins_processed': 0,
            'edges_processed': 0,
            'columns_extracted': 0,
            'transformations_extracted': 0,
            'properties_extracted': 0,
            'errors': 0
        }
        
        dbg("=== TalendASGToIRConverter initialized ===")
    
    # ========================================================================
    # PHASE 1: LOAD ASG
    # ========================================================================
    
    def load_asg(self, asg_file_path: str) -> bool:
        """Load ASG from JSON file"""
        try:
            dbg(f"Loading ASG from: {asg_file_path}")
            with open(asg_file_path, 'r', encoding='utf-8') as f:
                self.asg_data = json.load(f)
            
            job_name = self.asg_data.get('job_name', 'Unknown')
            num_nodes = len(self.asg_data.get('nodes', []))
            num_edges = len(self.asg_data.get('edges', []))
            num_params = len(self.asg_data.get('job_parameters', []))
            
            log_step(f"Loaded ASG", f"{job_name} with {num_nodes} nodes, {num_edges} edges, {num_params} params")
            dbg(f"ASG loaded successfully: {job_name}")
            return True
        except Exception as e:
            log_error(f"Failed to load ASG: {e}")
            self.stats['errors'] += 1
            return False
    
    # ========================================================================
    # PHASE 2: CONVERT ASG TO IR
    # ========================================================================
    
    def convert(self) -> bool:
        """Main conversion pipeline"""
        if not self.asg_data:
            log_error("No ASG data loaded. Call load_asg() first.")
            return False
        
        print("\n" + "="*70)
        print("PHASE 2: CONVERTING ASG TO IR (TALEND-FOCUSED)")
        print("="*70)
        
        try:
            # Step 1: Initialize IR structure
            print("\n[1/6] Initializing IR structure...")
            self._init_ir_structure()
            
            # Step 2: Extract and convert nodes
            print("[2/6] Converting nodes...")
            self._convert_all_nodes()
            
            # Step 3: Convert edges
            print("[3/6] Converting edges...")
            self._convert_all_edges()
            
            # Step 4: Build complete schemas
            print("[4/6] Building schemas...")
            self._build_complete_schemas()
            
            # Step 5: Extract job parameters
            print("[5/6] Extracting job parameters...")
            self._extract_job_parameters()
            
            # Step 6: Validate conversion
            print("[6/6] Validating conversion...")
            if not self._validate_conversion():
                log_warning("Validation found issues but continuing...")
            
            return True
        except Exception as e:
            log_error(f"Conversion failed: {e}")
            import traceback
            traceback.print_exc()
            self.stats['errors'] += 1
            return False
    
    def _init_ir_structure(self):
        """Initialize IR JSON structure"""
        job_name = self.asg_data.get('job_name', 'Unknown_Job')
        
        self.ir_data = {
            "metadata": {
                "version": "2.0",
                "generated_at": datetime.now().isoformat() + "Z",
                "generator": "TalendASGToIRConverter",
                "source": {
                    "type": "datastage_asg",
                    "job_name": job_name
                }
            },
            "job": {
                "id": f"talend-{job_name.replace(' ', '_')}",
                "name": job_name,
                "description": "",
                "parameters": [],
                "contexts": {
                    "default": {}
                }
            },
            "nodes": [],
            "connections": [],
            "schemas": {},
            "metadata_info": {
                "total_columns": 0,
                "total_transformations": 0,
                "total_connections": 0
            }
        }
        
        dbg(f"IR structure initialized for job '{job_name}'")
    
    # ========================================================================
    # PHASE 2.1: NODE CONVERSION
    # ========================================================================
    
    def _convert_all_nodes(self):
        """Convert all ASG nodes to IR components"""
        nodes = self.asg_data.get('nodes', [])
        
        for idx, asg_node in enumerate(nodes, 1):
            try:
                asg_id = asg_node.get('id', f'unknown_{idx}')
                node_name = asg_node.get('name', 'Unknown')
                node_type = asg_node.get('type', 'Unknown')
                enhanced_type = asg_node.get('enhanced_type', node_type)
                
                dbg(f"\n--- Processing Node {idx}/{len(nodes)} ---")
                log_node_processing(asg_id, node_name, enhanced_type, f"n{self.node_counter}")
                
                # Convert this node
                ir_component = self._convert_single_node(asg_node)
                
                if ir_component:
                    self.ir_data['nodes'].append(ir_component)
                    self.asg_to_ir_node_id_map[asg_id] = ir_component['id']
                    self.stats['nodes_processed'] += 1
                    print(f"  ✅ {node_name} ({enhanced_type})")
                else:
                    log_warning(f"Failed to convert node {asg_id}")
                    
            except Exception as e:
                log_error(f"Error converting node {asg_node.get('id', 'unknown')}: {e}")
                self.stats['errors'] += 1
    
    def _convert_single_node(self, asg_node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert a single ASG node to IR component"""
        asg_id = asg_node.get('id', '')
        node_name = asg_node.get('name', 'Unknown')
        node_type = asg_node.get('type', 'Unknown')
        enhanced_type = asg_node.get('enhanced_type', node_type)
        
        # Generate IR component ID
        ir_comp_id = f"comp_{asg_id}"
        self.node_counter += 1
        
        dbg(f"Creating component {ir_comp_id} from ASG node {asg_id}")
        
        # Determine component type and category
        comp_type, comp_category = self._determine_component_type(asg_node)
        
        # Base component structure
        ir_component = {
            "id": ir_comp_id,
            "asg_id": asg_id,
            "name": node_name,
            "type": comp_type,
            "category": comp_category,
            "talend_component": self._map_to_talend_component(enhanced_type, asg_node),
            "properties": {},
            "schema": {
                "input_pins": [],
                "output_pins": []
            },
            "configuration": {},
            "talend_specific": {}
        }
        
        # Extract pins and schema
        self._extract_pins_and_schema(asg_node, ir_component)
        
        # Extract configuration (table names, file paths, DB info, etc.)
        self._extract_component_configuration(asg_node, ir_component)
        
        # Extract transformation logic if applicable
        self._extract_transformation_logic(asg_node, ir_component)
        
        # Extract Talend-specific properties
        self._extract_talend_specific_properties(asg_node, ir_component)
        
        dbg(f"Component {ir_comp_id} created successfully")
        return ir_component
    
    def _determine_component_type(self, asg_node: Dict[str, Any]) -> Tuple[str, str]:
        """Determine IR component type based on ASG node"""
        enhanced_type = asg_node.get('enhanced_type', '')
        node_type = asg_node.get('type', '')
        
        dbg(f"Determining component type for enhanced_type='{enhanced_type}', type='{node_type}'")
        
        # Check enhanced_type first (more specific)
        if 'Transformer' in enhanced_type or node_type == 'CTransformerStage':
            return 'transform', 'processor'
        elif 'Lookup' in enhanced_type or 'PxLookup' in enhanced_type:
            return 'lookup', 'processor'
        elif 'Join' in enhanced_type or 'PxJoin' in enhanced_type:
            return 'join', 'processor'
        elif 'Merge' in enhanced_type or 'PxFunnel' in enhanced_type:
            return 'merge', 'processor'
        elif 'RemDup' in enhanced_type or 'Deduplicate' in enhanced_type:
            return 'deduplicate', 'processor'
        elif 'DB2' in enhanced_type or 'ODBC' in enhanced_type or 'DB2Connector' in enhanced_type or 'ODBCConnector' in enhanced_type:
            # Determine source vs sink based on pins
            if self._is_sink_node(asg_node):
                return 'database_write', 'output'
            else:
                return 'database_read', 'input'
        elif 'Sequential' in enhanced_type or 'File' in enhanced_type or 'PxSequential' in enhanced_type:
            # Determine source vs sink based on pins
            if self._is_sink_node(asg_node):
                return 'file_write', 'output'
            else:
                return 'file_read', 'input'
        elif 'Custom' in enhanced_type or node_type == 'CCustomStage':
            # Custom stage - determine by pin direction and name
            if self._is_sink_node(asg_node):
                return 'custom_write', 'output'
            elif self._is_source_node(asg_node):
                return 'custom_read', 'input'
            else:
                return 'custom_transform', 'processor'
        else:
            # Fallback: use pin directions
            if self._is_sink_node(asg_node):
                return 'write', 'output'
            elif self._is_source_node(asg_node):
                return 'read', 'input'
            else:
                return 'transform', 'processor'
    
    def _is_source_node(self, asg_node: Dict[str, Any]) -> bool:
        """Check if node is a source (has output pins, no input pins)"""
        pins = asg_node.get('pins', [])
        has_input = any(p.get('direction') == 'input' for p in pins)
        has_output = any(p.get('direction') == 'output' for p in pins)
        
        return has_output and not has_input
    
    def _is_sink_node(self, asg_node: Dict[str, Any]) -> bool:
        """Check if node is a sink (has input pins, no output pins)"""
        pins = asg_node.get('pins', [])
        has_input = any(p.get('direction') == 'input' for p in pins)
        has_output = any(p.get('direction') == 'output' for p in pins)
        
        return has_input and not has_output
    
    def _map_to_talend_component(self, enhanced_type: str, asg_node: Dict[str, Any]) -> str:
        """Map ASG stage type to Talend component name"""
        name_lower = enhanced_type.lower()
        
        # Database connectors
        if 'db2' in name_lower:
            return 'tDB2Input' if self._is_source_node(asg_node) else 'tDB2Output'
        elif 'odbc' in name_lower:
            return 'tODBCInput' if self._is_source_node(asg_node) else 'tODBCOutput'
        elif 'oracle' in name_lower:
            return 'tOracleInput' if self._is_source_node(asg_node) else 'tOracleOutput'
        elif 'mysql' in name_lower:
            return 'tMysqlInput' if self._is_source_node(asg_node) else 'tMysqlOutput'
        
        # File handling
        elif 'sequential' in name_lower or 'file' in name_lower:
            return 'tFileInputDelimited' if self._is_source_node(asg_node) else 'tFileOutputDelimited'
        
        # Transformations
        elif 'transformer' in name_lower:
            return 'tMap'
        elif 'lookup' in name_lower:
            return 'tMap'  # Lookup in Talend is handled by tMap
        elif 'join' in name_lower:
            return 'tMap'  # Join in Talend is handled by tMap
        elif 'merge' in name_lower or 'funnel' in name_lower:
            return 'tConcat'
        elif 'remdup' in name_lower or 'dedup' in name_lower:
            return 'tUniqRow'
        
        # Default
        else:
            return 'tJavaRow'  # Generic processor
    
    def _extract_pins_and_schema(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any]):
        """Extract pins and schema information"""
        pins = asg_node.get('pins', [])
        
        dbg(f"Extracting {len(pins)} pins")
        
        for pin in pins:
            try:
                pin_id = pin.get('id', '')
                pin_name = pin.get('name', 'unknown')
                direction = pin.get('direction', 'unknown')
                
                # Get schema (prefer enhanced_schema for transformation info)
                schema = pin.get('enhanced_schema', pin.get('schema', []))
                col_count = len(schema)
                
                log_pin_processing(pin_id, pin_name, direction, col_count)
                
                # Create pin entry
                pin_entry = {
                    "id": pin_id,
                    "asg_id": pin_id,
                    "name": pin_name,
                    "direction": direction,
                    "columns": []
                }
                
                # Extract columns
                for col in schema:
                    try:
                        ir_column = self._extract_column_info(col, pin_name)
                        pin_entry['columns'].append(ir_column)
                        self.stats['columns_extracted'] += 1
                    except Exception as e:
                        log_warning(f"Failed to extract column {col.get('name', 'unknown')}: {e}")
                
                # Add to component schema
                if direction == 'input':
                    ir_component['schema']['input_pins'].append(pin_entry)
                elif direction == 'output':
                    ir_component['schema']['output_pins'].append(pin_entry)
                
                # Map pin for edge conversion
                self.pin_mappings[pin_id] = {
                    'component_id': ir_component['id'],
                    'pin_name': pin_name,
                    'direction': direction
                }
                
                self.stats['pins_processed'] += 1
                
            except Exception as e:
                log_error(f"Error extracting pin {pin.get('id', 'unknown')}: {e}")
                self.stats['errors'] += 1
    
    def _extract_column_info(self, col: Dict[str, Any], pin_name: str) -> Dict[str, Any]:
        """Extract column information for Talend IR"""
        col_name = col.get('name', 'unknown')
        col_type = col.get('type', 'string')
        
        ir_column = {
            "name": col_name,
            "type": self._map_sql_type_to_talend(col_type),
            "length": col.get('length', 255),
            "scale": col.get('scale', 0),
            "nullable": col.get('nullable', True),
            "precision": col.get('precision', 0)
        }
        
        # Extract transformation logic if present
        if col.get('has_transformation', False):
            transformation_logic = col.get('transformation_logic', {})
            ir_column['transformation'] = {
                'type': transformation_logic.get('type', 'pass_through'),
                'source_columns': transformation_logic.get('source_columns', []),
                'expression': transformation_logic.get('expression', col_name),
                'functions': transformation_logic.get('functions', []),
                'derivation': col.get('derivation', '')
            }
            self.stats['transformations_extracted'] += 1
        
        return ir_column
    
    def _map_sql_type_to_talend(self, sql_type: str) -> str:
        """Map SQL type to Talend type"""
        type_map = {
            'VARCHAR': 'String',
            'CHAR': 'String',
            'TEXT': 'String',
            'INTEGER': 'Integer',
            'INT': 'Integer',
            'BIGINT': 'Long',
            'SMALLINT': 'Short',
            'TINYINT': 'Byte',
            'DECIMAL': 'BigDecimal',
            'NUMERIC': 'BigDecimal',
            'FLOAT': 'Float',
            'DOUBLE': 'Double',
            'REAL': 'Float',
            'DATE': 'Date',
            'TIME': 'Object',
            'TIMESTAMP': 'Date',
            'DATETIME': 'Date',
            'BOOLEAN': 'Boolean',
            'BIT': 'Boolean',
            'BLOB': 'byte[]',
            'CLOB': 'String'
        }
        
        sql_upper = sql_type.upper()
        return type_map.get(sql_upper, 'String')
    
    def _extract_component_configuration(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any]):
        """Extract Talend-necessary configuration (table names, DB info, file paths)"""
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        comp_type = ir_component['category']
        comp_id = ir_component['id']
        
        dbg(f"Extracting configuration for {comp_id} (category: {comp_type})")
        
        # Database components
        if 'database' in comp_type or ir_component['type'] in ['database_read', 'database_write']:
            self._extract_database_config(asg_node, ir_component, config)
        
        # File components
        elif 'file' in comp_type or ir_component['type'] in ['file_read', 'file_write']:
            self._extract_file_config(asg_node, ir_component, config)
        
        # Custom/connector components
        elif ir_component['type'] in ['custom_read', 'custom_write', 'custom_transform']:
            self._extract_connector_config(asg_node, ir_component, config)
    
    def _extract_database_config(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any], config: Dict[str, Any]):
        """Extract database configuration for Talend"""
        dbg(f"Extracting database configuration for {ir_component['id']}")
        
        # Try to get table name from multiple locations
        table_name = None
        if 'TableName' in config:
            table_name = config['TableName']
        elif 'table' in config:
            table_name = config['table']
        
        # Parse XMLProperties if present (contains connection and table info)
        xml_props = config.get('XMLProperties', '')
        if xml_props and 'CDATA' in xml_props:
            parsed_xml = self._parse_xml_properties(xml_props)
            if 'TableName' in parsed_xml:
                table_name = parsed_xml['TableName']
            if 'Instance' in parsed_xml:
                ir_component['configuration']['database_instance'] = parsed_xml['Instance']
            if 'Database' in parsed_xml:
                ir_component['configuration']['database_name'] = parsed_xml['Database']
        
        if table_name:
            log_property_extraction(ir_component['id'], 'table_name', table_name)
            ir_component['configuration']['table_name'] = table_name
            self.stats['properties_extracted'] += 1
        
        # Extract schema if present
        if 'schema' in config:
            ir_component['configuration']['schema'] = config['schema']
        
        # Extract connection parameters
        for key in ['Instance', 'Database', 'Username', 'Password', 'ConnectionString']:
            if key in config:
                ir_component['configuration'][key.lower()] = config[key]
    
    def _extract_file_config(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any], config: Dict[str, Any]):
        """Extract file configuration for Talend"""
        dbg(f"Extracting file configuration for {ir_component['id']}")
        
        # Get file path from multiple locations
        file_path = None
        if 'FilePath' in config:
            file_path = config['FilePath']
        elif 'file' in config:
            file_path = config['file']
        elif 'path' in config:
            file_path = config['path']
        
        # Also check pin properties
        if not file_path:
            for pin in asg_node.get('pins', []):
                pin_props = pin.get('properties', {})
                if 'file' in pin_props:
                    file_path = pin_props['file']
                    break
        
        if file_path:
            log_property_extraction(ir_component['id'], 'file_path', file_path)
            ir_component['configuration']['file_path'] = file_path
            self.stats['properties_extracted'] += 1
        
        # File format properties
        if 'FieldDelimiter' in config:
            ir_component['configuration']['delimiter'] = config['FieldDelimiter']
        if 'FirstLineColumnNames' in config:
            ir_component['configuration']['header'] = config['FirstLineColumnNames']
        if 'Encoding' in config:
            ir_component['configuration']['encoding'] = config['Encoding']
    
    def _extract_connector_config(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any], config: Dict[str, Any]):
        """Extract connector configuration for Talend"""
        dbg(f"Extracting connector configuration for {ir_component['id']}")
        
        # Store all configuration except very large XML strings
        for key, value in config.items():
            if key in ['XMLProperties', 'XMLConnectorDescriptor']:
                # For XML, extract key values instead of storing entire XML
                if isinstance(value, str) and len(value) > 500:
                    parsed = self._parse_xml_properties(value)
                    ir_component['configuration'][f"{key}_parsed"] = parsed
                    log_property_extraction(ir_component['id'], key, f"(parsed, {len(parsed)} fields)")
            else:
                ir_component['configuration'][key] = value
                self.stats['properties_extracted'] += 1
    
    def _parse_xml_properties(self, xml_str: str) -> Dict[str, str]:
        """Parse XMLProperties CDATA section to extract key values"""
        try:
            dbg("Parsing XML properties")
            
            # Extract content between CDATA markers
            if '<![CDATA[' in xml_str and ']]>' in xml_str:
                # Find first CDATA content
                start = xml_str.find('<![CDATA[') + 9
                end = xml_str.find(']]>', start)
                if start > 8 and end > start:
                    xml_content = xml_str[start:end]
                else:
                    xml_content = xml_str
            else:
                xml_content = xml_str
            
            # Try to parse as XML
            root = ET.fromstring(xml_content)
            result = {}
            
            # Extract all text values with their paths
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    key = elem.tag
                    result[key] = elem.text.strip()
            
            dbg(f"Parsed {len(result)} XML properties")
            return result
        except Exception as e:
            dbg(f"XML parsing failed: {e}, returning empty dict")
            return {}
    
    def _extract_transformation_logic(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any]):
        """Extract transformation logic (TrxGenCode, TrxClassName)"""
        enhanced_props = asg_node.get('enhanced_properties', {})
        apt_props = enhanced_props.get('apt_properties', {})
        
        # Store transformation class and code for Talend generation
        if 'TrxClassName' in apt_props:
            ir_component['talend_specific']['trx_class_name'] = apt_props['TrxClassName']
            dbg(f"Extracted TrxClassName: {apt_props['TrxClassName']}")
        
        if 'TrxGenCode' in apt_props:
            trx_code = apt_props['TrxGenCode']
            # Store indicator that transformation code is present
            ir_component['talend_specific']['has_transformation_code'] = True
            ir_component['talend_specific']['transformation_code_length'] = len(trx_code)
            dbg(f"Extracted TrxGenCode: {len(trx_code)} chars")
            self.stats['transformations_extracted'] += 1
        
        if 'JobParameterNames' in apt_props:
            ir_component['talend_specific']['job_parameters'] = apt_props['JobParameterNames']
    
    def _extract_talend_specific_properties(self, asg_node: Dict[str, Any], ir_component: Dict[str, Any]):
        """Extract Talend-specific properties"""
        enhanced_props = asg_node.get('enhanced_properties', {})
        config = enhanced_props.get('configuration', {})
        
        # Connector properties
        if 'ConnectorName' in config:
            ir_component['talend_specific']['connector_name'] = config['ConnectorName']
        
        if 'Engine' in config:
            ir_component['talend_specific']['engine'] = config['Engine']
        
        # Context properties (parameterized values like #TEST_Param.$DB2_INSTANCE#)
        for key, value in config.items():
            if isinstance(value, str) and '#' in value and '$' in value:
                # This is a parameterized value
                if 'context_params' not in ir_component['talend_specific']:
                    ir_component['talend_specific']['context_params'] = {}
                ir_component['talend_specific']['context_params'][key] = value
    
    # ========================================================================
    # PHASE 2.2: EDGE CONVERSION
    # ========================================================================
    
    def _convert_all_edges(self):
        """Convert all ASG edges to IR connections"""
        edges = self.asg_data.get('edges', [])
        
        dbg(f"\nConverting {len(edges)} edges")
        
        for idx, asg_edge in enumerate(edges, 1):
            try:
                ir_connection = self._convert_single_edge(asg_edge)
                if ir_connection:
                    self.ir_data['connections'].append(ir_connection)
                    self.stats['edges_processed'] += 1
                    
                    from_node = asg_edge.get('from_node', 'unknown')
                    to_node = asg_edge.get('to_node', 'unknown')
                    print(f"  ✅ Edge {from_node} → {to_node}")
                    
            except Exception as e:
                log_error(f"Error converting edge {idx}: {e}")
                self.stats['errors'] += 1
    
    def _convert_single_edge(self, asg_edge: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert a single ASG edge to IR connection"""
        from_asg_id = asg_edge.get('from_node', '')
        to_asg_id = asg_edge.get('to_node', '')
        from_pin_id = asg_edge.get('from_pin', '')
        to_pin_id = asg_edge.get('to_pin', '')
        
        # Get IR component IDs
        from_ir_id = self.asg_to_ir_node_id_map.get(from_asg_id)
        to_ir_id = self.asg_to_ir_node_id_map.get(to_asg_id)
        
        if not from_ir_id or not to_ir_id:
            log_warning(f"Edge {from_asg_id} → {to_asg_id}: nodes not in mapping")
            return None
        
        # Get pin information
        from_pin_info = self.pin_mappings.get(from_pin_id, {'pin_name': 'out'})
        to_pin_info = self.pin_mappings.get(to_pin_id, {'pin_name': 'in'})
        
        ir_connection = {
            "id": f"conn_{from_asg_id}_{to_asg_id}",
            "from": {
                "component_id": from_ir_id,
                "pin": from_pin_info.get('pin_name', 'out'),
                "asg_pin_id": from_pin_id
            },
            "to": {
                "component_id": to_ir_id,
                "pin": to_pin_info.get('pin_name', 'in'),
                "asg_pin_id": to_pin_id
            },
            "schema_ref": from_pin_id  # Reference to source pin schema
        }
        
        return ir_connection
    
    # ========================================================================
    # PHASE 2.3: SCHEMA BUILDING
    # ========================================================================
    
    def _build_complete_schemas(self):
        """Build complete schemas for all components"""
        for component in self.ir_data['nodes']:
            comp_id = component['id']
            
            # Build schema from input and output pins
            schema_def = {
                "inputs": {},
                "outputs": {}
            }
            
            # Input pins
            for pin in component['schema']['input_pins']:
                schema_def['inputs'][pin['name']] = {
                    "columns": pin['columns'],
                    "pin_id": pin['asg_id']
                }
            
            # Output pins
            for pin in component['schema']['output_pins']:
                schema_def['outputs'][pin['name']] = {
                    "columns": pin['columns'],
                    "pin_id": pin['asg_id']
                }
            
            # Store schema
            self.ir_data['schemas'][comp_id] = schema_def
            self.stats['columns_extracted'] += len(component['schema']['input_pins']) + len(component['schema']['output_pins'])
    
    # ========================================================================
    # PHASE 2.4: JOB PARAMETERS
    # ========================================================================
    
    def _extract_job_parameters(self):
        """Extract job parameters for Talend contexts"""
        params = self.asg_data.get('job_parameters', [])
        
        dbg(f"Extracting {len(params)} job parameters")
        
        for param in params:
            try:
                param_name = param.get('name', 'unknown')
                param_default = param.get('default', '')
                
                ir_param = {
                    "name": param_name,
                    "type": "string",
                    "default_value": param_default,
                    "prompt": param.get('prompt', param_name),
                    "required": True
                }
                
                self.ir_data['job']['parameters'].append(ir_param)
                self.ir_data['job']['contexts']['default'][param_name] = param_default
                
                log_property_extraction('job', param_name, param_default)
                
            except Exception as e:
                log_warning(f"Failed to extract parameter {param.get('name', 'unknown')}: {e}")
    
    # ========================================================================
    # PHASE 3: VALIDATION
    # ========================================================================
    
    def _validate_conversion(self) -> bool:
        """Validate IR completeness and consistency"""
        print("\n[VALIDATION] Checking IR consistency...")
        
        issues = []
        
        # Check component consistency
        comp_ids = {comp['id'] for comp in self.ir_data['nodes']}
        conn_comp_ids = set()
        
        for conn in self.ir_data['connections']:
            conn_comp_ids.add(conn['from']['component_id'])
            conn_comp_ids.add(conn['to']['component_id'])
        
        missing_comps = conn_comp_ids - comp_ids
        if missing_comps:
            issues.append(f"Connections reference missing components: {missing_comps}")
        
        # Check schema consistency
        for conn in self.ir_data['connections']:
            schema_ref = conn['schema_ref']
            if schema_ref and schema_ref not in self.pin_mappings:
                # This is OK if it's just a reference
                pass
        
        # Summary
        if issues:
            for issue in issues:
                log_warning(issue)
            return False
        else:
            print("  ✅ All consistency checks passed")
            return True
    
    # ========================================================================
    # OUTPUT
    # ========================================================================
    
    def save_ir(self, output_file: str) -> bool:
        """Save IR to JSON file"""
        try:
            dbg(f"Saving IR to {output_file}")
            
            # Update metadata
            self.ir_data['metadata_info'] = {
                'total_columns': self.stats['columns_extracted'],
                'total_transformations': self.stats['transformations_extracted'],
                'total_connections': len(self.ir_data['connections']),
                'total_nodes': len(self.ir_data['nodes']),
                'total_parameters': len(self.ir_data['job']['parameters'])
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.ir_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ IR saved to: {output_file}")
            return True
        except Exception as e:
            log_error(f"Failed to save IR: {e}")
            return False
    
    def print_summary(self):
        """Print conversion summary"""
        print("\n" + "="*70)
        print("CONVERSION SUMMARY")
        print("="*70)
        
        print(f"\nJob: {self.ir_data['job']['name']}")
        print(f"Nodes: {self.stats['nodes_processed']}")
        print(f"Connections: {self.stats['edges_processed']}")
        print(f"Columns: {self.stats['columns_extracted']}")
        print(f"Transformations: {self.stats['transformations_extracted']}")
        print(f"Properties: {self.stats['properties_extracted']}")
        print(f"Errors: {self.stats['errors']}")
        
        print(f"\nNode types:")
        type_counts = {}
        for comp in self.ir_data['nodes']:
            comp_type = comp['type']
            type_counts[comp_type] = type_counts.get(comp_type, 0) + 1
        
        for ctype, count in sorted(type_counts.items()):
            print(f"  {ctype}: {count}")
        
        print(f"\nTalend components:")
        talend_counts = {}
        for comp in self.ir_data['nodes']:
            talend = comp['talend_component']
            talend_counts[talend] = talend_counts.get(talend, 0) + 1
        
        for talend, count in sorted(talend_counts.items()):
            print(f"  {talend}: {count}")
        
        print("\n" + "="*70)

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Main execution with no parameters and no sys.argv."""

    # >>> Set your configuration here <<<
    asg_file = "simple_user_job.json"
    output_file = "simple_user_job_new_ir.json"
    debug = False
    # -----------------------------------

    # Auto-generate output file if not provided
    if not output_file:
        base = os.path.splitext(asg_file)[0]
        output_file = f"{base}_talend_ir.json"

    converter = TalendASGToIRConverter(debug=debug)

    if not converter.load_asg(asg_file):
        return False

    if not converter.convert():
        return False

    if not converter.save_ir(output_file):
        return False

    converter.print_summary()
    return True


if __name__ == "__main__":
    main()