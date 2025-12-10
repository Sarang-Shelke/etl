import re
import json
import sys
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict

# Add progress output
def progress(msg: str):
    print(msg, file=sys.stderr, flush=True)

# ============================================================================
# CONFIGURATION - Data structures and constants
# ============================================================================

@dataclass
class SQLType:
    code: str
    name: str
    
SQL_TYPES = [
    SQLType("1", "CHAR"),
    SQLType("3", "DECIMAL"), 
    SQLType("4", "INTEGER"),
    SQLType("5", "SMALLINT"),
    SQLType("6", "FLOAT"),
    SQLType("7", "REAL"),
    SQLType("8", "DOUBLE"),
    SQLType("9", "DATE"),
    SQLType("10", "TIME"),
    SQLType("11", "TIMESTAMP"),
    SQLType("12", "VARCHAR"),
    SQLType("-1", "LONGVARCHAR"),
    SQLType("-2", "BINARY"),
    SQLType("-3", "VARBINARY"), 
    SQLType("-4", "LONGVARBINARY"),
    SQLType("-5", "BIGINT"),
    SQLType("-6", "TINYINT"),
    SQLType("-7", "BIT"),
    SQLType("-8", "NCHAR"),
    SQLType("-9", "NVARCHAR"),
    SQLType("-10", "LONGNVARCHAR")
]

# Create lookup map
SQL_TYPE_MAP = {st.code: st.name for st in SQL_TYPES}

def map_sql_type(sql_type_code: str) -> str:
    """Map DataStage SqlType code to readable type name."""
    return SQL_TYPE_MAP.get(sql_type_code, f"UNKNOWN({sql_type_code})")

def decode_dsx_value(value: Any) -> Any:
    """
    Decode DataStage-escaped values such as '\\(2)path\\(2)0' that wrap
    actual strings in control markers. Keeps non-string values as-is.
    
    Example: '\\(2)\\(2)0\\(1)\\(3)file\\(2)/D:\\ETL_Migrator\\inputfile.csv\\(2)0'
    Should decode to: 'D:\\ETL_Migrator\\inputfile.csv'
    """
    if not isinstance(value, str):
        return value
    
    # Remove control markers like \(2) or \(1)
    decoded = re.sub(r'\\\(\d\)', '', value)
    # Unescape backslashes
    decoded = decoded.replace('\\\\', '\\')
    
    # For file paths, look for patterns like "0file/path" or "0file\\path" and extract just the path
    # The "0file" prefix is a DataStage artifact that should be removed
    if 'file' in decoded.lower() and ('/' in decoded or '\\' in decoded):
        # Try to find the actual path after "file" marker
        # Pattern: ...file/path... or ...file\\path...
        file_match = re.search(r'file[/\\](.+)', decoded, re.IGNORECASE)
        if file_match:
            decoded = file_match.group(1)
        else:
            # Fallback: remove "0file" prefix if present
            decoded = re.sub(r'^0+file[/\\]?', '', decoded, flags=re.IGNORECASE)
    
    # Trim a trailing sentinel "0" that often follows the control markers
    if decoded.endswith('0') and ('/' in decoded or '\\' in decoded):
        decoded = decoded[:-1]
    
    return decoded.strip()


# Debug logging helper
DEBUG = False
def dbg(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr, flush=True)

# ============================================================================
# STAGE PROPERTIES TO PRESERVE
# ============================================================================

# Job-level properties to skip
ROOT_OMIT = {
    'TraceMode', 'TraceSeq', 'TraceRecords', 'TraceSkip', 'TracePeriod',
    'RecordJobPerformanceData', 'GridNodes', 'IdentList',
    'ValidationStatus', 'Uploadable', 'RelStagesInJobStatus',
    'WebServiceEnabled', 'PgmCustomizationFlag', 'JobReportFlag',
    'AllowMultipleInvocations', 'Act2ActOverideDefaults', 'Act2ActEnableRowBuffer',
    'Act2ActUseIPC', 'Act2ActBufferSize', 'Act2ActIPCTimeout',
    'ExpressionSemanticCheckFlag', 'TraceOption', 'EnableCacheSharing',
    'RuntimeColumnPropagation', 'MFProcessMetaData', 'MFProcessMetaDataXMLFileExchangeMethod'
}

# Stage APT properties to skip
APT_PROPERTIES_SKIP = {
    'DiskWriteInc', 'BufFreeRun', 'MaxMemBufSize', 'QueueUpperSize',
    'RTColumnProp', 'SchemaFormat'
}

# APT properties to PRESERVE (important for transformers)
APT_PROPERTIES_PRESERVE = {
    'TrxGenCode', 'TrxClassName', 'JobParameterNames'
}

# Properties to extract as metadata
METADATA_PROPERTIES = {
    'TrxGenCode', 'TrxClassName', 'TrxGenCache', 'E2Assembly'
}

# ============================================================================
# ENHANCED STAGE TYPE DETECTION
# ============================================================================

def detect_complex_stage_type(stage_type: str, ole_type: str, properties: List[str]) -> str:
    """
    Enhanced stage type detection for complex stage types.
    """
    stage_type_upper = stage_type.upper() if stage_type else ""
    ole_type_upper = ole_type.upper() if ole_type else ""
    properties_str = " ".join(properties).upper()
    
    # Complex stage type mappings - check stage_type FIRST before ole_type
    if stage_type and "PxSequentialFile".lower() in stage_type.lower():
        return "PxSequentialFile"
    elif stage_type and "PxChangeCapture".lower() in stage_type.lower():
        return "PxChangeCapture"
    elif stage_type and "DB2ConnectorPX".lower() in stage_type.lower():
        return "DB2ConnectorPX"
    elif stage_type and "PxJoin".lower() in stage_type.lower():
        return "PxJoin"
    elif stage_type and "PxLookup".lower() in stage_type.lower():
        return "PxLookup"
    elif stage_type and "PxTransformer".lower() in stage_type.lower():
        return "PxTransformer"
    elif ole_type_upper == "CTRANSFORMERSTAGE":
        return "CTransformerStage"
    elif ole_type_upper == "CCUSTOMOUTPUT":
        return "CCustomOutput"
    elif ole_type_upper == "CCUSTOMINPUT":
        return "CCustomInput"
    elif ole_type_upper == "CCUSTOMSTAGE" and "TRANSACTION" in properties_str:
        return "TransactionalCustomStage"
    elif ole_type_upper == "CCUSTOMSTAGE":
        return "CCustomStage"
    else:
        return stage_type if stage_type else ole_type

# ============================================================================
# CORE PARSING FUNCTIONS
# ============================================================================

def get_lines(file_path: str) -> List[str]:
    """Read file and return as list of lines."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read().splitlines()
    except Exception as e:
        raise FileNotFoundError(f"Cannot read file {file_path}: {e}")

def get_section_details(content: List[str]) -> List[str]:
    """Extract a complete BEGIN/END section block."""
    if not content:
        return []
    try:
        # Determine section token after BEGIN
        first = content[0].strip()
        parts = first.split()
        if len(parts) < 2:
            return []
        section = parts[1]
        target = f'END {section}'

        # Walk lines until we find the matching END <SECTION>
        for i, line in enumerate(content):
            if line is None:
                continue
            if line.strip().upper().startswith(target.upper()):
                return content[:i+1]
    except Exception:
        pass

    return []

def parse_heredoc_value(content: List[str], start_idx: int) -> Tuple[str, int]:
    """Parse heredoc values with proper handling."""
    if start_idx >= len(content):
        return '', start_idx
    line = content[start_idx]

    delim = '=+=+=='
    # If this line contains the heredoc delimiter
    if delim in line:
        # Content after the first delimiter
        after = line.split(delim, 1)[1]
        # If nothing after the delimiter, the heredoc starts on next line
        if after.strip() == '':
            i = start_idx + 1
            collected = []
            while i < len(content):
                cur = content[i]
                if cur is None:
                    i += 1
                    continue
                if cur.strip().startswith(delim):
                    return '\n'.join(collected).rstrip(), i + 1
                collected.append(content[i])
                i += 1
            return '\n'.join(collected).rstrip(), len(content)
        else:
            # There is content on the same line after the opening delimiter
            # Possibly includes the closing delimiter on the same line
            if delim in after:
                inner = after.split(delim, 1)[0]
                return inner.rstrip(), start_idx + 1
            return after.rstrip(), start_idx + 1

    # Not a heredoc - try to extract inline quoted value
    m = re.search(r'Value "(.*?)"', line)
    if m:
        return m.group(1), start_idx + 1

    # Raw line value (return line as-is)
    return line.strip(), start_idx + 1

def parse_complex_sql_heredoc(value_str: str) -> Dict[str, Any]:
    """Parse complex SQL/XML heredoc values."""
    result = {
        'content': '',
        'sql_statements': [],
        'parameters': [],
        'xml_structure': None,
        'content_type': 'unknown'
    }
    
    # Clean the heredoc content
    if value_str and value_str.strip().startswith('=+=+=+=') and value_str.strip().endswith('=+=+=+='):
        content = value_str.strip()[8:-8]  # Remove =+=+=+= delimiters
    else:
        content = value_str
    
    result['content'] = content
    
    # Extract parameter references
    param_matches = re.findall(r'#([^#]*?)\.\$([^#]*?)#', content)
    result['parameters'] = [f"{match[0]}.${match[1]}" for match in param_matches]
    
    # Detect content type and extract SQL statements
    if content.strip().upper().startswith('SELECT') or \
       content.strip().upper().startswith('INSERT') or \
       content.strip().upper().startswith('UPDATE') or \
       content.strip().upper().startswith('DELETE') or \
       ('FROM' in content.upper() and 'WHERE' in content.upper()):
        result['content_type'] = 'sql'
        
        # Extract individual SQL statements
        sql_statements = []
        current_statement = ""
        in_string = False
        string_char = None
        
        for char in content:
            if char in ['"', "'"] and not in_string:
                in_string = True
                string_char = char
                current_statement += char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current_statement += char
            elif char == ';' and not in_string:
                if current_statement.strip():
                    sql_statements.append(current_statement.strip())
                current_statement = ""
            else:
                current_statement += char
        
        # Add the last statement if exists
        if current_statement.strip():
            sql_statements.append(current_statement.strip())
            
        result['sql_statements'] = sql_statements
    
    # Detect and parse XML structure
    elif content.strip().startswith('<?xml') or content.strip().startswith('<'):
        result['content_type'] = 'xml'
        try:
            root = ET.fromstring(content)
            result['xml_structure'] = _extract_xml_structure(root)
        except ET.ParseError:
            result['content_type'] = 'mixed'
        except Exception:
            result['content_type'] = 'mixed'
    
    # Mixed content
    elif '<' in content and ('SELECT' in content.upper() or 'FROM' in content.upper()):
        result['content_type'] = 'mixed'
        
        # Try to extract SQL parts
        sql_parts = re.findall(r'<!\[CDATA\[(.*?)\]\]>', content, re.DOTALL)
        if sql_parts:
            result['sql_statements'] = sql_parts
    
    return result

def _extract_xml_structure(element, path: str = "") -> Dict[str, Any]:
    """Extract XML structure for analysis."""
    structure = {
        'tag': element.tag,
        'path': path,
        'attributes': element.attrib,
        'text': element.text.strip() if element.text else None,
        'children': []
    }
    
    for child in element:
        child_path = f"{path}/{child.tag}" if path else f"/{child.tag}"
        structure['children'].append(_extract_xml_structure(child, child_path))
    
    return structure

def should_omit_property(property_name: str, context: str = 'stage') -> bool:
    """Check if a property should be omitted based on context."""
    if context == 'root':
        return property_name in ROOT_OMIT
    elif context == 'stage':
        return property_name in APT_PROPERTIES_SKIP
    return False

def is_apt_property(sub_record: List[str]) -> bool:
    """Check if a subrecord is an APT-owned property."""
    try:
        return any('Owner "APT"' in (line or '') for line in sub_record)
    except Exception:
        return False

def extract_property_name(line: str) -> Optional[str]:
    """Extract property name from a Name line."""
    match = re.search(r'Name "(.*?)"', line)
    if match:
        return match.group(1)
    return None

# ============================================================================
# ENHANCED TRANSFORMATION LOGIC CLASSIFICATION
# ============================================================================

def classify_derivation_type(derivation: str) -> str:
    """Classify derivation type for transformation logic preservation."""
    if not derivation:
        return 'empty'
    
    derivation_upper = derivation.upper()
    
    # Window functions
    window_functions = ['DENSE_RANK', 'ROW_NUMBER', 'RANK', 'LEAD', 'LAG', 
                       'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'CUME_DIST',
                       'PERCENT_RANK', 'NTILE']
    
    for func in window_functions:
        if func in derivation_upper:
            return 'window_function'
    
    # Conditional logic
    if any(keyword in derivation_upper for keyword in ['CASE WHEN', 'IF', 'IIF', 'DECODE']):
        return 'conditional'
    
    # String operations
    if any(keyword in derivation_upper for keyword in ['SUBSTR', 'SUBSTRING', 'LTRIM', 'RTRIM', 'TRIM', 'REPLACE']):
        return 'string_operation'
    
    # Date operations
    if any(keyword in derivation_upper for keyword in ['TO_DATE', 'TO_CHAR', 'CURRENT_DATE', 'CURRENT_TIMESTAMP']):
        return 'date_operation'
    
    # Aggregation
    if any(keyword in derivation_upper for keyword in ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN']):
        return 'aggregation'
    
    # Arithmetic
    if any(operator in derivation_upper for operator in [' + ', ' - ', ' * ', ' / ']):
        return 'arithmetic'
    
    # Simple column reference
    if re.match(r'^[A-Za-z_][A-Za-z0-9_.]*$', derivation):
        return 'simple_column'
    
    # Complex expressions
    if len(derivation) > 100 or derivation.count('(') > 5:
        return 'complex'
    
    return 'simple'

def extract_transformation_logic(derivation: str) -> Dict[str, Any]:
    """Extract detailed transformation logic from derivation."""
    if not derivation:
        return {
            'type': 'empty',
            'source_columns': [],
            'functions': [],
        }
    
    # Basic classification
    derivation_type = classify_derivation_type(derivation)
    
    # Extract source columns
    source_columns = []
    
    # Match column references
    column_patterns = [
        r'\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\b',  # LINK.COLUMN
        r'\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()'  # COLUMN (not followed by parentheses)
    ]
    
    for pattern in column_patterns:
        matches = re.findall(pattern, derivation)
        source_columns.extend(matches)
    
    # Remove duplicates and filter out SQL keywords
    sql_keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'}
    source_columns = list(set([col for col in source_columns if col.upper() not in sql_keywords]))
    
    # Extract function calls
    functions = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\s*\(', derivation)

    
    return {
        'type': derivation_type,
        'source_columns': source_columns,
        'functions': list(set(functions)),
        'expression': derivation
    }

# ============================================================================
# ENHANCED JOIN KEY PARSING
# ============================================================================

def parse_join_keys(join_key_spec: str) -> Dict[str, Any]:
    """Parse complex join key specifications."""
    if not join_key_spec:
        return {'raw': '', 'parsed_keys': [], 'join_type': 'unknown'}
    
    result = {
        'raw': join_key_spec,
        'parsed_keys': [],
        'join_type': 'unknown',
        'parsed_expression': ''
    }
    
    try:
        # Decode the escaped format
        decoded = join_key_spec
        decoded = decoded.replace('\\(', '(')  # \( -> (
        decoded = decoded.replace('\\)', ')')  # \) -> )
        decoded = decoded.replace('\\0', '')   # \0 -> (empty)
        
        # Look for key specification pattern
        if 'key' in decoded.lower():
            # Extract key fields
            key_match = re.search(r'key\)(.*)', decoded, re.IGNORECASE)
            if key_match:
                key_part = key_match.group(1)
                # Split by field delimiters
                fields = [f.strip() for f in key_part.split(')(') if f.strip()]
                result['parsed_keys'] = fields
                
                # Determine join type
                if 'leftouterjoin' in join_key_spec.lower():
                    result['join_type'] = 'left_outer'
                elif 'fullouterjoin' in join_key_spec.lower():
                    result['join_type'] = 'full_outer'
                elif 'innerjoin' in join_key_spec.lower():
                    result['join_type'] = 'inner'
                
                # Build readable expression
                if fields:
                    result['parsed_expression'] = ' AND '.join([f"source.{field} = lookup.{field}" for field in fields])
        
        result['decoded'] = decoded
        
    except Exception as e:
        result['error'] = str(e)
    
    return result

# ============================================================================
# ENHANCED SUBRECORD PARSING
# ============================================================================

def get_sub_records(content: List[str], context: str = 'stage') -> List[List[str]]:
    """Extract all DSSUBRECORD blocks from content with proper filtering."""
    sub_records = []
    i = 0
    
    while i < len(content):
        line = content[i].strip()
        
        if line.startswith("BEGIN DSSUBRECORD"):
            sub_record = get_section_details(content[i:])
            
            if not sub_record:
                i += 1
                continue
            
            # Skip APT-owned properties (except ones we want to preserve)
            if is_apt_property(sub_record):
                # Check if this is a property we should preserve
                property_name = None
                property_value = None
                for j, sr_line in enumerate(sub_record):
                    if 'Name "' in sr_line:
                        property_name = extract_property_name(sr_line)
                    elif 'Value =+=+=+=' in sr_line:
                        # Extract heredoc value using proper multi-line parsing
                        value_content, next_idx = parse_heredoc_value(sub_record, j)
                        property_value = value_content
                        break
                    elif 'Value "' in sr_line:
                        match = re.search(r'Value "(.*?)"', sr_line)
                        if match:
                            property_value = match.group(1)
                        break
                
                # Only skip if it's not in the preserve list
                if property_name not in APT_PROPERTIES_PRESERVE:
                    i += len(sub_record)
                    continue
            
            # Check if we should omit based on property name
            property_name = None
            for sr_line in sub_record:
                if 'Name "' in sr_line:
                    property_name = extract_property_name(sr_line)
                    break
            
            if property_name and should_omit_property(property_name, context):
                i += len(sub_record)
                continue
            
            # Process heredoc values in the subrecord
            sub_record = process_subrecord_values(sub_record)
            
            sub_records.append(sub_record)
            i += len(sub_record)
        else:
            i += 1
    
    return sub_records

def process_subrecord_values(sub_record: List[str]) -> List[str]:
    """Process a subrecord to handle heredoc values."""
    processed = []
    i = 0
    
    while i < len(sub_record):
        line = sub_record[i]
        # If this is a Value line with heredoc, delegate to parse_heredoc_value
        if line and 'Value' in line and '=+=+=+=' in line:
            val, next_idx = parse_heredoc_value(sub_record, i)
            # store as a normalized inline Value
            processed.append(f'Value "{val}"')
            i = next_idx
        else:
            processed.append(line)
            i += 1
    
    return processed

# ============================================================================
# ENHANCED RECORD PARSING
# ============================================================================

def get_records(content: List[str]) -> List[Dict[str, Any]]:
    """Extract all DSRECORD blocks from content."""
    records = []
    
    for i, line in enumerate(content):
        if line.startswith("BEGIN DSRECORD"):
            record = get_section_details(content[i:])
            
            if not record:
                continue
            
            # Determine context for property filtering
            context = 'stage'
            identifier = None
            
            for rec_line in record:
                if 'Identifier "ROOT"' in rec_line:
                    context = 'root'
                    identifier = 'ROOT'
                    break
                elif 'Identifier "V0"' in rec_line:
                    context = 'view'
                    identifier = 'V0'
                    break
                elif 'Identifier "' in rec_line:
                    match = re.search(r'Identifier "(.*?)"', rec_line)
                    if match:
                        identifier = match.group(1)
                    break
            
            # Filter subrecords
            filtered_record = []
            j = 0
            
            while j < len(record):
                if record[j].strip().startswith("BEGIN DSSUBRECORD"):
                    sub_record = get_section_details(record[j:])
                    
                    if not sub_record:
                        filtered_record.append(record[j])
                        j += 1
                        continue
                    
                    # Skip APT properties (except ones we preserve)
                    if is_apt_property(sub_record):
                        property_name = None
                        for sr_line in sub_record:
                            if 'Name "' in sr_line:
                                property_name = extract_property_name(sr_line)
                                break
                        
                        if property_name not in APT_PROPERTIES_PRESERVE:
                            j += len(sub_record)
                            continue
                    
                    # Check if we should omit
                    property_name = None
                    for sr_line in sub_record:
                        if 'Name "' in sr_line:
                            property_name = extract_property_name(sr_line)
                            
                            if property_name == "FirstLineColumnNames":
                                 # Optimization or explicit handling if needed
                                 pass
                                 
                            break
                    
                    if property_name and should_omit_property(property_name, context):
                        j += len(sub_record)
                        continue
                    
                    # Process heredoc values and add to filtered record
                    processed_sub = process_subrecord_values(sub_record)
                    filtered_record.extend(processed_sub)
                    j += len(sub_record)
                else:
                    filtered_record.append(record[j])
                    j += 1
            
            # Store record with metadata
            record_data = {
                'identifier': identifier,
                'context': context,
                'lines': filtered_record
            }
            records.append(record_data)
    
    return records

# ============================================================================
# JOB-LEVEL FUNCTIONS
# ============================================================================

def get_job(content: List[str]) -> List[str]:
    """Extract the DSJOB section from file content."""
    content = [line.strip() for line in content]
    
    for i, line in enumerate(content):
        if line.startswith("BEGIN DSJOB"):
            job = get_section_details(content[i:])
            return job
    
    return []

def get_sections(content: List[str]) -> List[Dict[str, Any]]:
    """Parse all records from job content."""
    content = [line.strip() for line in content]
    return get_records(content=content)

# ============================================================================
# SCHEMA LINEAGE TRACKER
# ============================================================================

class SchemaLineageTracker:
    """Track schema evolution across pipeline stages."""
    
    def __init__(self):
        self.lineage = {}
        self.schema_evolution = []
    
    def track_stage_schema(self, node_id: str, input_schema: List[Dict], 
                          output_schema: List[Dict], transformations: List[Dict]):
        """Track schema changes for a stage."""
        evolution_event = {
            'node_id': node_id,
            'input_columns': len(input_schema),
            'output_columns': len(output_schema),
            'transformations': transformations,
            'schema_changes': self._compare_schemas(input_schema, output_schema)
        }
        
        self.schema_evolution.append(evolution_event)
        self.lineage[node_id] = {
            'input_schema': input_schema,
            'output_schema': output_schema,
            'transformations': transformations
        }
    
    def _compare_schemas(self, input_schema: List[Dict], output_schema: List[Dict]) -> Dict[str, List[str]]:
        """Compare input and output schemas to identify changes."""
        input_cols = {col['name']: col for col in input_schema}
        output_cols = {col['name']: col for col in output_schema}
        
        added_columns = list(set(output_cols.keys()) - set(input_cols.keys()))
        removed_columns = list(set(input_cols.keys()) - set(output_cols.keys()))
        modified_columns = []
        
        for col_name in set(input_cols.keys()) & set(output_cols.keys()):
            if input_cols[col_name] != output_cols[col_name]:
                modified_columns.append(col_name)
        
        return {
            'added': added_columns,
            'removed': removed_columns,
            'modified': modified_columns
        }
    
    def get_evolution_summary(self) -> Dict[str, Any]:
        """Get summary of schema evolution across pipeline."""
        if not self.schema_evolution:
            return {}
        
        total_evolution = {
            'stages_analyzed': len(self.schema_evolution),
            'total_input_columns': sum(e['input_columns'] for e in self.schema_evolution),
            'total_output_columns': sum(e['output_columns'] for e in self.schema_evolution),
            'net_column_change': sum(e['output_columns'] - e['input_columns'] for e in self.schema_evolution),
            'stages_with_enrichment': len([e for e in self.schema_evolution if e['output_columns'] > e['input_columns']]),
            'stages_with_filtering': len([e for e in self.schema_evolution if e['output_columns'] < e['input_columns']])
        }
        
        return total_evolution

# ============================================================================
# ENHANCED ASG BUILDER
# ============================================================================

class ASGBuilder:
    """Enhanced ASG Builder with complete property extraction."""
    
    def __init__(self):
        self.asg = {
            "job_name": "",
            "job_parameters": [],
            "nodes": [],
            "edges": []
        }
        self.pin_records = {}
        self.stage_records = {}
        self.pin_partner_map = {}
        self.stage_input_pins = {}
        self.stage_output_pins = {}
        self.container_record = None  # Store V0 container info
        self.schema_lineage_tracker = SchemaLineageTracker()
    
    def build_from_records(self, records: List[Dict[str, Any]], job_name: str = ""):
        """Main method: Convert parsed records to ASG JSON structure."""
        self.asg["job_name"] = job_name
        
        print("  [1/6] Extracting job parameters...")
        self._extract_job_parameters(records)
        
        print("  [2/6] Organizing records...")
        self._organize_records(records)
        print(f"      Found {len(self.stage_records)} stage records")
        print(f"      Found {len(self.pin_records)} pin records")
        
        print("  [3/6] Extracting nodes with schemas...")
        self._extract_nodes()
        print(f"      Built {len(self.asg['nodes'])} nodes")
        
        print("  [4/6] Building edges from Partner references...")
        self._build_edges()
        print(f"      Created {len(self.asg['edges'])} edges")
        
        print("  [5/6] Tracking schema evolution...")
        self._track_schema_evolution()
        print(f"      Analyzed {len(self.schema_lineage_tracker.schema_evolution)} schema changes")
        
        print("  [6/6] Validating ASG structure...")
        self._validate_asg()
        
        return self.asg
    
    def _extract_job_parameters(self, records: List[Dict[str, Any]]):
        """Extract job parameters from ROOT record."""
        for record in records:
            if record.get('identifier') == 'ROOT':
                lines = record['lines']
                i = 0
                
                while i < len(lines):
                    if lines[i].strip().startswith('BEGIN DSSUBRECORD'):
                        sub_record = get_section_details(lines[i:])
                        
                        if not sub_record:
                            i += 1
                            continue
                        
                        # Extract parameter details
                        param = {}
                        for line in sub_record:
                            if 'Name "' in line:
                                param['name'] = extract_property_name(line)
                            elif 'Prompt "' in line:
                                match = re.search(r'Prompt "(.*?)"', line)
                                if match:
                                    param['prompt'] = match.group(1)
                            elif 'Default "' in line:
                                match = re.search(r'Default "(.*?)"', line)
                                if match:
                                    param['default'] = match.group(1)
                        
                        if param.get('name'):
                            self.asg['job_parameters'].append(param)
                        
                        i += len(sub_record)
                    else:
                        i += 1
    
    def _organize_records(self, records: List[Dict[str, Any]]):
        """Organize stage and pin records for quick lookup."""
        for record in records:
            identifier = record.get('identifier', '')
            
            # Skip ROOT and V0 (container) records
            if identifier in ['ROOT', 'V0'] or not identifier:
                continue
            # Check if this is a pin record (pattern: V<digits>S<digits>P<digits>)
            if re.match(r'^V\d+S\d+P\d+$', identifier):
                self.pin_records[identifier] = record
                
                # Extract Partner ID from pin record
                for line in record['lines']:
                    if 'Partner "' in line:
                        match = re.search(r'Partner "(.*?)"', line)
                        if match:
                            self.pin_partner_map[identifier] = match.group(1)
                            break
            
            # Check if this is a stage record (pattern: V<digits>S<digits>)
            elif re.match(r'^V\d+S\d+$', identifier):
                self.stage_records[identifier] = record
                
                # Extract InputPins and OutputPins
                for line in record['lines']:
                    if 'InputPins "' in line:
                        match = re.search(r'InputPins "(.*?)"', line)
                        if match:
                            pin_list = match.group(1).split('|')
                            self.stage_input_pins[identifier] = pin_list
                    elif 'OutputPins "' in line:
                        match = re.search(r'OutputPins "(.*?)"', line)
                        if match:
                            pin_list = match.group(1).split('|')
                            self.stage_output_pins[identifier] = pin_list
            
            # Store V0 container record
            elif identifier == 'V0':
                self.container_record = record
    
    def _extract_nodes(self):
        """Extract nodes (stages) from stage records."""
        for stage_id, record in self.stage_records.items():
            print(f"      Processing node: {stage_id}")
            node = self._build_enhanced_node_from_record(record)
            self.asg['nodes'].append(node)
    
    def _build_enhanced_node_from_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Build a node structure with complete property extraction."""
        identifier = record.get('identifier', '')
        lines = record['lines']
        
        node = {
            "id": identifier,
            "name": "",
            "type": "",
            "enhanced_type": "",
            "properties": {},
            "enhanced_properties": {},
            "transactional_properties": {},
            "pins": [],
            "schema_lineage": {}
        }
        
        # Extract basic stage properties from main record
        ole_type = ""
        stage_type = ""
        stage_name_found = False
        for line in lines:
            # Extract stage name FIRST - only from the main DSRECORD Name field (not subrecords)
            if 'Name "' in line and not stage_name_found:
                # Check if this is the main record Name field (not inside a subrecord)
                # Main record Name appears early, before BEGIN DSSUBRECORD
                if not any('BEGIN DSSUBRECORD' in prev_line for prev_line in lines[:lines.index(line)]):
                    stage_name = extract_property_name(line)
                    # Avoid setting name to property names that are column names or config keys
                    if stage_name and stage_name not in ['file', 'delimiter', 'firstLineColumnNames', 
                                                       'keys', 'aggregations', 'schema', 'TrxGenCode', 
                                                       'TrxClassName', 'JobParameterNames', 'USERID', 
                                                       'USERNAME', 'EMAIL', 'STATUS', 'VarUsername']:
                        node['name'] = stage_name
                        stage_name_found = True
            elif 'OLEType "' in line:
                match = re.search(r'OLEType "(.*?)"', line)
                if match:
                    ole_type = match.group(1)
                    node['type'] = ole_type
            elif 'StageType "' in line:
                match = re.search(r'StageType "(.*?)"', line)
                if match:
                    stage_type = match.group(1)
                    node['properties']['stage_type'] = stage_type
            elif 'operator "' in line:
                match = re.search(r'operator "(.*?)"', line)
                if match:
                    node['properties']['operator'] = match.group(1)
            elif 'key "' in line and 'KeyPosition' not in line:
                match = re.search(r'key "(.*?)"', line)
                if match:
                    parsed_keys = parse_join_keys(match.group(1))
                    node['properties']['join_key'] = parsed_keys
        
        # Enhanced stage type detection (initial)
        node['enhanced_type'] = detect_complex_stage_type(stage_type, ole_type, lines)
        
        # If stage_type not found in record, try to get it from container's StageTypes
        if (not stage_type or stage_type == "") and self.container_record:
            container_stage_type = self._get_stage_type_from_container(identifier)
            if container_stage_type:
                stage_type = container_stage_type
                node['properties']['stage_type'] = stage_type
                # Re-detect enhanced_type with the container stage type
                node['enhanced_type'] = detect_complex_stage_type(stage_type, ole_type, lines)
        
        # Extract ALL stage properties
        node['enhanced_properties'] = self._extract_stage_properties(lines)
        
        # Find and attach pins for this node
        node['pins'] = self._get_pins_for_node(identifier)
        
        # ALWAYS get stage name from V0 container (most reliable source)
        container_name = self._get_stage_name_from_container_fallback(identifier)
        if container_name and container_name != "Unknown":
            node['name'] = container_name
        
        return node
    
    def _get_stage_name_from_container_fallback(self, stage_id: str) -> str:
        """Get stage name from V0 container StageNames field."""
        if not self.container_record:
            return "Unknown"
        
        # Extract StageList and StageNames separately (they're on different lines)
        stage_list_str = None
        stage_names_str = None
        
        for line in self.container_record['lines']:
            if 'StageList "' in line:
                stage_list_match = re.search(r'StageList "([^"]*)"', line)
                if stage_list_match:
                    stage_list_str = stage_list_match.group(1)
            elif 'StageNames "' in line:
                stage_names_match = re.search(r'StageNames "([^"]*)"', line)
                if stage_names_match:
                    stage_names_str = stage_names_match.group(1)
        
        # Map IDs to names if both are found
        if stage_list_str and stage_names_str:
            stage_ids = stage_list_str.split('|')
            stage_names = stage_names_str.split('|')
            
            # Map IDs to names
            for i, stage_id_in_list in enumerate(stage_ids):
                if stage_id_in_list == stage_id and i < len(stage_names):
                    return stage_names[i]
        
        return "Unknown"
    
    def _get_stage_type_from_container(self, stage_id: str) -> Optional[str]:
        """Get stage type from V0 container StageTypes field."""
        if not self.container_record:
            return None
        
        # Extract StageList and StageTypes separately (they're on different lines)
        stage_list_str = None
        stage_types_str = None
        
        for line in self.container_record['lines']:
            if 'StageList "' in line:
                stage_list_match = re.search(r'StageList "([^"]*)"', line)
                if stage_list_match:
                    stage_list_str = stage_list_match.group(1)
            elif 'StageTypes "' in line:
                stage_types_match = re.search(r'StageTypes "([^"]*)"', line)
                if stage_types_match:
                    stage_types_str = stage_types_match.group(1)
        
        # Map IDs to types if both are found
        if stage_list_str and stage_types_str:
            stage_ids = stage_list_str.split('|')
            stage_types = stage_types_str.split('|')
            
            # Map IDs to types
            for i, stage_id_in_list in enumerate(stage_ids):
                if stage_id_in_list == stage_id and i < len(stage_types):
                    return stage_types[i]
        
        return None

    def _extract_stage_properties(self, lines: List[str]) -> Dict[str, Any]:
        """Extract ALL stage properties from DSSUBRECORD blocks."""
        stage_properties = {
            'configuration': {},
            'apt_properties': {},
            'metadata': {}
        }
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('BEGIN DSSUBRECORD'):
                sub_record = get_section_details(lines[i:])
                
                if not sub_record:
                    i += 1
                    continue
                
                # Process heredoc values in the subrecord
                sub_record = process_subrecord_values(sub_record)
                
                # Extract property name and value
                property_name = None
                property_value = None
                
                for j, sr_line in enumerate(sub_record):
                    if 'Name "' in sr_line:
                        property_name = extract_property_name(sr_line)
                    elif 'Value "' in sr_line:
                        match = re.search(r'Value "(.*?)"', sr_line)
                        if match:
                            property_value = match.group(1)
                    elif 'Value =+=+=+=' in sr_line:
                        # Extract heredoc value using proper multi-line parsing
                        value_content, next_idx = parse_heredoc_value(sub_record, j)
                        property_value = value_content
                
                # Decode DataStage-escaped property values (e.g., file paths)
                property_value = decode_dsx_value(property_value)
                
                # Store property in appropriate category
                if property_name:
                    if property_name in APT_PROPERTIES_PRESERVE:
                        stage_properties['apt_properties'][property_name] = property_value
                    elif property_name in METADATA_PROPERTIES:
                        stage_properties['metadata'][property_name] = property_value
                    else:
                        # Store all other properties as configuration
                        stage_properties['configuration'][property_name] = property_value
                
                i += len(sub_record)
            else:
                i += 1
        
        return stage_properties
    
    def _get_pins_for_node(self, node_id: str) -> List[Dict[str, Any]]:
        """Get all pins for a given node."""
        pins = []
        
        # Get input pins from stage InputPins field
        input_pin_ids = self.stage_input_pins.get(node_id, [])
        for pin_id in input_pin_ids:
            if pin_id in self.pin_records:
                pin = self._build_enhanced_pin(pin_id, self.pin_records[pin_id], 'input')
                pins.append(pin)
        
        # Get output pins from stage OutputPins field
        output_pin_ids = self.stage_output_pins.get(node_id, [])
        for pin_id in output_pin_ids:
            if pin_id in self.pin_records:
                pin = self._build_enhanced_pin(pin_id, self.pin_records[pin_id], 'output')
                pins.append(pin)
        
        # Fallback: Find pins by ID pattern matching
        for pin_id, pin_record in self.pin_records.items():
            if pin_id.startswith(node_id + 'P') and pin_id not in input_pin_ids and pin_id not in output_pin_ids:
                direction = self._get_pin_direction_from_oletype(pin_record)
                pin = self._build_enhanced_pin(pin_id, pin_record, direction)
                pins.append(pin)
        
        return pins
    
    def _build_enhanced_pin(self, pin_id: str, pin_record: Dict[str, Any], direction: str) -> Dict[str, Any]:
        """Build a pin structure with complete schema."""
        lines = pin_record['lines']
        
        pin = {
            "id": pin_id,
            "name": "",
            "direction": direction,
            "schema": [],
            "enhanced_schema": [],
            "properties": {}
        }
        
        # Extract basic pin info
        for line in lines:
            if 'Name "' in line and 'BEGIN' not in line:
                pin['name'] = extract_property_name(line)
                break
        
        # Extract column schemas
        pin['schema'] = self._extract_column_schemas(lines)
        pin['enhanced_schema'] = self._extract_enhanced_column_schemas(lines)

        # Extract pin-level properties (e.g., file path on SequentialFile pins)
        pin['properties'] = {}
        i = 0
        while i < len(lines):
            if lines[i].strip().startswith('BEGIN DSSUBRECORD'):
                sub_record = get_section_details(lines[i:])
                if not sub_record:
                    i += 1
                    continue

                sub_record = process_subrecord_values(sub_record)

                prop_name = None
                prop_value = None
                for j, sr_line in enumerate(sub_record):
                    if 'Name "' in sr_line:
                        prop_name = extract_property_name(sr_line)
                    elif 'Value "' in sr_line:
                        match = re.search(r'Value "(.*?)"', sr_line)
                        if match:
                            prop_value = match.group(1)
                    elif 'Value =+=+=+=' in sr_line:
                        value_content, _ = parse_heredoc_value(sub_record, j)
                        prop_value = value_content

                prop_value = decode_dsx_value(prop_value)
                if prop_name:
                    pin['properties'][prop_name] = prop_value

                i += len(sub_record)
            else:
                i += 1
        
        return pin
    
    def _extract_enhanced_column_schemas(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract column schemas with transformation logic."""
        schemas = []
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('BEGIN DSSUBRECORD'):
                sub_record = get_section_details(lines[i:])
                
                if not sub_record:
                    i += 1
                    continue
                
                # Check if this is a column definition
                column = {}
                transformation_logic = {}
                

                
                for sr_line in sub_record:
                    if 'Name "' in sr_line and 'BEGIN' not in sr_line:
                        column['name'] = extract_property_name(sr_line)
                    elif 'SqlType "' in sr_line:
                        match = re.search(r'SqlType "(.*?)"', sr_line)
                        if match:
                            sql_type_code = match.group(1)
                            column['type'] = map_sql_type(sql_type_code)
                            column['sql_type_code'] = sql_type_code
                    elif 'Precision "' in sr_line:
                        match = re.search(r'Precision "(.*?)"', sr_line)
                        if match:
                            column['length'] = match.group(1)
                    elif 'Scale "' in sr_line:
                        match = re.search(r'Scale "(.*?)"', sr_line)
                        if match:
                            column['scale'] = match.group(1)
                    elif 'Nullable "' in sr_line:
                        match = re.search(r'Nullable "(.*?)"', sr_line)
                        if match:
                            column['nullable'] = match.group(1) == '1'
                    elif 'Derivation "' in sr_line:
                        match = re.search(r'Derivation "(.*?)"', sr_line)
                        if match:
                            derivation = match.group(1)
                            column['derivation'] = derivation
                            transformation_logic = extract_transformation_logic(derivation)
                            column['transformation_logic'] = transformation_logic
                
                # Enhanced column structure
                if column.get('name') and column.get('type'):
                    enhanced_column = column.copy()
                    enhanced_column['has_transformation'] = bool(column.get('derivation'))
                    enhanced_column['transformation_classification'] = transformation_logic.get('type', 'none')
                    schemas.append(enhanced_column)
                
                i += len(sub_record)
            else:
                i += 1
        
        return schemas
    
    def _extract_column_schemas(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract column schemas from pin record lines."""
        schemas = []
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('BEGIN DSSUBRECORD'):
                sub_record = get_section_details(lines[i:])
                
                if not sub_record:
                    i += 1
                    continue
                
                # Check if this is a column definition
                column = {}
                for sr_line in sub_record:
                    if 'Name "' in sr_line and 'BEGIN' not in sr_line:
                        column['name'] = extract_property_name(sr_line)
                    elif 'SqlType "' in sr_line:
                        match = re.search(r'SqlType "(.*?)"', sr_line)
                        if match:
                            sql_type_code = match.group(1)
                            column['type'] = map_sql_type(sql_type_code)
                            column['sql_type_code'] = sql_type_code
                    elif 'Precision "' in sr_line:
                        match = re.search(r'Precision "(.*?)"', sr_line)
                        if match:
                            column['length'] = match.group(1)
                    elif 'Scale "' in sr_line:
                        match = re.search(r'Scale "(.*?)"', sr_line)
                        if match:
                            column['scale'] = match.group(1)
                    elif 'Nullable "' in sr_line:
                        match = re.search(r'Nullable "(.*?)"', sr_line)
                        if match:
                            column['nullable'] = match.group(1) == '1'
                    elif 'Derivation "' in sr_line:
                        match = re.search(r'Derivation "(.*?)"', sr_line)
                        if match:
                            column['derivation'] = match.group(1)
                
                # Only add if it's a valid column definition
                if column.get('name') and column.get('type'):
                    schemas.append(column)
                
                i += len(sub_record)
            else:
                i += 1
        
        return schemas
    
    def _get_pin_direction_from_oletype(self, pin_record: Dict[str, Any]) -> str:
        """Determine pin direction from OLEType field."""
        for line in pin_record['lines']:
            if 'OLEType "CTrxInput"' in line:
                return 'input'
            elif 'OLEType "CTrxOutput"' in line:
                return 'output'
        return ''
    
    def _build_edges(self):
        """Build edges by resolving Partner IDs between pins."""
        # Process partner mappings to create edges
        for pin_id, partner_ref in self.pin_partner_map.items():
            # Partner format: "V0S348|V0S348P4" or just "V0S348P4"
            partner_parts = partner_ref.split('|')
            if len(partner_parts) >= 2:
                partner_pin_id = partner_parts[1]
            else:
                partner_pin_id = partner_parts[0]
            
            # Get pin directions
            pin_direction = self._get_pin_direction(pin_id)
            
            # Create edge based on direction - both input and output pins can create edges
            edge = {
                "from_node": pin_id.rsplit('P', 1)[0],
                "from_pin": pin_id,
                "from_pin_name": self._get_pin_name(pin_id),
                "to_node": partner_pin_id.rsplit('P', 1)[0],
                "to_pin": partner_pin_id,
                "to_pin_name": self._get_pin_name(partner_pin_id),
                "join_type": self._determine_join_type(pin_id, partner_pin_id)
            }
            
            # Avoid duplicates
            if not self._edge_exists(edge):
                self.asg['edges'].append(edge)
        
        # Also check container record for additional edges
        if self.container_record:
            for line in self.container_record['lines']:
                # Look for LinkSourcePinIDs to find additional connections
                if 'LinkSourcePinIDs "' in line:
                    link_source_match = re.search(r'LinkSourcePinIDs "([^"]*)"', line)
                    if link_source_match:
                        source_pins = link_source_match.group(1).split('|')
                        
                        # Look for TargetStageIDs to find targets
                        if 'TargetStageIDs "' in line:
                            target_stage_match = re.search(r'TargetStageIDs "([^"]*)"', line)
                            if target_stage_match:
                                target_stages = target_stage_match.group(1).split('|')
                                
                                # Create edges from source pins to target stages
                                for i, source_pin in enumerate(source_pins):
                                    if source_pin and i < len(target_stages) and target_stages[i]:
                                        target_stage_id = target_stages[i]
                                        if target_stage_id in self.stage_records:
                                            # Find the first input pin of the target stage
                                            target_pins = self.stage_input_pins.get(target_stage_id, [])
                                            if target_pins:
                                                for target_pin in target_pins:
                                                    if target_pin in self.pin_records:
                                                        edge = {
                                                            "from_node": source_pin.rsplit('P', 1)[0] if 'P' in source_pin else source_pin,
                                                            "from_pin": source_pin,
                                                            "from_pin_name": self._get_pin_name(source_pin),
                                                            "to_node": target_stage_id,
                                                            "to_pin": target_pin,
                                                            "to_pin_name": self._get_pin_name(target_pin),
                                                            "join_type": "unknown"
                                                        }
                                                        
                                                        if not self._edge_exists(edge):
                                                            self.asg['edges'].append(edge)
    
    def _determine_join_type(self, from_pin: str, to_pin: str) -> str:
        """Determine join type from stage properties."""
        from_node_id = from_pin.rsplit('P', 1)[0]
        to_node_id = to_pin.rsplit('P', 1)[0]
        
        # Check if target node has join operator property
        if to_node_id in self.stage_records:
            for line in self.stage_records[to_node_id]['lines']:
                if 'operator "' in line:
                    match = re.search(r'operator "(.*?)"', line)
                    if match:
                        operator = match.group(1).lower()
                        if 'left' in operator:
                            return 'left_outer'
                        elif 'full' in operator:
                            return 'full_outer'
                        elif 'inner' in operator:
                            return 'inner'
        
        return 'unknown'
    
    def _get_pin_direction(self, pin_id: str) -> str:
        """Get pin direction from pin record."""
        if pin_id in self.pin_records:
            for line in self.pin_records[pin_id]['lines']:
                if 'OLEType "CTrxInput"' in line:
                    return 'input'
                elif 'OLEType "CTrxOutput"' in line:
                    return 'output'
        return ""
    
    def _get_pin_name(self, pin_id: str) -> str:
        """Get pin name from pin record."""
        if pin_id in self.pin_records:
            for line in self.pin_records[pin_id]['lines']:
                if 'Name "' in line and 'BEGIN' not in line:
                    return extract_property_name(line)
        return ""
    
    def _track_schema_evolution(self):
        """Track schema evolution across pipeline stages."""
        for node in self.asg['nodes']:
            # Get input and output schemas
            input_pins = [pin for pin in node['pins'] if pin['direction'] == 'input']
            output_pins = [pin for pin in node['pins'] if pin['direction'] == 'output']
            
            # Combine schemas
            input_schema = []
            for pin in input_pins:
                input_schema.extend(pin['enhanced_schema'])
            
            output_schema = []
            for pin in output_pins:
                output_schema.extend(pin['enhanced_schema'])
            
            # Track transformations
            transformations = []
            for col in output_schema:
                if col.get('has_transformation'):
                    transformations.append({
                        'column': col['name'],
                        'type': col.get('transformation_classification'),
                    })
            
            # Track lineage
            self.schema_lineage_tracker.track_stage_schema(
                node['id'], input_schema, output_schema, transformations
            )
    
    def _edge_exists(self, edge: Dict[str, Any]) -> bool:
        """Check if edge already exists."""
        for e in self.asg['edges']:
            if e['from_pin'] == edge['from_pin'] and e['to_pin'] == edge['to_pin']:
                return True
        return False
    
    def _validate_asg(self):
        """Validate ASG structure and report any issues."""
        issues = []
        warnings = []
        
        # Check for pins without direction
        for node in self.asg['nodes']:
            for pin in node['pins']:
                if not pin['direction']:
                    issues.append(f"Pin {pin['id']} ({pin['name']}) has no direction")
        
        # Check for missing enhanced properties
        for node in self.asg['nodes']:
            if not node.get('enhanced_type'):
                warnings.append(f"Node {node['id']} ({node['name']}) missing enhanced type classification")
        
        # Check for missing transformation logic
        total_columns_with_derivations = 0
        for node in self.asg['nodes']:
            for pin in node['pins']:
                for col in pin['enhanced_schema']:
                    if col.get('has_transformation'):
                        total_columns_with_derivations += 1
        
        if total_columns_with_derivations > 0:
            print(f"      ✓ Found {total_columns_with_derivations} columns with transformation logic preserved")
        
        # Schema evolution validation
        evolution_summary = self.schema_lineage_tracker.get_evolution_summary()
        if evolution_summary:
            print(f"      ✓ Schema evolution: {evolution_summary.get('stages_with_enrichment', 0)} enrichment stages, "
                  f"{evolution_summary.get('net_column_change', 0)} net column change")
        
        if issues:
            print(f"\n      ⚠ Validation found {len(issues)} critical issues:")
            for issue in issues[:5]:
                print(f"        - {issue}")
        else:
            print(f"      ✓ All pins have directions")
        
        if warnings:
            print(f"\n      ⚠ Validation found {len(warnings)} warnings:")
            for warning in warnings[:5]:
                print(f"        - {warning}")
        else:
            print(f"      ✓ All enhanced properties properly classified")
    
    def save_to_file(self, filepath: str):
        """Save ASG to JSON file with enhanced structure."""
        enhanced_asg = self.asg.copy()
        enhanced_asg['schema_lineage'] = {
            'evolution_events': self.schema_lineage_tracker.schema_evolution,
            'summary': self.schema_lineage_tracker.get_evolution_summary()
        }
        
        with open(filepath, 'w') as f:
            json.dump(enhanced_asg, f, indent=2)
    
    def to_json_string(self, indent: int = 2) -> str:
        """Return ASG as formatted JSON string with enhanced structure."""
        enhanced_asg = self.asg.copy()
        enhanced_asg['schema_lineage'] = {
            'evolution_events': self.schema_lineage_tracker.schema_evolution,
            'summary': self.schema_lineage_tracker.get_evolution_summary()
        }
        return json.dumps(enhanced_asg, indent=indent)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # CLI: accept filepath and optional -d/--debug
    file_path = 'simple_user_job.dsx'
    if len(sys.argv) > 1:
        # Allow flags and the first non-flag as file
        for arg in sys.argv[1:]:
            if arg in ('-d', '--debug'):
                DEBUG = True
            elif arg.startswith('-'):
                continue
            else:
                file_path = arg

    print("=" * 70)
    print("IBM DATASTAGE DSX PARSER - ENHANCED v5.0")
    print("=" * 70)

    try:
        lines = get_lines(file_path)
        print(f"Loaded DSX file: {file_path}")
        print(f"Total lines: {len(lines)}\n")
        
        # Extract job section
        job = get_job(lines)
        print(f"Extracted job section with {len(job)} lines\n")
        
        # Parse all records
        records = get_sections(job)
        print(f"Found {len(records)} records\n")
        
        # Build ASG from parsed records
        print("=" * 70)
        print("BUILDING ABSTRACT SYNTAX GRAPH (ASG) - ENHANCED v5.0")
        print("=" * 70)
        
        builder = ASGBuilder()
        asg = builder.build_from_records(records, job_name="J_DEMO_001")
        
        # Print ASG summary
        print(f"\n✓ ASG Construction Complete!")
        print(f"  Job Name: {asg['job_name']}")
        print(f"  Parameters: {len(asg['job_parameters'])}")
        print(f"  Nodes (Stages): {len(asg['nodes'])}")
        print(f"  Edges (Links): {len(asg['edges'])}")
        
        # Print job parameters
        if asg['job_parameters']:
            print("\n" + "=" * 70)
            print("JOB PARAMETERS")
            print("=" * 70)
            for param in asg['job_parameters']:
                print(f"  {param.get('name', 'N/A')}: {param.get('default', 'N/A')}")
        
        # Print nodes summary with complete properties
        print("\n" + "=" * 70)
        print("NODES (STAGES) - ENHANCED WITH ALL PROPERTIES")
        print("=" * 70)
        for node in asg['nodes']:
            pin_count = len(node['pins'])
            schema_counts = sum(len(pin['schema']) for pin in node['pins'])
            enhanced_schema_counts = sum(len(pin['enhanced_schema']) for pin in node['pins'])
            
            print(f"  [{node['id']}] {node['name']} ({node['type']} -> {node['enhanced_type']})")
            print(f"      Pins: {pin_count} | Columns: {schema_counts} | Enhanced: {enhanced_schema_counts}")
            
            # Show configuration properties
            if node['enhanced_properties'].get('configuration'):
                print(f"      Configuration Properties:")
                for prop_name, prop_value in node['enhanced_properties']['configuration'].items():
                    print(f"        - {prop_name}: {prop_value}")
            
            # Show APT properties
            if node['enhanced_properties'].get('apt_properties'):
                print(f"      APT Properties:")
                for prop_name, prop_value in node['enhanced_properties']['apt_properties'].items():
                    # Truncate long values
                    display_value = prop_value[:100] + "..." if len(str(prop_value)) > 100 else prop_value
                    print(f"        - {prop_name}: {display_value}")
            
            # Show join keys if present
            if 'join_key' in node.get('properties', {}):
                join_key_info = node['properties']['join_key']
                if isinstance(join_key_info, dict) and 'parsed_keys' in join_key_info:
                    print(f"      Join Keys: {', '.join(join_key_info['parsed_keys'])} ({join_key_info.get('join_type', 'unknown')})")
        
        # Print edges summary
        print("\n" + "=" * 70)
        print("EDGES (LINKS)")
        print("=" * 70)
        for edge in asg['edges']:
            join_type = edge.get('join_type', 'unknown')
            print(f"  {edge['from_node']}.{edge['from_pin_name']} -> {edge['to_node']}.{edge['to_pin_name']} ({join_type})")
        
        # Show complete schema for all nodes
        print("\n" + "=" * 70)
        print("COMPLETE NODE SCHEMAS")
        print("=" * 70)
        for node in asg['nodes']:
            print(f"\nNode: {node['name']} (ID: {node['id']})")
            for pin in node['pins']:
                print(f"\n  Pin: {pin['name']} ({pin['direction']}) - {len(pin['enhanced_schema'])} columns")
                for col in pin['enhanced_schema']:
                    derivation = col.get('derivation', '')
                    print(f"    - {col['name']}: {col['type']}({col.get('length', 'N/A')}) "
                          f"nullable={col.get('nullable', False)}")
                    if derivation:
                        print(f"      Derivation: {derivation[:80]}{'...' if len(derivation) > 80 else ''}")
        
        # Schema evolution summary
        evolution_summary = builder.schema_lineage_tracker.get_evolution_summary()
        if evolution_summary:
            print("\n" + "=" * 70)
            print("SCHEMA EVOLUTION ANALYSIS")
            print("=" * 70)
            print(f"  Stages Analyzed: {evolution_summary.get('stages_analyzed', 0)}")
            print(f"  Total Input Columns: {evolution_summary.get('total_input_columns', 0)}")
            print(f"  Total Output Columns: {evolution_summary.get('total_output_columns', 0)}")
            print(f"  Net Column Change: {evolution_summary.get('net_column_change', 0)}")
            print(f"  Enrichment Stages: {evolution_summary.get('stages_with_enrichment', 0)}")
            print(f"  Filtering Stages: {evolution_summary.get('stages_with_filtering', 0)}")
        
        # Save enhanced ASG to file (based on input filename)
        import os
        base = os.path.basename(file_path)
        output_file = os.path.splitext(base)[0] + '.json'
        builder.save_to_file(output_file)
        print(f"\n✓ Enhanced ASG saved to: {output_file}")
        
        # Print statistics
        total_columns = sum(
            len(pin['schema']) 
            for node in asg['nodes'] 
            for pin in node['pins']
        )
        total_enhanced_columns = sum(
            len(pin['enhanced_schema']) 
            for node in asg['nodes'] 
            for pin in node['pins']
        )
        columns_with_transformations = sum(
            sum(1 for col in pin['enhanced_schema'] if col.get('has_transformation'))
            for node in asg['nodes'] 
            for pin in node['pins']
        )
        
        print(f"\n" + "=" * 70)
        print("ENHANCED STATISTICS")
        print("=" * 70)
        print(f"  Total Columns Extracted: {total_columns}")
        print(f"  Total Enhanced Columns: {total_enhanced_columns}")
        print(f"  Columns with Transformations: {columns_with_transformations}")
        print(f"  Total Edges: {len(asg['edges'])}")
        print(f"  Average Columns per Node: {total_columns / len(asg['nodes']) if asg['nodes'] else 0:.1f}")
        print(f"  Transformation Coverage: {(columns_with_transformations / total_enhanced_columns * 100) if total_enhanced_columns > 0 else 0:.1f}%")
        
    except FileNotFoundError:
        print(f"❌ Error: DSX file '{file_path}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print(f"\n✅ Parsing completed successfully!")