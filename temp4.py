import re

# ============================================================================
# CONFIGURATION - Properties to omit during parsing
# ============================================================================

# Job-level properties to skip (ROOT record)
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

# Container/View properties to skip (V0 record)
VIEW_OMIT = {
    'StageXPos', 'StageYPos', 'StageXSize', 'StageYSize',
    'ContainerViewSizing', 'ZoomValue', 'SnapToGrid', 'GridLines',
    'LinkNamePositionXs', 'LinkNamePositionYs'
}

# Stage-level properties to skip
STAGE_OMIT = {
    'DiskWriteInc', 'BufFreeRun', 'MaxMemBufSize', 'QueueUpperSize',
    'Preserve', 'SchemaFormat', 'RTColumnProp'
}

# Properties to extract but flag for manual review
EXTRACT_AS_METADATA = {
    'TrxGenCode', 'TrxClassName', 'TrxGenCache', 'E2Assembly'
}


# ============================================================================
# CORE PARSING FUNCTIONS
# ============================================================================

def get_lines(file_path):
    """Read file and return as list of lines."""
    with open(file_path, errors='ignore') as f:
        return f.read().splitlines()


def get_section_details(content: list[str]):
    """
    Extract a complete BEGIN/END section block.
    Returns all lines from BEGIN to matching END.
    """
    try:
        section = content[0].split(' ')[1]
        search_str = f'END {section}'
        
        for i, line in enumerate(content):
            # FIXED: Added strip() to handle whitespace
            if line.strip().endswith(search_str):
                return content[:i+1]
    except IndexError:
        return []
    
    return []


def parse_heredoc_value(content: list[str], start_idx: int):
    """
    Parse a value that might be a heredoc block (=+=+=+=).
    Returns: (value_string, next_line_index)
    """
    line = content[start_idx].strip()
    
    # Check if this is a heredoc block
    if '=+=+=+=' in line:
        heredoc_lines = []
        i = start_idx + 1
        
        # Read until closing delimiter
        while i < len(content):
            if content[i].strip() == '=+=+=+=':
                return '\n'.join(heredoc_lines), i + 1
            heredoc_lines.append(content[i])
            i += 1
        
        # No closing delimiter found - return what we have
        return '\n'.join(heredoc_lines), len(content)
    
    # Not a heredoc - try to extract inline quoted value
    if 'Value "' in line:
        match = re.search(r'Value "(.*)"', line)
        return match.group(1) if match else '', start_idx + 1
    
    # Raw line value
    return line, start_idx + 1


def should_omit_property(property_name: str, context: str = 'stage'):
    """Check if a property should be omitted based on context."""
    if context == 'root':
        return property_name in ROOT_OMIT
    elif context == 'view':
        return property_name in VIEW_OMIT
    elif context == 'stage':
        return property_name in STAGE_OMIT
    return False


def is_apt_property(sub_record: list[str]):
    """Check if a subrecord is an APT-owned property."""
    if len(sub_record) > 1:
        # FIXED: More precise check
        return 'Owner "APT"' in sub_record[1]
    return False


def extract_property_name(line: str):
    """Extract property name from a Name line."""
    match = re.search(r'Name "(.*?)"', line)
    if match:
        return match.group(1)
    return None


# ============================================================================
# SUBRECORD PARSING
# ============================================================================

def get_sub_records(content: list[str], context: str = 'stage'):
    """
    Extract all DSSUBRECORD blocks from content.
    Filters out APT properties and omitted properties based on context.
    """
    sub_records = []
    i = 0
    
    while i < len(content):
        line = content[i].strip()
        
        if line.startswith("BEGIN DSSUBRECORD"):
            sub_record = get_section_details(content[i:])
            
            if not sub_record:
                i += 1
                continue
            
            # Skip APT-owned properties
            if is_apt_property(sub_record):
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


def process_subrecord_values(sub_record: list[str]):
    """
    Process a subrecord to handle heredoc values.
    Returns a new subrecord with heredoc content properly extracted.
    """
    processed = []
    i = 0
    
    while i < len(sub_record):
        line = sub_record[i]
        
        # Check if this is a Value line with heredoc
        if 'Value =+=+=+=' in line or line.strip() == '=+=+=+=':
            # Extract the heredoc value
            value_content, next_idx = parse_heredoc_value(sub_record, i)
            
            # Store as a single processed line
            processed.append(f'      Value =+=+=+={value_content}=+=+=+=')
            i = next_idx
        else:
            processed.append(line)
            i += 1
    
    return processed


# ============================================================================
# RECORD PARSING
# ============================================================================

def get_records(content: list[str]):
    """
    Extract all DSRECORD blocks from content.
    Filters nested DSSUBRECORDs based on APT ownership.
    """
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
            
            # Filter out APT-owned subrecords inline
            filtered_record = []
            j = 0
            
            while j < len(record):
                if record[j].strip().startswith("BEGIN DSSUBRECORD"):
                    sub_record = get_section_details(record[j:])
                    
                    if not sub_record:
                        filtered_record.append(record[j])
                        j += 1
                        continue
                    
                    # Skip APT properties
                    if is_apt_property(sub_record):
                        j += len(sub_record)
                        continue
                    
                    # Check if we should omit based on property name
                    property_name = None
                    for sr_line in sub_record:
                        if 'Name "' in sr_line:
                            property_name = extract_property_name(sr_line)
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

def get_job(content: list[str]):
    """Extract the DSJOB section from file content."""
    content = [line.strip() for line in content]
    
    for i, line in enumerate(content):
        if line.startswith("BEGIN DSJOB"):
            job = get_section_details(content[i:])
            return job
    
    return []


def get_sections(content: list[str]):
    """
    Parse all records from job content.
    FIXED: Now returns the result.
    """
    content = [line.strip() for line in content]
    return get_records(content=content)


# ============================================================================
# HELPER FUNCTIONS FOR ANALYSIS
# ============================================================================

def extract_job_parameters(records: list[dict]):
    """Extract job parameters from ROOT record."""
    parameters = []
    
    for record in records:
        if record.get('identifier') == 'ROOT':
            i = 0
            lines = record['lines']
            
            while i < len(lines):
                if lines[i].strip().startswith('BEGIN DSSUBRECORD'):
                    sub_record = get_section_details(lines[i:])
                    
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
                        parameters.append(param)
                    
                    i += len(sub_record)
                else:
                    i += 1
    
    return parameters


def extract_stages(records: list[dict]):
    """Extract stage information from records."""
    stages = []
    
    for record in records:
        identifier = record.get('identifier', '')
        
        # Skip ROOT and V0 (container) records
        if identifier in ['ROOT', 'V0'] or not identifier:
            continue
        
        # Look for stage properties
        stage = {'identifier': identifier}
        
        for line in record['lines']:
            if 'Name "' in line and 'BEGIN' not in line:
                stage['name'] = extract_property_name(line)
            elif 'OLEType "' in line:
                match = re.search(r'OLEType "(.*?)"', line)
                if match:
                    stage['type'] = match.group(1)
            elif 'StageType "' in line:
                match = re.search(r'StageType "(.*?)"', line)
                if match:
                    stage['stage_type'] = match.group(1)
        
        stages.append(stage)
    
    return stages


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Load the DSX file
    file_path = 'DIM_TEST_CONTRACT_AECB_I (1).dsx'
    lines = get_lines(file_path)
    
    # Extract job section
    job = get_job(lines)
    print(f"Extracted job with {len(job)} lines\n")
    
    # Parse all records
    records = get_sections(job)
    print(f"Found {len(records)} records\n")
    
    # Display record summary
    print("=" * 70)
    print("RECORD SUMMARY")
    print("=" * 70)
    
    for record in records:
        identifier = record.get('identifier', 'Unknown')
        context = record.get('context', 'unknown')
        line_count = len(record.get('lines', []))
        print(f"ID: {identifier:30} Context: {context:10} Lines: {line_count}")
    
    # Extract job parameters
    print("\n" + "=" * 70)
    print("JOB PARAMETERS")
    print("=" * 70)
    
    parameters = extract_job_parameters(records)
    for param in parameters:
        print(f"Name: {param.get('name', 'N/A')}")
        print(f"  Default: {param.get('default', 'N/A')}")
        print(f"  Prompt: {param.get('prompt', 'N/A')}")
        print()
    
    # Extract stages
    print("=" * 70)
    print("STAGES")
    print("=" * 70)
    
    stages = extract_stages(records)
    for stage in stages:
        print(f"ID: {stage.get('identifier', 'N/A'):20} "
              f"Name: {stage.get('name', 'N/A'):30} "
              f"Type: {stage.get('type', 'N/A')}")
    
    print(f"\nTotal stages found: {len(stages)}")