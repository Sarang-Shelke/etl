def get_lines(file_path):
    """Simple file reader"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            return file.readlines()
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

def get_section_details(lines, start_marker, end_marker):
    """Extract sections between markers"""
    sections = []
    current_section = []
    in_section = False
    
    for line in lines:
        if start_marker in line:
            in_section = True
            current_section = [line]
        elif end_marker in line and in_section:
            current_section.append(line)
            sections.append(''.join(current_section))
            current_section = []
            in_section = False
        elif in_section:
            current_section.append(line)
    
    return sections

def get_sub_records(section):
    """Get subrecords from section - just grab DSSUBRECORD blocks"""
    records = []
    lines = section.split('\n')
    current_record = []
    in_record = False
    
    for line in lines:
        if 'DSSUBRECORD' in line:
            in_record = True
            current_record = [line]
        elif line.strip() == '' and in_record and 'DSENDRECORD' in '\n'.join(current_record):
            current_record.append(line)
            records.append('\n'.join(current_record))
            current_record = []
            in_record = False
        elif in_record:
            current_record.append(line)
    
    return records

def get_records(content):
    """Get records from content DSRECORD blocks"""
    records = []
    lines = content.split('\n')
    current_record = []
    in_record = False
    
    for line in lines:
        if 'BEGIN DSRECORD' in line:
            in_record = True
            current_record = [line]
        elif 'END DSRECORD' in line and in_record:
            current_record.append(line)
            records.append('\n'.join(current_record))
            current_record = []
            in_record = False
        elif in_record:
            current_record.append(line)
    
    return records

def get_sections(content):
    """Get all sections from content"""
    sections = []
    lines = content.split('\n')
    current_section = []
    in_section = False
    
    for line in lines:
        if line.strip().startswith('BEGIN'):
            in_section = True
            current_section = [line]
        elif line.strip().startswith('END') and in_section:
            current_section.append(line)
            sections.append('\n'.join(current_section))
            current_section = []
            in_section = False
        elif in_section:
            current_section.append(line)
    
    return sections

def get_job(content):
    """Get job section"""
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'DSJOB' in line:
            # Get this line and all lines until we hit DSENDJOB
            job_lines = [line]
            for j in range(i + 1, len(lines)):
                job_lines.append(lines[j])
                if 'DSENDJOB' in lines[j]:
                    break
            return '\n'.join(job_lines)
    return ""

def extract_stage_info(record):
    """Extract simple stage info from record"""
    lines = record.split('\n')
    stage_info = {}
    
    for line in lines:
        line = line.strip()
        # Extract key-value pairs in format: Key "Value"
        if ' ' in line and '"' in line:
            parts = line.split(' ', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip().strip('"')
                
                # Store important properties
                if key in ['Name', 'StageType', 'InputPins', 'OutputPins', 'OLEType', 'Identifier']:
                    stage_info[key] = value
                else:
                    # Store other properties too
                    stage_info[key] = value
    
    return stage_info

def build_asg(dsx_file_path):
    """Build simple ASG from DSX file"""
    print(f"Reading file: {dsx_file_path}")
    
    # Read file
    lines = get_lines(dsx_file_path)
    if not lines:
        print("No lines read from file")
        return None
    
    content = ''.join(lines)
    print(f"Read {len(lines)} lines")
    
    all_records = get_records(content)
    print(f"Found {len(all_records)} records")
    
    nodes = {}
    stage_count = 0
    
    for record in all_records:
        stage_info = extract_stage_info(record)
        
        if stage_count < 3:
            print(f"Record {stage_count + 1} keys: {list(stage_info.keys())}")
            if 'StageType' in stage_info:
                print(f"  Found stage type: {stage_info['StageType']}")
            if 'OLEType' in stage_info:
                print(f"  OLEType: {stage_info['OLEType']}")
        
        if 'StageType' in stage_info:
            stage_name = stage_info.get('Name', 'Unknown_Stage')
            nodes[stage_name] = stage_info
            nodes[stage_name]['raw_record'] = record  # Store raw data for reference
            stage_count += 1
    
    print(f"Found {len(nodes)} stages")
    
    edges = {}
    stage_names = list(nodes.keys())
    
    for i in range(len(stage_names) - 1):
        from_stage = stage_names[i]
        to_stage = stage_names[i + 1]
        edges[from_stage] = to_stage
    
    asg = {
        "nodes": nodes,
        "edges": edges
    }
    
    print(f"Built ASG with {len(nodes)} nodes and {len(edges)} edges")
    return asg

def save_asg_to_file(asg, output_file_path):
    """Save ASG to JSON file"""
    import json
    
    try:
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(asg, file, indent=2, ensure_ascii=False)
        print(f"ASG saved to: {output_file_path}")
        return True
    except Exception as e:
        print(f"Error saving ASG: {e}")
        return False

if __name__ == "__main__":
    dsx_file = "DIM_TEST_CONTRACT_AECB_I (1).dsx"
    output_file = "asg_simple.json"
    
    print("=== Super Simple ASG Builder ===")
    print("Building ASG from DSX file...")
    
    asg = build_asg(dsx_file)
    
    if asg:
        save_asg_to_file(asg, output_file)
        
        print(f"\n=== ASG Summary ===")
        print(f"Nodes: {len(asg['nodes'])}")
        print(f"Edges: {len(asg['edges'])}")
        
        print(f"\nStage names: {list(asg['nodes'].keys())}")
        print(f"Edges: {asg['edges']}")

        if asg['nodes']:
            first_node = list(asg['nodes'].keys())[0]
            print(f"\nSample stage '{first_node}' properties:")
            for key, value in asg['nodes'][first_node].items():
                if key != 'raw_record':
                    print(f"  {key}: {str(value)[:100]}...")
    else:
        print("Failed to build ASG")