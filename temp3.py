def get_lines(file_path):
    with open(file_path, errors='ignore') as f:
        return(f.read().splitlines())

def get_section_details(content: list[str]):
    try:
        section = content[0].split(' ')[1]
        search_str = f'END {section}'
        for i, line in enumerate(content):
            if line.endswith(search_str):
                return content[:i+1]
    except IndexError:
        return []
    return []


def get_sub_records(content: list[str]):
    sub_records = []
    i = 0 
    # CHANGED: Use while loop to manually control index 'i'
    while i < len(content):
        line = content[i].strip()
        if line.startswith("BEGIN DSSUBRECORD"):
            sub_record = get_section_details(content[i:])
            
            if len(sub_record) > 1 and '"APT"' in sub_record[1]:
                i += len(sub_record)
                continue
            else:
                sub_records.append(sub_record)
                # Advance index 'i' past this subrecord
                i += len(sub_record)
        else:
            i += 1
    return sub_records

def validate_sub_record(content: list[str]):
    sub_record = get_section_details(content)
    
    if len(sub_record) > 1 and 'Owner "APT"' in sub_record[1]:
        return len(sub_record)
    else:
        return sub_record 

def get_records(content: list[str]):
    records = []
    for i, line in enumerate(content):
        if line.startswith("BEGIN DSRECORD"):
            record = get_section_details(content[i:])
            
            j = 0
            while j < len(record):
                if record[j].startswith("BEGIN DSSUBRECORD"):
                    sub_record_result = validate_sub_record(record[j:])
                    
                    if isinstance(sub_record_result, int):
                        del record[j : j + sub_record_result]
                        continue 
                
                j += 1
            
            records.append(record)

    return records

def get_sections(content: list[str]):
    content = [line.strip() for line in content]
    get_records(content=content)

def get_job(content: list[str]):
    content = [line.strip() for line in content]
    for i, line in enumerate(content):
        if line.startswith("BEGIN DSJOB"):
            job=get_section_details(content[i:])
            return job
    return []

# Run
lines = get_lines('DIM_TEST_CONTRACT_AECB_I (1).dsx')

print(get_sections(get_job(lines)))