#!/usr/bin/env python3
"""
Unit tests for ASG → Talend IR conversion

Tests both simple_user_job.json and INERACTIVE_TEST_HEADER_DATA 1.json
"""

import json
import sys
import os
from pathlib import Path

# Add temp_7 to path
sys.path.insert(0, os.path.dirname(__file__))

def load_json(filepath):
    """Load JSON file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_simple_job_nodes():
    """Test: Simple job has 3 nodes"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    components = ir.get('components', [])
    assert len(components) == 3, f"Expected 3 components, got {len(components)}"
    
    names = {c['name'] for c in components}
    assert names == {'Input_File', 'User_Transformer', 'Output_File'}, f"Unexpected component names: {names}"
    
    print("✅ Test 1: Simple job has 3 nodes")

def test_simple_job_connections():
    """Test: Simple job has 3 connections"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    connections = ir.get('connections', [])
    assert len(connections) == 3, f"Expected 3 connections, got {len(connections)}"
    
    print("✅ Test 2: Simple job has 3 connections")

def test_simple_job_talend_components():
    """Test: Simple job maps to correct Talend components"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    comp_map = {c['name']: c['talend_component'] for c in ir['components']}
    
    assert comp_map['Input_File'] == 'tFileInputDelimited', f"Expected tFileInputDelimited for Input_File"
    assert comp_map['User_Transformer'] == 'tMap', f"Expected tMap for User_Transformer"
    
    print("✅ Test 3: Simple job maps to correct Talend components")

def test_simple_job_file_path():
    """Test: Simple job extracts file path"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    input_comp = next(c for c in ir['components'] if c['name'] == 'Input_File')
    assert 'file_path' in input_comp['configuration'], "Missing file_path in Input_File"
    assert 'inputfile.csv' in input_comp['configuration']['file_path'], "Unexpected file path"
    
    print("✅ Test 4: Simple job extracts file path")

def test_simple_job_transformations():
    """Test: Simple job extracts 4 transformations"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    transformer = next(c for c in ir['components'] if c['name'] == 'User_Transformer')
    output_pin = transformer['schema']['output_pins'][0]
    columns = output_pin['columns']
    
    assert len(columns) == 4, f"Expected 4 columns, got {len(columns)}"
    
    # Check transformation logic
    transformations = [c for c in columns if c.get('transformation')]
    assert len(transformations) == 4, f"Expected 4 transformations, got {len(transformations)}"
    
    print("✅ Test 5: Simple job extracts 4 transformations")

def test_simple_job_trxgen():
    """Test: Simple job preserves TrxClassName"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    transformer = next(c for c in ir['components'] if c['name'] == 'User_Transformer')
    talend_spec = transformer.get('talend_specific', {})
    
    assert 'trx_class_name' in talend_spec, "Missing trx_class_name"
    assert talend_spec['trx_class_name'] == 'V0S1_SIMPLE_USER_PROCESS_User_Transformer'
    
    print("✅ Test 6: Simple job preserves TrxClassName")

# ============================================================================
# COMPLEX JOB TESTS
# ============================================================================

def test_complex_job_nodes():
    """Test: Complex job has 7 nodes"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    components = ir.get('components', [])
    assert len(components) == 7, f"Expected 7 components, got {len(components)}"
    
    print("✅ Test 7: Complex job has 7 nodes")

def test_complex_job_connections():
    """Test: Complex job has 12 connections"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    connections = ir.get('connections', [])
    assert len(connections) == 12, f"Expected 12 connections, got {len(connections)}"
    
    print("✅ Test 8: Complex job has 12 connections")

def test_complex_job_db_components():
    """Test: Complex job has database components"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    talend_comps = [c['talend_component'] for c in ir['components']]
    
    assert 'tDB2Input' in talend_comps, "Expected tDB2Input"
    assert 'tDB2Output' in talend_comps, "Expected tDB2Output"
    assert talend_comps.count('tDB2Input') == 3, "Expected 3 tDB2Input components"
    
    print("✅ Test 9: Complex job has database components")

def test_complex_job_lookup():
    """Test: Complex job has lookup component"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    lookup = next((c for c in ir['components'] if c['type'] == 'lookup'), None)
    assert lookup is not None, "Expected lookup component"
    assert lookup['name'] == 'Lookup_62'
    
    print("✅ Test 10: Complex job has lookup component")

def test_complex_job_lookup_pins():
    """Test: Complex job lookup has 4 input pins and 1 output pin"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    lookup = next(c for c in ir['components'] if c['name'] == 'Lookup_62')
    
    input_pins = lookup['schema']['input_pins']
    output_pins = lookup['schema']['output_pins']
    
    assert len(input_pins) == 3, f"Expected 3 input pins, got {len(input_pins)}"
    assert len(output_pins) == 1, f"Expected 1 output pin, got {len(output_pins)}"
    
    # Lookup has 36 columns
    assert len(output_pins[0]['columns']) == 36, f"Expected 36 columns in output"
    
    print("✅ Test 11: Complex job lookup has correct pins and columns")

def test_complex_job_job_parameters():
    """Test: Complex job extracts 4 job parameters"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    params = ir['job']['parameters']
    assert len(params) == 4, f"Expected 4 parameters, got {len(params)}"
    
    param_names = {p['name'] for p in params}
    expected = {'TEST_Param', 'STMT_START', 'STMT_END', 'PRODUCT_CODE'}
    assert param_names == expected, f"Unexpected parameter names: {param_names}"
    
    print("✅ Test 12: Complex job extracts 4 job parameters")

def test_complex_job_contexts():
    """Test: Complex job has context values"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    contexts = ir['job']['contexts']['default']
    assert 'STMT_START' in contexts, "Missing STMT_START context"
    assert contexts['STMT_START'] == '2016-03-01', f"Unexpected STMT_START value"
    
    print("✅ Test 13: Complex job has context values")

def test_complex_job_db_config():
    """Test: Complex job extracts database configuration"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    source = next(c for c in ir['components'] if c['name'] == 'SOURCE')
    config = source['configuration']
    
    # Should have connection parameters
    assert len(config) > 0, "Expected database configuration"
    
    print("✅ Test 14: Complex job extracts database configuration")

def test_complex_job_connector_context_params():
    """Test: Complex job preserves connector context params (parameterized values)"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    ijara = next(c for c in ir['components'] if c['name'] == 'IJARA_HEADER')
    context_params = ijara['talend_specific'].get('context_params', {})
    
    # Should have XMLProperties with parameterized values like #TEST_Param.$DB2_INSTANCE#
    if 'XMLProperties' in context_params:
        xml_props = context_params['XMLProperties']
        assert '#TEST_Param.' in xml_props, "Expected parameterized values in XMLProperties"
    
    print("✅ Test 15: Complex job preserves connector context params")

def test_complex_job_transformer():
    """Test: Complex job has transformer with TrxClassName"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    xfm = next(c for c in ir['components'] if c['name'] == 'xfm')
    assert xfm['type'] == 'transform'
    
    talend_spec = xfm.get('talend_specific', {})
    assert 'trx_class_name' in talend_spec, "Missing trx_class_name"
    assert 'V47S2' in talend_spec['trx_class_name']
    
    print("✅ Test 16: Complex job has transformer with TrxClassName")

def test_complex_job_transformations():
    """Test: Complex job extracts 94 transformations"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    total_transformations = 0
    for comp in ir['components']:
        for pin in comp['schema']['output_pins']:
            for col in pin['columns']:
                if col.get('transformation'):
                    total_transformations += 1
    
    assert total_transformations > 0, "Expected transformations to be extracted"
    
    print(f"✅ Test 17: Complex job extracts transformations ({total_transformations} found)")

def test_metadata_info():
    """Test: IR metadata_info is populated correctly"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    metadata = ir['metadata_info']
    assert metadata['total_components'] == 3, "Incorrect total_components"
    assert metadata['total_connections'] == 3, "Incorrect total_connections"
    
    print("✅ Test 18: IR metadata_info populated correctly")

def test_ir_schema_structure():
    """Test: IR has schemas for each component"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    schemas = ir.get('schemas', {})
    assert len(schemas) == 7, f"Expected 7 schemas, got {len(schemas)}"
    
    print("✅ Test 19: IR has schemas for each component")

def test_connection_consistency():
    """Test: All connections reference valid components"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    comp_ids = {c['id'] for c in ir['components']}
    
    for conn in ir['connections']:
        from_id = conn['from']['component_id']
        to_id = conn['to']['component_id']
        
        assert from_id in comp_ids, f"Connection from invalid component {from_id}"
        assert to_id in comp_ids, f"Connection to invalid component {to_id}"
    
    print("✅ Test 20: All connections reference valid components")

# ============================================================================
# RUN ALL TESTS
# ============================================================================

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("TESTING ASG → TALEND IR CONVERSION")
    print("="*70 + "\n")
    
    tests = [
        # Simple job tests
        test_simple_job_nodes,
        test_simple_job_connections,
        test_simple_job_talend_components,
        test_simple_job_file_path,
        test_simple_job_transformations,
        test_simple_job_trxgen,
        
        # Complex job tests
        test_complex_job_nodes,
        test_complex_job_connections,
        test_complex_job_db_components,
        test_complex_job_lookup,
        test_complex_job_lookup_pins,
        test_complex_job_job_parameters,
        test_complex_job_contexts,
        test_complex_job_db_config,
        test_complex_job_connector_context_params,
        test_complex_job_transformer,
        test_complex_job_transformations,
        
        # General tests
        test_metadata_info,
        test_ir_schema_structure,
        test_connection_consistency,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__}: {e}")
            failed += 1
    
    print("\n" + "="*70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("="*70 + "\n")
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
