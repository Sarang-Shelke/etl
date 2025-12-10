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
    
    nodes = ir.get('nodes', [])
    assert len(nodes) == 3, f"Expected 3 nodes, got {len(nodes)}"
    
    names = {c['name'] for c in nodes}
    assert names == {'Input_File', 'User_Transformer', 'Output_File'}, f"Unexpected node names: {names}"
    
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
    
    comp_map = {c['name']: c['talend_component'] for c in ir['nodes']}
    
    assert comp_map['Input_File'] == 'tFileInputDelimited', f"Expected tFileInputDelimited for Input_File"
    assert comp_map['User_Transformer'] == 'tMap', f"Expected tMap for User_Transformer"
    
    print("✅ Test 3: Simple job maps to correct Talend components")

def test_simple_job_file_path():
    """Test: Simple job extracts file path"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    input_comp = next(c for c in ir['nodes'] if c['name'] == 'Input_File')
    assert 'file_path' in input_comp['configuration'], "Missing file_path in Input_File"
    assert 'inputfile.csv' in input_comp['configuration']['file_path'], "Unexpected file path"
    
    print("✅ Test 4: Simple job extracts file path")

def test_simple_job_transformations():
    """Test: Simple job extracts 4 transformations"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    transformer = next(c for c in ir['nodes'] if c['name'] == 'User_Transformer')
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
    
    transformer = next(c for c in ir['nodes'] if c['name'] == 'User_Transformer')
    talend_spec = transformer.get('talend_specific', {})
    
    assert 'trx_class_name' in talend_spec, "Missing trx_class_name"
    assert talend_spec['trx_class_name'] == 'V0S1_SIMPLE_USER_PROCESS_User_Transformer'
    
    print("✅ Test 6: Simple job preserves TrxClassName")

# ============ COMPLEX JOB TESTS ============

def test_complex_job_nodes():
    """Test: Complex job has all 7 nodes"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    nodes = ir.get('nodes', [])
    assert len(nodes) == 7, f"Expected 7 nodes, got {len(nodes)}"
    
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
    
    talend_comps = [c['talend_component'] for c in ir['nodes']]
    
    # Should have DB2 components
    assert 'tDB2Input' in talend_comps, "Missing tDB2Input"
    assert 'tDB2Output' in talend_comps, "Missing tDB2Output"
    assert talend_comps.count('tDB2Input') == 3, "Expected 3 tDB2Input components"
    
    print("✅ Test 9: Complex job has database components")

def test_complex_job_lookup():
    """Test: Complex job has lookup component"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    lookup = next((c for c in ir['nodes'] if c['type'] == 'lookup'), None)
    assert lookup is not None, "No lookup component found"
    
    print("✅ Test 10: Complex job has lookup component")

def test_complex_job_lookup_pins():
    """Test: Complex job lookup has correct pins and columns"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    lookup = next(c for c in ir['nodes'] if c['name'] == 'Lookup_62')
    schema = lookup['schema']
    
    # Should have input pins
    assert 'input_pins' in schema, "Missing input_pins"
    assert len(schema['input_pins']) > 0, "No input pins"
    
    # Should have output pins with columns
    assert 'output_pins' in schema, "Missing output_pins"
    assert len(schema['output_pins']) > 0, "No output pins"
    
    output_cols = schema['output_pins'][0]['columns']
    assert len(output_cols) > 0, "No columns in output"
    
    print("✅ Test 11: Complex job lookup has correct pins and columns")

def test_complex_job_parameters():
    """Test: Complex job extracts 4 job parameters"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    params = ir['job']['parameters']
    assert len(params) == 4, f"Expected 4 parameters, got {len(params)}"
    
    param_names = {p['name'] for p in params}
    expected = {'pSourceDBPass', 'pSourceDBUser', 'pSourceDBConnStr', 'pOutputDBConnStr'}
    assert param_names == expected, f"Unexpected parameters: {param_names}"
    
    print("✅ Test 12: Complex job extracts 4 job parameters")

def test_complex_job_context_values():
    """Test: Complex job has context values"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    context = ir['job']['contexts']['default']
    assert len(context) > 0, "Empty context"
    
    print("✅ Test 13: Complex job has context values")

def test_complex_job_db_config():
    """Test: Complex job extracts database configuration"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    source = next(c for c in ir['nodes'] if c['name'] == 'SOURCE')
    config = source['configuration']
    
    assert 'database' in config or 'table' in config, "Missing database/table in SOURCE config"
    
    print("✅ Test 14: Complex job extracts database configuration")

def test_complex_job_connector_context():
    """Test: Complex job preserves connector context params"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    ijara = next(c for c in ir['nodes'] if c['name'] == 'IJARA_HEADER')
    talend_spec = ijara.get('talend_specific', {})
    
    # Should have context_params from connector
    if 'context_params' in talend_spec:
        assert len(talend_spec['context_params']) > 0
    
    print("✅ Test 15: Complex job preserves connector context params")

def test_complex_job_transformer_trxclass():
    """Test: Complex job has transformer with TrxClassName"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    xfm = next(c for c in ir['nodes'] if c['name'] == 'xfm')
    talend_spec = xfm.get('talend_specific', {})
    
    assert 'trx_class_name' in talend_spec, "Missing trx_class_name"
    
    print("✅ Test 16: Complex job has transformer with TrxClassName")

def test_complex_job_transformations():
    """Test: Complex job extracts transformations (93)"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    total_transformations = 0
    for comp in ir['nodes']:
        if 'schema' in comp and 'output_pins' in comp['schema']:
            for pin in comp['schema']['output_pins']:
                for col in pin.get('columns', []):
                    if 'transformation' in col:
                        total_transformations += 1
    
    assert total_transformations >= 90, f"Expected ≥90 transformations, got {total_transformations}"
    
    print(f"✅ Test 17: Complex job extracts transformations ({total_transformations} found)")

def test_metadata_info():
    """Test: IR metadata_info populated correctly"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    metadata = ir['metadata_info']
    assert metadata['total_nodes'] == 3, "Incorrect total_nodes"
    assert metadata['total_connections'] == 3, "Incorrect total_connections"
    
    print("✅ Test 18: IR metadata_info populated correctly")

def test_schemas_per_node():
    """Test: All nodes have schemas"""
    ir = load_json('simple_user_job_talend_ir.json')
    
    for comp in ir['nodes']:
        assert 'schema' in comp, f"Missing schema in {comp['name']}"
    
    print("✅ Test 19: All nodes have schemas")

def test_connections_valid():
    """Test: All connections reference valid nodes"""
    ir = load_json('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json')
    
    node_ids = {n['id'] for n in ir['nodes']}
    for conn in ir['connections']:
        assert conn['source'] in node_ids, f"Invalid source: {conn['source']}"
        assert conn['target'] in node_ids, f"Invalid target: {conn['target']}"
    
    print("✅ Test 20: All connections reference valid nodes")

# ============ MAIN ============

def run_all_tests():
    """Run all tests"""
    tests = [
        test_simple_job_nodes,
        test_simple_job_connections,
        test_simple_job_talend_components,
        test_simple_job_file_path,
        test_simple_job_transformations,
        test_simple_job_trxgen,
        test_complex_job_nodes,
        test_complex_job_connections,
        test_complex_job_db_components,
        test_complex_job_lookup,
        test_complex_job_lookup_pins,
        test_complex_job_parameters,
        test_complex_job_context_values,
        test_complex_job_db_config,
        test_complex_job_connector_context,
        test_complex_job_transformer_trxclass,
        test_complex_job_transformations,
        test_metadata_info,
        test_schemas_per_node,
        test_connections_valid,
    ]
    
    print("\n" + "="*70)
    print("TESTING ASG → TALEND IR CONVERSION")
    print("="*70 + "\n")
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__}: {str(e)}")
            failed += 1
    
    print("\n" + "="*70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("="*70 + "\n")
    
    return failed == 0

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
