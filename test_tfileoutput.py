"""Quick test for tFileOutputDelimited generation"""
import asyncio
import json
from unittest.mock import MagicMock

# Mock the db
class MockDB:
    async def execute(self, *args, **kwargs):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none = MagicMock(return_value=None)  # Simulate empty DB
        return mock_result

async def main():
    from translation_service import TranslationService
    
    # Create service with mock db
    svc = TranslationService(db=MockDB())
    
    # Create a simple IR with tFileOutputDelimited
    ir = {
        "job": {"id": "test_job", "name": "TestJob"},
        "nodes": [
            {
                "id": "n1",
                "type": "Sink",
                "subtype": "File",
                "name": "OUT_FILE",
                "props": {
                    "customType": "tFileOutputDelimited",
                    "configuration": {
                        "file": "output.csv",
                        "delimiter": ";"
                    },
                    "append": "true",
                    "include_header": "true"
                },
                "schemaRef": "s1"
            }
        ],
        "links": [],
        "schemas": {
            "s1": [{"name": "col1", "type": "string"}]
        }
    }
    
    # Mock mappings
    mappings = {"nodes": {}, "links": {}}
    
    print("Testing tFileOutputDelimited generation...")
    
    # We can't easily run fill_jinja_templates because of imports, 
    # so we'll test the internal methods directly
    
    # 1. Test _create_node_parameters (Fallback logic)
    print("\n1. Testing _create_node_parameters (Fallback logic)...")
    params = svc._create_node_parameters("tFileOutputDelimited", ir["nodes"][0]["props"])
    
    expected_params = {
        "FILENAME": '"output.csv"',
        "FIELDSEPARATOR": '";"',
        "APPEND": "true",
        "INCLUDEHEADER": "true"
    }
    
    for param in params:
        name = param.get("name")
        value = param.get("value")
        if name in expected_params:
            if value == expected_params[name]:
                print(f"  ✅ {name} = {value}")
            else:
                print(f"  ❌ {name} = {value} (Expected: {expected_params[name]})")
                
    # 2. Test _generate_tfileoutputdelimited_from_template (Template logic)
    print("\n2. Testing _generate_tfileoutputdelimited_from_template...")
    with open("d:/ETL_Migrator/componentTemplates/tFileOutputDelimited.xmlt", "r") as f:
        template_content = f.read()
        
    node = {
        "componentName": "tFileOutputDelimited",
        "uniqueName": "OUT_FILE",
        "posX": 100, "posY": 100,
        "props": ir["nodes"][0]["props"]
    }
    schema = ir["schemas"]["s1"]
    
    try:
        xml = svc._generate_tfileoutputdelimited_from_template(node, schema, template_content)
        if 'name="FILENAME" value="&quot;output.csv&quot;"' in xml:
            print("  ✅ Template generated correct FILENAME")
        else:
            print("  ❌ Template failed to generate correct FILENAME")
            
        if 'name="APPEND" value="true"' in xml:
            print("  ✅ Template generated correct APPEND")
        else:
            print("  ❌ Template failed to generate correct APPEND")
            
    except Exception as e:
        print(f"  ❌ Template generation failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
