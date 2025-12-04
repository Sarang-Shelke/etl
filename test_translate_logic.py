"""Test translate_logic with a sample IR"""
import asyncio
import json
from translation_service import TranslationService

# Sample IR structure
sample_ir = {
    "jobs": [
        {
            "id": "job_1",
            "name": "CustomerETL",
            "nodes": [
                {
                    "id": "node_1",
                    "name": "CustomerInput",
                    "type": "Source",
                    "subtype": "File",
                    "properties": {
                        "filepath": "D:/data/customers.csv",
                        "delimiter": ","
                    }
                },
                {
                    "id": "node_2",
                    "name": "CustomerTransform",
                    "type": "Transform",
                    "subtype": "Map",
                    "properties": {
                        "mapping_expression": "concat(first_name, ' ', last_name)"
                    }
                },
                {
                    "id": "node_3",
                    "name": "CustomerOutput",
                    "type": "Sink",
                    "subtype": "Database",
                    "properties": {
                        "host": "localhost",
                        "database": "etl_db",
                        "table": "customers_output"
                    }
                }
            ],
            "links": [
                {
                    "from": {"nodeId": "node_1"},
                    "to": {"nodeId": "node_2"}
                },
                {
                    "from": {"nodeId": "node_2"},
                    "to": {"nodeId": "node_3"}
                }
            ]
        }
    ]
}

async def main():
    print("Testing translate_logic with sample IR...")
    svc = TranslationService()
    
    try:
        result = await svc.translate_logic(sample_ir)
        print("\n✅ Success! Generated files:")
        for key, path in result.items():
            print(f"  {key}: {path}")
        
        # Verify files were created
        import os
        if os.path.exists(result["workspace"]):
            print(f"\n📁 Files in {result['workspace']}:")
            for file in os.listdir(result["workspace"]):
                file_path = os.path.join(result["workspace"], file)
                size = os.path.getsize(file_path)
                print(f"  - {file} ({size} bytes)")
                
                # Print first few lines of each file
                if file.endswith(('.xmlt', '.item', '.properties', '.project')):
                    print(f"\n    Preview of {file}:")
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()[:5]
                        for line in lines:
                            print(f"      {line.rstrip()}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
