"""Quick test for tFileInputDelimited generation"""
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock

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
    
    # Load IR
    with open("new_ir1.json", "r") as f:
        ir = json.load(f)
    
    # Get mappings
    from translate import get_mappings
    mappings = get_mappings()
    
    print("Testing tFileInputDelimited generation...")
    
    result = await svc.fill_jinja_templates(ir=ir, mappings=mappings)
    print(f"\n✅ Generated: {result}")
    
    # Check the generated file
    item_file = "verynew_generated_jobs_new/VeryNewMigratedProjectNew/process/DataStage/SIMPLE_IR_JOB.item"
    try:
        with open(item_file, "r") as f:
            content = f.read()
        
        # Count tFileInputDelimited elements
        if "tFileInputDelimited" in content:
            print("\n✅ tFileInputDelimited found in generated file")
            
            # Check for key parameters
            checks = [
                ("FILENAME", "File path"),
                ("FIELDSEPARATOR", "Field separator"),
                ("ROWSEPARATOR", "Row separator"),
                ("TRIMSELECT", "TRIMSELECT table"),
                ("DECODE_COLS", "DECODE_COLS table"),
                ("REJECT", "REJECT metadata"),
                ("CSV_OPTION", "CSV option"),
                ("REMOVE_EMPTY_ROW", "Remove empty row"),
            ]
            
            for param, desc in checks:
                if param in content:
                    print(f"  ✅ {desc} ({param}) - PRESENT")
                else:
                    print(f"  ❌ {desc} ({param}) - MISSING")
        else:
            print("❌ tFileInputDelimited not found!")
            
    except FileNotFoundError:
        print(f"❌ File not found: {item_file}")

if __name__ == "__main__":
    asyncio.run(main())
