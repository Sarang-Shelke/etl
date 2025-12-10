# import asyncio
# import json

# from translation_service import TranslationService


# async def main() -> None:
#     service = TranslationService()

#     # Load IR JSON explicitly and call Weaver-style translate_logic(ir_data)
#     with open("new_ir.json", "r", encoding="utf-8") as infile:
#         ir_data = json.load(infile)

#     translated_logic = await service.translate_logic(ir_data)

#     # For now, render only the first Talend job to files (same as before)
#     first_job = translated_logic["jobs"][0]
#     paths = service._render_talend_artifacts(first_job, "generated_jobs")
#     print(paths)


# asyncio.run(main())

import asyncio
import json
from translation_service import TranslationService
from db import get_db

from translate import get_mappings

async def main() -> None:
    # Enable debug to get detailed diagnostics during IR->Talend generation
    svc = TranslationService(db=get_db(), debug=True)
    with open("simple_user_job_new_ir.json", "r", encoding="utf-8") as f:
        ir = json.load(f)
        
        # IR structure should be: { "irVersion": "...", "job": {...}, "nodes": [...], "links": [...] }
        # Check if IR already has the correct structure
        if isinstance(ir, dict):
            # If it has 'irVersion' or 'job' at top level, it's already correctly structured
            if 'job' in ir or 'irVersion' in ir:
                # Already correct structure, use as-is
                pass
            elif 'nodes' in ir or 'links' in ir:
                # Has nodes/links but no 'job' wrapper - add job info
                if 'name' in ir:
                    ir = {
                        "irVersion": ir.get("irVersion", "1.0"),
                        "job": {
                            "name": ir.get("name"),
                            "id": ir.get("id", "job_1"),
                            "version": ir.get("version", "0.1")
                        },
                        "nodes": ir.get("nodes", []),
                        "links": ir.get("links", []),
                        "schemas": ir.get("schemas", {})
                    }
                else:
                    # No name, create default job wrapper
                    ir = {
                        "irVersion": ir.get("irVersion", "1.0"),
                        "job": {"name": "IR_JOB", "id": "job_1", "version": "0.1"},
                        "nodes": ir.get("nodes", []),
                        "links": ir.get("links", []),
                        "schemas": ir.get("schemas", {})
                    }
        
        
        print(f"\nNote: DB components (tDBInput, tDBOutput) will be filtered out during translation")
    
    # translated = await svc.translate_logic(ir)
    mappings = await get_mappings()
    paths = await svc.fill_jinja_templates(ir, mappings, output_base_dir="verification_output")
    print(paths)

asyncio.run(main())