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
        
        print(f"IR structure - keys: {list(ir.keys())}")
        if 'job' in ir:
            print(f"  - job name: {ir.get('job', {}).get('name', 'N/A')}")
        print(f"  - nodes: {len(ir.get('nodes', []))}")
        print(f"  - links: {len(ir.get('links', []))}")
        print(f"  - schemas: {len(ir.get('schemas', {}))}")
        print(f"\nNote: DB components (tDBInput, tDBOutput) will be filtered out during translation")
    
    translated = await svc.translate_logic(ir)
    # paths = svc.render_first_job(translated, output_base_dir="generated_jobs_test1")
    # print(paths)

asyncio.run(main())