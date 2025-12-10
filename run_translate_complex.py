import asyncio, json
from translation_service import TranslationService
from db import get_db

async def run():
    svc = TranslationService(db=get_db(), debug=True)
    with open('INERACTIVE_TEST_HEADER_DATA 1_talend_ir.json', 'r', encoding='utf-8') as f:
        ir = json.load(f)
    await svc.translate_logic(ir)

asyncio.run(run())
