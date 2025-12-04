import asyncio
import json
from sqlalchemy import text
from db import get_db


def read_ir(path):
    return json.load(open(path, "r"))

def get_nodes(ir_data):
    return ir_data.get("nodes", [])

def get_node_type_subtype(node):
    return node.get("type", ""), node.get("subtype", "")

async def get_mappings():
    ir_data = read_ir("new_ir.json")
    type_pairs = [get_node_type_subtype(n) for n in get_nodes(ir_data)]
    mappings = {}
    async for session in get_db():
        for ir_type, ir_subtype in type_pairs:

            query = text("""
                SELECT component
                FROM ir_property_mappings
                WHERE ir_type = :ir_type AND ir_subtype = :ir_subtype
                LIMIT 1
            """)

            result = await session.execute(
                query, {"ir_type": ir_type, "ir_subtype": ir_subtype}
            )
            row = result.first()
            mappings[(ir_type, ir_subtype)] = row[0] if row else None
            # print(f"{ir_type},{ir_subtype}  →  {row[0]}")

        break 
    return(mappings)

asyncio.run(get_mappings())
