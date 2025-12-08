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
    """Fetch all available (ir_type, ir_subtype) → component mappings from DB."""
    mappings = {}
    async for session in get_db():
        # Fetch ALL distinct type mappings from the database
        query = text("""
            SELECT DISTINCT ir_type, ir_subtype, component
            FROM ir_property_mappings
            WHERE ir_type IS NOT NULL 
              AND ir_subtype IS NOT NULL 
              AND component IS NOT NULL
        """)

        result = await session.execute(query)
        for row in result:
            ir_type, ir_subtype, component = row[0], row[1], row[2]
            # Only store the first component for each (ir_type, ir_subtype) pair
            if (ir_type, ir_subtype) not in mappings:
                mappings[(ir_type, ir_subtype)] = component
        
        break
    
    print(f"✅ Loaded {len(mappings)} component mappings from DB")
    return mappings


if __name__ == "__main__":
    asyncio.run(get_mappings())

