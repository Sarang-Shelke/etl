import json
import os
import sys
import datetime
import uuid
import logging
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ASG_to_IR")

# --- Mappings ---

import xml.etree.ElementTree as ET

# --- Mappings ---

# Components that are strictly one type can stay here. 
# Bidirectional components (Connectors, Files) are handled dynamically.
COMPONENT_MAPPINGS = {
    'CTransformerStage': {'type': 'Transform', 'subtype': 'Map'},
    'PxLookup': {'type': 'Transform', 'subtype': 'Lookup'},
    'PxJoin': {'type': 'Transform', 'subtype': 'Join'},
    'PxFunnel': {'type': 'Transform', 'subtype': 'Merge'},
    'PxRemoveDup': {'type': 'Transform', 'subtype': 'Deduplicate'},
}

PROPERTY_MAPPINGS = {
    'FilePath': 'path',
    'FieldDelimiter': 'delimiter',
    'RowSeparator': 'row_separator',
    'FirstLineColumnNames': 'firstLineColumnNames',
    'HeaderLines': 'header_lines',
    'FooterLines': 'footer_lines',
    'RowLimit': 'row_limit',
    'RemoveEmptyRow': 'remove_empty_row',
    'DieOnError': 'die_on_error',
    'IncludeHeader': 'include_header',
    'Append': 'append',
    'Compress': 'compress',
    'AdvancedSeparator': 'advanced_separator',
    'Instance': 'instance',
    'Database': 'database_name',
    'Username': 'username',
    'Password': 'password',
    'TableName': 'table_name',
    'ConnectionString': 'connection_string',
    'VariantName': 'variant_name',
    'VariantLibrary': 'variant_library',
    'VariantVersion': 'variant_version',
}

class ASGToIRConverter:
    def __init__(self):
        self.node_map = {} # ASG ID -> IR ID
        self.nodes = []
        self.links = []
        self.schemas = {}
        self.node_counter = 0
        self.link_counter = 0

    def generate_ir_id(self, prefix="n"):
        val = f"{prefix}{self.node_counter}"
        self.node_counter += 1
        return val
        
    def generate_link_id(self):
        self.link_counter += 1
        return f"l{self.link_counter}"

    def convert(self, asg_file: str, output_file: str):
        logger.info(f"Loading ASG from {asg_file}")
        with open(asg_file, 'r', encoding='utf-8') as f:
            asg = json.load(f)

        job_name = asg.get("job_name", "Unknown_Job")
        asg_nodes = asg.get("nodes", [])
        asg_edges = asg.get("edges", [])
        
        # Pre-scan Edges for Connectivity Stats (Robustness for missing Pins)
        self.node_connectivity = {}
        for edge in asg_edges:
            src = edge.get("source_node") or edge.get("from_node")
            tgt = edge.get("target_node") or edge.get("to_node")
            
            if src:
                if src not in self.node_connectivity: self.node_connectivity[src] = {'in': 0, 'out': 0}
                self.node_connectivity[src]['out'] += 1
            if tgt:
                if tgt not in self.node_connectivity: self.node_connectivity[tgt] = {'in': 0, 'out': 0}
                self.node_connectivity[tgt]['in'] += 1

        # Pass 1: Create Nodes
        for node in asg_nodes:
            self._convert_single_node(node)

        # Pass 2: Create Links (Edges)
        if asg_edges:
             for edge in asg_edges:
                 self._convert_single_edge(edge)
        else:
            logger.info("No top-level edges found. Inferring links by Pin Name Matching...")
            self._infer_links_by_name(asg_nodes)

        # Pass 3: Finalize Structure
        ir = {
            "irVersion": "1.0",
            "generatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            "job": {
                "id": f"job-{job_name}-{datetime.datetime.now().strftime('%Y%m%d%H%M')}",
                "name": job_name
            },
            "nodes": self.nodes,
            "links": self.links,
            "schemas": self.schemas,
            "transformationTracking": self._generate_stats()
        }

        logger.info(f"Saving IR to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(ir, f, indent=2)
        
        logger.info("Conversion Complete.")

    def _is_source_node(self, asg_node: Dict[str, Any]) -> bool:
        """Check if node is a source"""
        # 1. Check Pins
        pins = asg_node.get('pins', [])
        pin_in = any(p.get('direction') == 'input' for p in pins)
        pin_out = any(p.get('direction') == 'output' for p in pins)
        
        if pin_in or pin_out:
            return pin_out and not pin_in
            
        # 2. Check Edges (Fallback)
        node_id = asg_node.get("id")
        stats = self.node_connectivity.get(node_id, {'in': 0, 'out': 0})
        return stats['out'] > 0 and stats['in'] == 0

    def _is_sink_node(self, asg_node: Dict[str, Any]) -> bool:
        """Check if node is a sink"""
        # 1. Check Pins
        pins = asg_node.get('pins', [])
        pin_in = any(p.get('direction') == 'input' for p in pins)
        pin_out = any(p.get('direction') == 'output' for p in pins)
        
        if pin_in or pin_out:
            return pin_in and not pin_out

        # 2. Check Edges (Fallback)
        node_id = asg_node.get("id")
        stats = self.node_connectivity.get(node_id, {'in': 0, 'out': 0})
        return stats['in'] > 0 and stats['out'] == 0

    def _determine_type(self, asg_node: Dict[str, Any]) -> Tuple[str, str]:
        enhanced_type = asg_node.get("enhanced_type", "")
        base_type = asg_node.get("type", "")
        
        # 1. Lookup by explicit mapping (Transformers, special stages)
        if enhanced_type in COMPONENT_MAPPINGS:
            return COMPONENT_MAPPINGS[enhanced_type]['type'], COMPONENT_MAPPINGS[enhanced_type]['subtype']
            
        # 2. Dynamic Detection for Bidirectional Components (DB, File, Custom)
        # DB Connectors
        if any(x in enhanced_type for x in ['DB2', 'ODBC', 'Oracle', 'SQL', 'Connector', 'TransactionalCustomStage']):
            if self._is_sink_node(asg_node):
                return 'Sink', 'Database'
            else:
                return 'Source', 'Database'
        
        # File Stages
        if any(x in enhanced_type for x in ['Sequential', 'File', 'CCustomStage']):
            # CCustomStage is generic, but often File or DB. Defaults to File if simple.
            if self._is_sink_node(asg_node):
                return 'Sink', 'File'
            elif self._is_source_node(asg_node):
                return 'Source', 'File'
            else:
                return 'Transform', 'Generic'

        # 3. Fallback Heuristic
        if "Transformer" in enhanced_type or "Transformer" in base_type:
             return 'Transform', 'Map'
        
        if self._is_sink_node(asg_node):
            return 'Sink', 'Generic'
        elif self._is_source_node(asg_node):
            return 'Source', 'Generic'
            
        return 'Transform', 'Generic'

    def _parse_xml_properties(self, xml_str: str) -> Dict[str, str]:
        """Parse XMLProperties CDATA section to extract key values"""
        try:
            # Extract content between CDATA markers
            if '<![CDATA[' in xml_str and ']]>' in xml_str:
                start = xml_str.find('<![CDATA[') + 9
                end = xml_str.find(']]>', start)
                if start > 8 and end > start:
                    xml_str = xml_str[start:end]
            
            # Simple wrapper if missing root
            if not xml_str.strip().startswith('<'):
                 return {}
                 
            # Try to parse as XML
            if xml_str.startswith('<?xml'):
                 # Skip definition
                 xml_str = xml_str[xml_str.find('?>')+2:]
                 
            # Wrap in root if multiple top level? XML usually has one root.
            # The properties string in DSX is often <Properties>...</Properties>
            
            root = ET.fromstring(xml_str)
            result = {}
            
            # Extract all text values
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    key = elem.tag
                    result[key] = elem.text.strip()
            return result
        except Exception as e:
            # logger.warning(f"XML parsing failed: {e}")
            return {}

    def _convert_single_node(self, asg_node: Dict[str, Any]):
        asg_id = asg_node.get("id")
        name = asg_node.get("name", "Unknown")
        
        logger.info(f"Converting Node: {name} ({asg_id})")
        
        ir_id = self.generate_ir_id()
        self.node_map[asg_id] = ir_id
        
        ir_type, ir_subtype = self._determine_type(asg_node)
        
        # Extract Properties
        props = {}
        config = asg_node.get("enhanced_properties", {}).get("configuration", {})
        
        # Parse XML Properties if present
        xml_props = config.get("XMLProperties") or config.get("XMLConnectorDescriptor")
        if xml_props and isinstance(xml_props, str) and ( "<" in xml_props or "CDATA" in xml_props):
             parsed_xml = self._parse_xml_properties(xml_props)
             # Merge into config for mapping? Or direct to props?
             # Let's direct to props with mappings
             for k, v in parsed_xml.items():
                 # Common keys in XML
                 if k == 'Instance': props['instance'] = v
                 elif k == 'Database': props['database_name'] = v
                 elif k == 'Username': props['username'] = v
                 elif k == 'Password': props['password'] = v
                 elif k == 'TableName': props['table_name'] = v
                 # Add others as needed or pass through
        
        # Generic Property Extraction
        for k, v in config.items():
            if v is None: continue # Skip nulls
            if k in ["XMLProperties", "XMLConnectorDescriptor"]: continue # Handled above
            
            # Map key if known, else usage as-is
            prop_key = PROPERTY_MAPPINGS.get(k, k) 
            
            # Handle Booleans (DSX "true"/"false" strings)
            if isinstance(v, str):
                if v.lower() == "true":
                    props[prop_key] = True
                elif v.lower() == "false":
                    props[prop_key] = False
                else:
                    props[prop_key] = v
            else:
                props[prop_key] = v

        # Fix specific missing props if needed (e.g. HeaderLines)
        # Assuming temp5.py puts them in configuration, loop above catches them.
        
        # Extract Schema & Transformations
        # Check output pins for Source/Transform, Input schema for Sink?
        # Actually usually it's the schema of the link 'originating' from the node, 
        # OR the schema of the link 'entering' the node (for sinks).
        # We will iterate PINS.
        
        schema_ref = f"s_{asg_id}" # Default schema ID for this node
        
        # Find the primary schema (usually the first output, or first input if sink)
        primary_columns = []
        
        pins = asg_node.get("pins", [])
        target_pin = None
        
        # Heuristic: Prefer Output pins for schema definition unless it's a Sink
        output_pins = [p for p in pins if p.get("direction") == "output"]
        input_pins = [p for p in pins if p.get("direction") == "input"]
        
        if output_pins:
            target_pin = output_pins[0]
        elif input_pins:
            target_pin = input_pins[0]
            
        if target_pin:
             # Extract columns
             # Check 'enhanced_schema' first (should contain transformation logic)
             cols = target_pin.get("enhanced_schema", [])
             if not cols:
                 cols = target_pin.get("schema", [])
                 
             primary_columns = self._convert_columns(cols)
             
        self.schemas[schema_ref] = primary_columns
        
        ir_node = {
            "id": ir_id,
            "type": ir_type,
            "subtype": ir_subtype,
            "name": name,
            "props": props,
            "transformationDetails": {
                 "hasTransformations": any(c.get("hasTransformation", False) for c in primary_columns),
                 "transformationType": "mixed" if primary_columns else "none",
                 "complexityScore": 0.0,
                 "transformationCount": len([c for c in primary_columns if c.get("hasTransformation")])
            },
            "schemaRef": schema_ref,
            "provenance": {
                "source": "dsx",
                "location": "original.dsx" 
            }
        }
        
        self.nodes.append(ir_node)

    def _convert_columns(self, asg_cols: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ir_cols = []
        for col in asg_cols:
            c = {
                "name": col.get("name"),
                "type": col.get("type", "string").lower(), # Normalize type
                "nullable": col.get("nullable", True),
                "hasTransformation": col.get("has_transformation", False),
                "sourceColumns": [],
                "functions": [],
                "expression": col.get("derivation", "") # Default derivation
            }
            
            # Detailed Transformation Logic
            logic = col.get("transformation_logic", {})
            if logic:
                c["transformationLogic"] = logic
                c["transformationClassification"] = logic.get("type", "simple")
                c["sourceColumns"] = logic.get("source_columns", [])
                c["functions"] = logic.get("functions", [])
                c["expression"] = logic.get("expression", c["expression"])
            
            ir_cols.append(c)
        return ir_cols

    def _extract_links_from_pins(self, asg_node: Dict[str, Any]):
        # This method is now Deprecated in favor of _infer_links_by_name
        # But we keep the call in convert() or better yet, change convert() to call the new method ONCE
        pass

    def _infer_links_by_name(self, asg_nodes: List[Dict[str, Any]]):
        """
        Infers links by matching Output Pins to Input Pins with the same 'name'.
        DataStage jobs often define links as named entities connecting stages.
        """
        # 1. Collect all pins by name
        # link_name -> {'source': (node_id, pin_name), 'targets': [(node_id, pin_name)]}
        connections = {}
        
        for node in asg_nodes:
            node_id = node.get("id")
            ir_node_id = self.node_map.get(node_id)
            if not ir_node_id: continue
            
            for pin in node.get("pins", []):
                pin_name = pin.get("name")
                direction = pin.get("direction", "").lower()
                
                if not pin_name: continue
                
                if pin_name not in connections:
                    connections[pin_name] = {'source': None, 'targets': []}
                
                if direction == 'output':
                    if connections[pin_name]['source']:
                        logger.warning(f"Duplicate source for link '{pin_name}': {node_id} and {connections[pin_name]['source'][0]}")
                    connections[pin_name]['source'] = (node_id, ir_node_id)
                elif direction == 'input':
                    connections[pin_name]['targets'].append((node_id, ir_node_id))
        
        # 2. Generate Links for complete pairs
        for link_name, info in connections.items():
            source = info['source']
            targets = info['targets']
            
            if source and targets:
                # We have a valid connection
                src_asg_id, src_ir_id = source
                
                for tgt_asg_id, tgt_ir_id in targets:
                    link = {
                        "id": self.generate_link_id(),
                        "from": {"nodeId": src_ir_id, "port": "out"}, # Generic port
                        "to": {"nodeId": tgt_ir_id, "port": "in"},
                        "schemaRef": f"s_{src_asg_id}" # Schema usually defined at source
                        # Note: In DSX, the pinned schema might differ, but usually the Link itself carries the schema.
                    }
                    self.links.append(link)
                    logger.info(f"Inferred Link '{link_name}': {src_asg_id} -> {tgt_asg_id}")
            else:
                # Partial link (orphaned)
                if not source and targets:
                     logger.debug(f"Link '{link_name}' has targets {targets} but no source.")
                elif source and not targets:
                     logger.debug(f"Link '{link_name}' has source {source} but no targets.")

    def _convert_single_edge(self, edge: Dict[str, Any]):
        source_id = edge.get("source_node") or edge.get("from_node")
        target_id = edge.get("target_node") or edge.get("to_node")
        
        if not source_id or not target_id:
            return

        ir_source = self.node_map.get(source_id)
        ir_target = self.node_map.get(target_id)
        
        if not ir_source or not ir_target:
            # This is common if we filtered out some nodes (e.g. annotations)
            # logger.warning(f"Skipping link {source_id}->{target_id}: Node ID not found in map")
            return
            
        link = {
            "id": self.generate_link_id(),
            "from": {
                "nodeId": ir_source,
                "port": "out" # Generic port
            },
            "to": {
                "nodeId": ir_target,
                "port": "in"
            },
            "schemaRef": f"s_{source_id}" # Link schema usually matches source node's schema
        }
        self.links.append(link)
        logger.info(f"Created Link: {source_id} -> {target_id}")
    def _generate_stats(self):
        return {
            "totalTransformations": sum(n["transformationDetails"]["transformationCount"] for n in self.nodes),
            "transformationTypes": {},
            "complexityDistribution": {}
        }

def main():
    """Main execution with no parameters and no sys.argv."""

    # >>> Set your configuration here <<<
    asg_file = "simple_user_job.json"
    output_file = "simple_user_job_new_ir.json"
    debug = False
    # -----------------------------------

    if not os.path.exists(asg_file):
        logger.error(f"ASG file not found: {asg_file}")
        return False

    # Create converter (old class does not support debug flag)
    converter = ASGToIRConverter()

    try:
        converter.convert(asg_file, output_file)
    except Exception as e:
        logger.exception(f"Conversion failed: {e}")
        return False

    logger.info("Conversion finished successfully.")
    return True


if __name__ == "__main__":
    main()
