import re
import json

class ASGNode:
    def __init__(self, node_id, name, type_name):
        self.id = node_id
        self.name = name
        self.type = type_name
        self.properties = {}

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "properties": self.properties
        }

class ASGEdge:
    def __init__(self, name, source_node_id, target_node_id):
        self.name = name
        self.source = source_node_id
        self.target = target_node_id
        self.schema = [] # List of columns

    def to_dict(self):
        return {
            "name": self.name,
            "source": self.source,
            "target": self.target,
            "schema": self.schema
        }

class DSXParser:
    def __init__(self, filepath):
        self.filepath = filepath
        self.raw_records = {} # Map of Identifier -> Record Dict
        self.nodes = {}
        self.edges = []

    def parse(self):
        """Main execution method"""
        self._read_and_structure_file()
        self._build_asg()
        return self.nodes, self.edges

    def _read_and_structure_file(self):
        """
        Parses the text file into a dictionary of records.
        Handles the nested BEGIN/END structure and Heredoc strings.
        """
        with open(self.filepath, 'r', encoding='cp1252', errors='ignore') as f:
            lines = f.readlines()

        current_stack = []
        
        # Regex to capture "Key Value" or "Key"
        kv_pattern = re.compile(r'^\s*([A-Za-z0-9_]+)\s*(?:(?:"([^"]*)")|(=+=+=+=)|(.+))?')

        in_heredoc = False
        heredoc_buffer = []
        heredoc_key = None

        for line in lines:
            line = line.strip()
            if not line: continue

            # 1. Handle Heredoc Strings (The =+=+=+= blocks)
            if in_heredoc:
                if line.endswith("=+=+=+="):
                    # End of heredoc
                    content = "\n".join(heredoc_buffer)
                    # Safely assign to current block
                    if current_stack:
                        current_stack[-1][heredoc_key] = content
                    in_heredoc = False
                    heredoc_buffer = []
                    heredoc_key = None
                else:
                    heredoc_buffer.append(line)
                continue

            # 2. Optimization: Skip the compiled section entirely
            if line.startswith("BEGIN DSEXECJOB"):
                # We stop parsing here because we don't need the runtime artifacts
                break 

            # 3. Handle Block Start
            if line.startswith("BEGIN"):
                # Format: BEGIN [TYPE]
                parts = line.split()
                block_type = parts[1] if len(parts) > 1 else "UNKNOWN"
                new_block = {"_block_type": block_type}
                
                if not current_stack:
                    # Root Level Block (e.g., HEADER or DSJOB)
                    current_stack.append(new_block)
                else:
                    # Nested Block (e.g., DSRECORD inside DSJOB)
                    parent = current_stack[-1]
                    
                    # Initialize subrecords list if not present
                    if "subrecords" not in parent:
                        parent["subrecords"] = []
                    
                    parent["subrecords"].append(new_block)
                    current_stack.append(new_block)
                
                continue

            # 4. Handle Block End
            if line.startswith("END"):
                if current_stack:
                    finished_block = current_stack.pop()
                    
                    # If this was a DSRECORD (a logical object), store it in our main map
                    if finished_block.get("_block_type") == "DSRECORD":
                        ident = finished_block.get("Identifier")
                        if ident:
                            self.raw_records[ident] = finished_block
                continue

            # 5. Handle Key-Values
            # We only process properties if we are actually inside a block
            if not current_stack:
                continue

            match = kv_pattern.match(line)
            if match:
                key = match.group(1)
                val_quoted = match.group(2)
                is_heredoc = match.group(3)
                val_raw = match.group(4)

                if is_heredoc:
                    in_heredoc = True
                    heredoc_key = key
                elif val_quoted is not None:
                    current_stack[-1][key] = val_quoted
                elif val_raw is not None:
                    current_stack[-1][key] = val_raw.strip()
                else:
                    # Boolean flags or empty keys
                    current_stack[-1][key] = True

    def _build_asg(self):
        """
        Converts raw records into ASG Nodes and Edges.
        """
        
        # 1. Identify Nodes (Stages)
        for ident, record in self.raw_records.items():
            # OLEType usually denotes the object class.
            ole_type = record.get("OLEType", "")
            
            # We look for records that represent Stages (excluding the Job definition itself)
            if "Stage" in ole_type:
                name = record.get("Name", "Unknown")
                stage_type = record.get("StageType", "Unknown")
                
                node = ASGNode(ident, name, stage_type)
                
                # Extract properties from subrecords
                if "subrecords" in record:
                    for sub in record["subrecords"]:
                        if sub.get("_block_type") == "DSSUBRECORD":
                            prop_name = sub.get("Name")
                            prop_val = sub.get("Value")
                            if prop_name and prop_val:
                                node.properties[prop_name] = prop_val
                                
                self.nodes[ident] = node

        # 2. Identify Edges (Links)
        # Map Pins to Stages to determine source/target
        pin_to_stage_map = {}
        
        for ident, record in self.raw_records.items():
            if "Stage" in record.get("OLEType", ""):
                # Get Input Pins
                inputs = record.get("InputPins", "").split("|")
                for p in inputs: 
                    if p: pin_to_stage_map[p] = ident
                
                # Get Output Pins
                outputs = record.get("OutputPins", "").split("|")
                for p in outputs: 
                    if p: pin_to_stage_map[p] = ident

        # Now, process Edges by looking at pins that are OUTPUTS
        for pin_id, stage_id in pin_to_stage_map.items():
            pin_record = self.raw_records.get(pin_id)
            if not pin_record: continue
            
            stage_record = self.raw_records.get(stage_id)
            output_pins_list = stage_record.get("OutputPins", "").split("|")
            
            # If this pin is an output of the current stage, it's the start of an edge
            if pin_id in output_pins_list:
                partner_str = pin_record.get("Partner", "") # e.g., "V0S0|V0S0P1"
                link_name = pin_record.get("Name", "Unknown")
                
                if "|" in partner_str:
                    target_stage_id, target_pin_id = partner_str.split("|")
                    
                    # Create Edge
                    edge = ASGEdge(link_name, stage_id, target_stage_id)
                    
                    # Extract Schema (Columns) from the Pin definition
                    if "subrecords" in pin_record:
                        for sub in pin_record["subrecords"]:
                            # Columns are typically subrecords. 
                            # In modern DSX they are "COutputColumn", in older ones just generic SUBRECORD
                            if sub.get("_block_type") == "COutputColumn": 
                                col_name = sub.get("Name")
                                col_type = sub.get("SqlType") 
                                if col_name:
                                    edge.schema.append(f"{col_name} (Type: {col_type})")
                            elif sub.get("SqlType"):
                                edge.schema.append(f"{sub.get('Name')} (Type: {sub.get('SqlType')})")
                    
                    self.edges.append(edge)

# --- EXECUTION ---

# Update the filename to match your local file
parser = DSXParser("dsx.txt.txt")
nodes, edges = parser.parse()

# --- PRINT OUTPUT ---

print(f"{'='*20} NODES (STAGES) {'='*20}")
for n_id, node in nodes.items():
    print(f"Node: {node.name}")
    print(f"  Type: {node.type}")
    print(f"  ID:   {node.id}")
    # Print only a few props to keep output clean
    print(f"  Props (First 3): {dict(list(node.properties.items())[:3])}...")
    print("-" * 30)

print(f"\n{'='*20} EDGES (LINKS) {'='*20}")
for edge in edges:
    source_name = nodes[edge.source].name if edge.source in nodes else edge.source
    target_name = nodes[edge.target].name if edge.target in nodes else edge.target
    print(f"Edge: {edge.name}")
    print(f"  Flow: {source_name} -> {target_name}")
    print(f"  Schema: {edge.schema}")
    print("-" * 30)




import xml.etree.ElementTree as ET
import uuid
from datetime import datetime

# ==========================================
# 1. INTERMEDIATE REPRESENTATION (IR) CLASSES
# ==========================================

class IRNode:
    def __init__(self, uid, name, component_type, original_props=None):
        self.uid = uid
        self.name = name
        self.component_type = component_type  # Generic type: e.g., "FileReader", "XMLComposer"
        self.properties = original_props or {}
        self.metadata_columns = []  # List of {name, type}
        self.ui_x = 0
        self.ui_y = 0

    def to_dict(self):
        return {
            "uid": self.uid,
            "name": self.name,
            "type": self.component_type,
            "columns": self.metadata_columns,
            "ui": {"x": self.ui_x, "y": self.ui_y}
        }

class IRLink:
    def __init__(self, name, source_uid, target_uid):
        self.name = name
        self.source = source_uid
        self.target = target_uid

# ==========================================
# 2. CONVERTER: ASG -> IR
# ==========================================

class ASGToIRConverter:
    def __init__(self, nodes, edges):
        self.asg_nodes = nodes
        self.asg_edges = edges
        self.ir_nodes = []
        self.ir_links = []
    
    def convert(self):
        # 1. Convert Nodes and Map Types
        for nid, asg_node in self.asg_nodes.items():
            
            # Determine if this node is a Source or Target based on edges
            is_source = any(e.source == nid for e in self.asg_edges)
            is_target = any(e.target == nid for e in self.asg_edges)
            has_incoming = any(e.target == nid for e in self.asg_edges)

            ir_type = "Unknown"
            
            # Simple Heuristics for Mapping DataStage -> Generic IR
            ds_type = asg_node.type
            
            if "SequentialFile" in ds_type:
                if has_incoming:
                    ir_type = "FileWriter"
                else:
                    ir_type = "FileReader"
            elif "XMLStage" in ds_type:
                ir_type = "XMLComposer" # Simplifying assumption for this file
            else:
                ir_type = "GenericTransform"

            ir_node = IRNode(nid, asg_node.name, ir_type, asg_node.properties)
            self.ir_nodes.append(ir_node)

        # 2. Convert Links and Hydrate Metadata
        for edge in self.asg_edges:
            link = IRLink(edge.name, edge.source, edge.target)
            self.ir_links.append(link)
            
            # Propagate Schema to the Source Node's metadata
            # In Talend, the schema is defined on the component
            source_node = next((n for n in self.ir_nodes if n.uid == edge.source), None)
            if source_node:
                for schema_item in edge.schema:
                    # Parse "colname (Type: 12)"
                    parts = schema_item.split(" (Type: ")
                    col_name = parts[0]
                    # Map DataStage SQLType 12 (Varchar) to String
                    col_type = "id_String" 
                    source_node.metadata_columns.append({"name": col_name, "type": col_type})

        # 3. Apply Auto-Layout (Simple Grid)
        x_pos = 100
        y_pos = 100
        for node in self.ir_nodes:
            node.ui_x = x_pos
            node.ui_y = y_pos
            x_pos += 400  # Move right
        
        return self.ir_nodes, self.ir_links

# ==========================================
# 3. GENERATOR: IR -> TALEND XML (.item)
# ==========================================

class TalendGenerator:
    def __init__(self, ir_nodes, ir_links, job_name="MigratedJob"):
        self.nodes = ir_nodes
        self.links = ir_links
        self.job_name = job_name
        # Namespaces
        self.ns = {
            "talendfile": "platform:/resource/org.talend.model/model/TalendFile.xsd",
            "xmi": "http://www.omg.org/XMI"
        }

    def generate_xml(self):
        root = ET.Element("talendfile:ProcessType", {
            "xmi:version": "2.0",
            "xmlns:xmi": self.ns["xmi"],
            "xmlns:talendfile": "platform:/resource/org.talend.model/model/TalendFile.xsd",
            "defaultContext": "Default",
            "jobType": "Standard"
        })

        # --- Generate Node Components ---
        for node in self.nodes:
            talend_comp = self._map_component(node.component_type)
            
            # <node> element
            node_elem = ET.SubElement(root, "node", {
                "componentName": talend_comp,
                "componentVersion": "0.101",
                "offsetLabelX": "0",
                "offsetLabelY": "0",
                "posX": str(node.ui_x),
                "posY": str(node.ui_y)
            })

            # <elementParameter> (Configuration)
            self._add_param(node_elem, "TEXT", "UNIQUE_NAME", node.name)
            
            # Handle File Paths
            if node.component_type == "FileReader":
                # Clean up DataStage param syntax [&"folder"] -> context.folder
                raw_path = node.properties.get("file ", "") # Note the space in DSX key
                clean_path = raw_path.replace('[&"', 'context.').replace('"]', '').replace('\\', '/')
                self._add_param(node_elem, "FILE", "FILENAME", clean_path)
                self._add_param(node_elem, "TEXT", "ROWSEPARATOR", '"\\n"')
                self._add_param(node_elem, "TEXT", "FIELDSEPARATOR", '","')

            # <metadata> (Schema)
            metadata = ET.SubElement(node_elem, "metadata", {"connector": "FLOW", "name": node.name})
            for col in node.metadata_columns:
                ET.SubElement(metadata, "column", {
                    "comment": "",
                    "key": "false",
                    "length": "255",
                    "name": col["name"],
                    "nullable": "true",
                    "pattern": "",
                    "precision": "0",
                    "sourceType": "",
                    "type": col["type"]
                })

        # --- Generate Connections ---
        for link in self.links:
            # Talend requires the UNIQUE_NAME of source and target
            source_node = next(n for n in self.nodes if n.uid == link.source)
            target_node = next(n for n in self.nodes if n.uid == link.target)

            ET.SubElement(root, "connection", {
                "connectorName": "FLOW",
                "label": link.name,
                "lineStyle": "0", # Main flow
                "metaname": source_node.name,
                "offsetLabelX": "0",
                "offsetLabelY": "0",
                "source": source_node.name,
                "target": target_node.name
            })

        return self._prettify(root)

    def _map_component(self, ir_type):
        """Maps IR generic types to Talend Component Names"""
        mapping = {
            "FileReader": "tFileInputDelimited",
            "FileWriter": "tFileOutputDelimited",
            "XMLComposer": "tWriteXMLField", # Approximating XML Composition
            "GenericTransform": "tJavaRow"
        }
        return mapping.get(ir_type, "tDummyRow")

    def _add_param(self, parent, field, name, value):
        ET.SubElement(parent, "elementParameter", {
            "field": field,
            "name": name,
            "value": str(value)
        })

    def _prettify(self, elem):
        """Return a pretty-printed XML string for the Element."""
        from xml.dom import minidom
        raw_string = ET.tostring(elem, 'utf-8')
        reparsed = minidom.parseString(raw_string)
        return reparsed.toprettyxml(indent="  ")

# ==========================================
# 4. EXECUTION ORCHESTRATOR
# ==========================================

# Assuming 'nodes' and 'edges' are available from your previous parser run
# If not, ensure the parser code is above this or imported.

print("\n>>> STARTING CONVERSION PIPELINE <<<\n")

# 1. Convert ASG to IR
ir_converter = ASGToIRConverter(nodes, edges)
ir_nodes, ir_links = ir_converter.convert()

print(f"Generated IR: {len(ir_nodes)} Nodes, {len(ir_links)} Links")
for n in ir_nodes:
    print(f"  [IR Node] {n.name} ({n.component_type})")

# 2. Convert IR to Talend XML
talend_gen = TalendGenerator(ir_nodes, ir_links, job_name="Migrated_XML_Pivot")
xml_content = talend_gen.generate_xml()

# 3. Output
output_filename = "Migrated_XML_Pivot.item"
with open(output_filename, "w") as f:
    f.write(xml_content)

print(f"\n[SUCCESS] Talend Job generated: {output_filename}")
print("You can verify the file content below (first 20 lines):\n")
print("\n".join(xml_content.split('\n')[:20]))