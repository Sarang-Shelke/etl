import xml.etree.ElementTree as ET

def collect_tags(elem, tags=None, level=0):
    if tags is None:
        tags = []
    tags.append([elem.tag, level])
    for child in elem:
        collect_tags(child, tags, level + 1)
    return tags


from collections import defaultdict

class Graph:
    def __init__(self):
        self.nodes = {}          # {node_id: {'type':..., 'props':..., 'level':...}}
        self.adjacency = defaultdict(list)  # {node_id: [connected_node_id, ...]}

    def add_node(self, node_id, node_type, props=None, level=None):
        self.nodes[node_id] = {'type': node_type, 'props': props or {}, 'level': level}

    def add_edge(self, from_id, to_id):
        self.adjacency[from_id].append(to_id)

def build_asg_from_dsx(root):
    graph = Graph()

    # Find all stages and add as nodes
    for stage in root.findall(".//Stage"):
        name = stage.findtext("Name", default="UNKNOWN")
        stype = stage.findtext("StageType", default="Generic")
        properties = {}
        # Collect various properties, fields, etc.
        props_elem = stage.find("Properties")
        if props_elem is not None:
            for prop in props_elem:
                # Example: collect fields, filepath, input/output, transformations, etc.
                if prop.tag == "FieldDefinitions":
                    properties["Fields"] = [
                        {c.tag: c.text.strip() for c in field}
                        for field in prop.findall("Field")
                    ]
                elif prop.tag == "FilePath":
                    properties["FilePath"] = prop.text.strip()
                elif prop.tag in ("InputFields", "OutputFields"):
                    properties[prop.tag] = [f.text.strip() for f in prop.findall("Field")]
                elif prop.tag == "Transformations":
                    properties["Transformations"] = [
                        {child.tag: child.text.strip() for child in trans}
                        for trans in prop.findall("Transformation")
                    ]
                else:
                    # Add any other custom properties
                    properties[prop.tag] = prop.text.strip() if prop.text else None
        graph.add_node(name, stype, props=properties)

    # Find all links and add as edges
    for link in root.findall(".//Link"):
        from_stage = link.findtext("From", default=None)
        to_stage = link.findtext("To", default=None)
        if from_stage and to_stage:
            graph.add_edge(from_stage, to_stage)

    return graph

# ---------- Usage Example ----------
# Parse file and build everything
tree = ET.parse("datastage_sample 2.dsx")
root = tree.getroot()

# Step 1: List tags
tags = collect_tags(root)
print("Tags with levels:", tags)

# Step 2 & 3: Build ASG
asg = build_asg_from_dsx(root)
print("ASG Nodes:")
for nid, data in asg.nodes.items():
    print(f"{nid}: {data}")
print("ASG Edges:")
for from_id, tos in asg.adjacency.items():
    for to_id in tos:
        print(f"{from_id} --> {to_id}")
