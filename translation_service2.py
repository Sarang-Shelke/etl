import json
import re
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from uuid import uuid4
from jinja2 import Environment, FileSystemLoader
from translate import get_mappings

class TranslationService:
    def __init__(self):
        """Initialize TranslationService with Jinja2 environment for template rendering."""
        self.templates_dir = "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        # Define tuple-based IR to Talend component mappings
        self.ir_to_talend_mappings = {
            ('Source', 'Database'): 'tDBInput',
            ('Source', 'File'): 'tFileInputDelimited',
            ('Transform', 'Map'): 'tMap',
            ('Transform', 'Filter'): 'tFilterRow',
            ('Transform', 'Aggregate'): 'tAggregateRow',
            ('Sink', 'Database'): 'tDBOutput',
        }
    
    async def translate_logic(self, ir: Dict[str, Any]) -> Dict[str, str]:
        """
        Translate IR (Intermediate Representation) JSON to Talend job artifacts.
        
        Args:
            ir: IR dictionary containing jobs, nodes, links, etc.
        
        Returns:
            Dictionary with paths to generated files: {project, item, properties, workspace}
        """
        print(f"Starting IR to Talend translation...")
        
        # Get mappings
        mappings = await get_mappings()
        print(f"Found {len(mappings) if mappings else 0} mappings")
        
        # Process jobs from IR
        jobs = ir.get("jobs", [])
        if not jobs:
            raise ValueError("No jobs found in IR")
        
        # For now, process the first job
        job_ir = jobs[0]
        job_name = job_ir.get("name", "MigratedJob")
        print(f"Processing job: {job_name}")
        
        # Build Talend job structure from IR
        talend_job = self._build_talend_job_from_ir(job_ir, mappings)
        
        # Render and save Talend artifacts
        output_paths = await self._render_and_save_talend_artifacts(talend_job)
        
        print(f"Translation complete. Files saved to: {output_paths['workspace']}")
        return output_paths
    
    def _build_talend_job_from_ir(self, job_ir: Dict[str, Any], mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build Talend job structure from IR nodes and links."""
        nodes = job_ir.get("nodes", [])
        links = job_ir.get("links", [])
        
        # Note: mappings from get_mappings() may not be used here since we have
        # self.ir_to_talend_mappings already defined with tuple keys
        # If needed in future, can augment with mapping_lookup
        
        # Build Talend nodes
        talend_nodes = []
        pos_config = {"max_per_row": 3, "row_spacing": 200, "col_spacing": 250}
        
        for idx, node in enumerate(nodes):
            ir_type = node.get("type", "")
            ir_subtype = node.get("subtype", "")
            node_name = node.get("name", f"node_{idx}")
            
            # Find Talend component using tuple mapping
            talend_component = self.ir_to_talend_mappings.get(
                (ir_type, ir_subtype), "tUnknown"
            )
            
            print(f"  Node {idx}: {node_name} ({ir_type}/{ir_subtype}) → {talend_component}")
            
            # Calculate position
            row = idx // pos_config["max_per_row"]
            col = idx % pos_config["max_per_row"]
            pos_x = 100 + (col * pos_config["col_spacing"])
            pos_y = 100 + (row * pos_config["row_spacing"])
            
            # Build basic parameters from node properties
            params = self._create_node_parameters(talend_component, node.get("properties", {}))
            
            talend_node = {
                "componentName": talend_component,
                "componentVersion": "0.102",
                "uniqueName": node_name,
                "posX": pos_x,
                "posY": pos_y,
                "parameters": params,
            }
            talend_nodes.append(talend_node)
        
        # Build Talend connections from links
        talend_connections = []
        for link in links:
            from_node = link.get("from", {}).get("nodeId")
            to_node = link.get("to", {}).get("nodeId")
            if from_node and to_node:
                # Find node names by ID
                from_name = next((n.get("name") for n in nodes if n.get("id") == from_node), from_node)
                to_name = next((n.get("name") for n in nodes if n.get("id") == to_node), to_node)
                
                connection = {
                    "source": from_name,
                    "target": to_name,
                    "connectorName": "FLOW",
                    "label": f"row{from_name}",
                    "lineStyle": "0",
                    "metaname": from_name,
                }
                talend_connections.append(connection)
        
        return {
            "name": job_ir.get("name", "MigratedJob"),
            "nodes": talend_nodes,
            "connections": talend_connections,
        }
    
    def _create_node_parameters(self, component_type: str, properties: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create Talend element parameters from IR node properties."""
        params = [
            {"field": "TEXT", "name": "UNIQUE_NAME", "value": component_type, "show": False}
        ]
        
        # Map common properties based on component type
        if component_type == "tFileInputDelimited":
            if "filepath" in properties:
                params.append({
                    "field": "FILE", "name": "FILENAME",
                    "value": properties["filepath"], "show": True
                })
            if "delimiter" in properties:
                params.append({
                    "field": "TEXT", "name": "FIELDSEPARATOR",
                    "value": properties["delimiter"], "show": True
                })
        elif component_type in ("tDBInput", "tDBOutput"):
            if "host" in properties:
                params.append({
                    "field": "TEXT", "name": "HOST",
                    "value": properties["host"], "show": True
                })
            if "database" in properties:
                params.append({
                    "field": "TEXT", "name": "DBNAME",
                    "value": properties["database"], "show": True
                })
            if "table" in properties:
                params.append({
                    "field": "DBTABLE", "name": "TABLE",
                    "value": properties["table"], "show": True
                })
        elif component_type == "tMap":
            # tMap parameters are handled separately via metadata/nodeData
            pass
        
        return params
    
    async def _render_and_save_talend_artifacts(self, talend_job: Dict[str, Any]) -> Dict[str, str]:
        """Render Jinja templates and save Talend artifacts to populated_talend_files."""
        output_dir = "populated_talend_files"
        os.makedirs(output_dir, exist_ok=True)
        
        job_name = talend_job["name"]
        base_name = f"{job_name}_0.1"
        
        # Generate UUIDs for all template variables
        uuids = {f"uuid{i}": f"_{uuid4().hex}" for i in range(1, 12)}
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        
        # ===== Render project file =====
        project_tpl = self.jinja_env.get_template("talend.project.xmlt")
        project_ctx = {
            **uuids,
            "project_name": "MigratedProject",
            "project_label": "MigratedProject",
            "product_version": "8.0.1.20250218_0945-patch",
        }
        project_content = project_tpl.render(project_ctx)
        project_path = os.path.join(output_dir, "talend.project")
        with open(project_path, "w", encoding="utf-8") as f:
            f.write(project_content)
        print(f"  Rendered: {project_path}")
        
        # ===== Render item file =====
        item_tpl = self.jinja_env.get_template("talend_job.item.xmlt")
        
        # Convert nodes to dict with raw_xml for template
        nodes_with_xml = []
        for node in talend_job["nodes"]:
            node_xml = self._node_to_xml(node)
            nodes_with_xml.append({
                **node,
                "raw_xml": node_xml
            })
        
        # Convert connections to dict with parameters
        connections_with_params = []
        for conn in talend_job["connections"]:
            connections_with_params.append({
                "connectorName": conn.get("connectorName", "FLOW"),
                "label": conn.get("label", ""),
                "lineStyle": conn.get("lineStyle", "0"),
                "metaname": conn.get("metaname", ""),
                "source": conn.get("source", ""),
                "target": conn.get("target", ""),
                "parameters": conn.get("parameters", [])
            })
        
        item_ctx = {
            **uuids,
            "job_name": job_name,
            "job_version": "0.1",
            "job": {
                "name": job_name,
                "version": "0.1",
                "nodes": nodes_with_xml,
                "connections": connections_with_params,
                "subjobs": []
            }
        }
        item_content = item_tpl.render(item_ctx)
        item_path = os.path.join(output_dir, f"{base_name}.item")
        with open(item_path, "w", encoding="utf-8") as f:
            f.write(item_content)
        print(f"  Rendered: {item_path}")
        
        # ===== Render properties file =====
        props_tpl = self.jinja_env.get_template("talend_job.properties.xmlt")
        props_ctx = {
            **uuids,
            "label": job_name,
            "display_name": job_name,
            "product_version": "8.0.1.20250218_0945-patch",
            "created_date": timestamp,
            "modified_date": timestamp,
            "process_href": f"{base_name}.item#/",
        }
        props_content = props_tpl.render(props_ctx)
        props_path = os.path.join(output_dir, f"{base_name}.properties")
        with open(props_path, "w", encoding="utf-8") as f:
            f.write(props_content)
        print(f"  Rendered: {props_path}")
        
        return {
            "project": project_path,
            "item": item_path,
            "properties": props_path,
            "workspace": output_dir,
        }
    
    def _node_to_xml(self, node: Dict[str, Any]) -> str:
        """Convert a Talend node to XML element."""
        xml_lines = [
            f'  <node componentName="{node["componentName"]}" '
            f'componentVersion="{node["componentVersion"]}" '
            f'offsetLabelX="0" offsetLabelY="0" '
            f'posX="{node["posX"]}" posY="{node["posY"]}">'
        ]
        
        for param in node.get("parameters", []):
            show_str = "true" if param.get("show", False) else "false"
            xml_lines.append(
                f'    <elementParameter field="{param["field"]}" '
                f'name="{param["name"]}" value="{param["value"]}" show="{show_str}"/>'
            )
        
        xml_lines.append("  </node>")
        return "\n".join(xml_lines)
    
    def _connection_to_xml(self, conn: Dict[str, Any]) -> str:
        """Convert a Talend connection to XML element."""
        xml_lines = [
            f'  <connection connectorName="{conn["connectorName"]}" '
            f'label="{conn["label"]}" lineStyle="{conn["lineStyle"]}" '
            f'metaname="{conn["metaname"]}" source="{conn["source"]}" '
            f'target="{conn["target"]}">'
        ]
        
        # Add connection parameters if needed
        xml_lines.append("  </connection>")
        return "\n".join(xml_lines)
    
    async def _translate_job_with_llm(self, job: Dict[str, Any], mappings: List[Dict[str, Any]], templates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Translate a single DataStage job to Talend using LLM with template context"""
        job_name = job.get("name", "UnknownJob")
        
        # Create mapping lookup
        mapping_lookup = {m["datastage_name"]: m for m in mappings}
        
        # Process stages with LLM assistance
        talend_nodes = []
        talend_connections = []
        
        # Track component positions for dynamic layout
        last_component_pos = {"x": 100, "y": 100}  # Starting position
        component_spacing = {"x": 250, "y": 200}   # Spacing between components
        
        # Layout configuration for different flow patterns
        layout_config = {
            "max_components_per_row": 3,  # Maximum components in a row before wrapping
            "row_spacing": 200,           # Vertical spacing between rows
            "component_spacing": 250      # Horizontal spacing between components
        }
        
        # Apply layout strategy based on job complexity
        layout_config = self._select_layout_strategy(job, layout_config)
        
        for i, stage in enumerate(job.get("stages", [])):
            stage_name = stage.get("name", f"stage_{i}")
            stage_type = stage.get("type", "Unknown")
            stage_properties = stage.get("properties", {})
            
            print(f"DEBUG: Processing stage {i}: {stage_name} ({stage_type})")
            print(f"DEBUG: Stage properties keys: {list(stage_properties.keys())}")
            
            # Find Talend component for this stage
            talend_component = self._find_talend_component(stage, mapping_lookup)
            print(f"DEBUG: Mapped to Talend component: {talend_component}")
            
            if talend_component and talend_component != "tUnknown":
                # Get template for this component
                template_info = templates.get(talend_component, {})
                print(f"DEBUG: Found template for {talend_component}: {bool(template_info)}")
                
                # Generate Talend node with LLM assistance and dynamic positioning
                node = await self._create_talend_node_with_llm(
                    stage_name, stage_properties, talend_component, template_info, 
                    last_component_pos, component_spacing, layout_config, i
                )
                talend_nodes.append(node)
                print(f"DEBUG: Generated node with {len(node.get('parameters', []))} parameters")
                
                # Update last component position for next iteration
                last_component_pos = {"x": node["posX"], "y": node["posY"]}
                
                # Create connection to next node
                if i < len(job.get("stages", [])) - 1:
                    connection = self._create_connection(
                        stage_name, 
                        job.get("stages", [])[i + 1].get("name", f"stage_{i+1}")
                    )
                    talend_connections.append(connection)
            else:
                # Handle unknown components with fallback
                print(f"Warning: No mapping found for stage {stage_name} ({stage_type})")
                fallback_node = self._create_fallback_node(
                    stage_name, stage_type, last_component_pos, component_spacing, layout_config, i
                )
                talend_nodes.append(fallback_node)
                
                # Update last component position for next iteration
                last_component_pos = {"x": fallback_node["posX"], "y": fallback_node["posY"]}
        
        # Optimize the layout for better visual flow
        optimized_nodes = self._optimize_layout_for_flow(talend_nodes, layout_config)
        
        # Enhance connections with intelligent positioning based on actual node positions
        enhanced_connections = self._enhance_connections_with_intelligent_positioning(optimized_nodes, talend_connections)
        
        return {
            "name": job_name,
            "nodes": optimized_nodes,
            "connections": enhanced_connections,
            "metadata": job.get("metadata", {})
        }
    
    def _enhance_connections_with_intelligent_positioning(self, nodes: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enhance connections with intelligent positioning based on actual node positions"""
        enhanced_connections = []
        
        # Create a lookup for nodes by name
        node_lookup = {node["uniqueName"]: node for node in nodes}
        
        for connection in connections:
            source_name = connection["source"]
            target_name = connection["target"]
            
            # Get the actual node objects if they exist
            source_node = node_lookup.get(source_name)
            target_node = node_lookup.get(target_name)
            
            if source_node and target_node:
                # Create intelligent connection based on actual positions
                enhanced_connection = self._create_intelligent_connection(source_node, target_node)
                enhanced_connections.append(enhanced_connection)
            else:
                # Fallback to original connection if nodes not found
                enhanced_connections.append(connection)
        
        return enhanced_connections
    
    async def _create_talend_node_with_llm(self, stage_name: str, stage_properties: Dict[str, Any], 
                                          talend_component: str, template_info: Dict[str, Any], 
                                          last_component_pos: Dict[str, int], component_spacing: Dict[str, int], 
                                          layout_config: Dict[str, Any], position: int) -> Dict[str, Any]:
        """Create Talend node using LLM to generate appropriate properties with intelligent dynamic positioning"""
        
        # Build LLM prompt with context
        prompt = self._build_component_property_prompt(
            stage_name, stage_properties, talend_component, template_info
        )
        
        try:
            # Call OpenAI to generate properties
            response =  self.client.chat.completions.create(
                model=self.settings.default_model,
                messages=[
                    {"role": "system", "content": "You are an expert ETL migration specialist. Your task is to convert DataStage component properties to appropriate Talend component properties. You MUST return ONLY XML elementParameter tags, no JSON or other formats."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=4000
            )
            
            # Parse LLM response
            llm_response = response.choices[0].message.content
            print(f"DEBUG: Raw LLM response for {stage_name}: {llm_response[:200]}...")
            generated_properties = self._parse_llm_property_response(llm_response, template_info)
            
            print(f"DEBUG: LLM generated properties for {stage_name} -> {talend_component}")
            print(f"DEBUG: Properties: {generated_properties}")
            
        except Exception as e:
            print(f"Warning: LLM property generation failed for {stage_name}: {str(e)}")
            # Fallback to basic properties
            generated_properties = self._create_basic_properties(talend_component, stage_properties)
            print(f"DEBUG: Using fallback properties: {len(generated_properties)} properties")
        
        # Calculate intelligent position based on layout configuration
        pos_x, pos_y = self._calculate_component_position(
            position, last_component_pos, layout_config
        )
        
        # Create base node
        node = {
            "componentName": talend_component,
            "componentVersion": "0.102",  # Default version
            "uniqueName": stage_name,
            "posX": pos_x,
            "posY": pos_y,
            "parameters": generated_properties
        }
        
        # Add metadata and nodeData for complex components
        if talend_component == "tMap":
            node.update(await self._generate_tmap_metadata_and_nodedata(stage_name, stage_properties))
        elif talend_component == "tFileInputDelimited":
            node.update(await self._generate_fileinput_metadata(stage_name, stage_properties))
        elif talend_component == "tMysqlInput":
            node.update(await self._generate_database_metadata(stage_name, stage_properties))
        
        return node
    
    def _build_component_property_prompt(self, stage_name: str, stage_properties: Dict[str, Any], 
                                       talend_component: str, template_info: Dict[str, Any]) -> str:
        """Build comprehensive prompt for LLM property generation"""
        
        property_definitions = template_info.get("property_definitions", {})
        component_description = template_info.get("description", "No description available")
        component_category = template_info.get("category", "unknown")
        additional_context = template_info.get("additional_context", "")
        template_xml = template_info.get("template_xml", "")
        
        # Extract property examples from XML template
        xml_property_examples = self._extract_property_examples_from_xml(template_xml)
        
        prompt = f"""
You are an expert ETL migration specialist converting DataStage components to Talend components.

## DataStage Component Information:
- **Component Name**: {stage_name}
- **Component Properties**: {json.dumps(stage_properties, indent=2)}

## Target Talend Component:
- **Component Type**: {talend_component}
- **Category**: {component_category}
- **Description**: {component_description}
- **Additional Context**: {additional_context}

## Talend Component Template Structure:
```xml
{template_xml}
```

## Property Definitions:
{json.dumps(property_definitions, indent=2)}

## XML Property Examples:
{xml_property_examples}

## Task:
Convert the DataStage component properties to appropriate Talend component properties using the XML template as reference. Consider:
1. **Intelligent Property Mapping**: Use semantic understanding to map DataStage properties to Talend properties (e.g., "server" → "HOST", "database" → "DBNAME", "table" → "TABLE")
2. **Data Type Conversions**: Convert based on XML field types and property descriptions
3. **Context-Aware Values**: Extract meaningful values from DataStage properties
4. **Talend Requirements**: Ensure all required properties are included with appropriate values
5. **Use XML Template**: Follow the exact structure and field types from the XML template
6. **Component-Specific Logic**: For complex components like tMap, focus on generating the elementParameter tags only - metadata and nodeData will be handled separately

## Output Format:
Return ONLY the XML elementParameter tags with the structure exactly as in the template. Do NOT include any JSON, explanations, or other formatting.



## Guidelines:
- **Generate XML Only**: Return pure XML elementParameter tags, no JSON or other formats
- **Use LLM Intelligence**: Let the LLM determine the best property mappings based on semantic understanding
- **Follow XML Structure**: Use field types and structure from the XML template exactly
- **Include Required Properties**: Ensure all required properties from property_definitions are present
- **Context-Aware Values**: Extract and transform values from DataStage properties intelligently
- **Set Visibility**: Set "show" to false for internal/system properties, true for user-configurable ones
- **Validate Against Template**: Ensure generated properties match the XML template structure

Generate the Talend properties now in XML format:
"""
        return prompt
    
    def _extract_property_examples_from_xml(self, template_xml: str) -> str:
        """Extract property examples from XML template for LLM context"""
        import xml.etree.ElementTree as ET
        
        try:
            # Parse XML and extract elementParameter examples
            root = ET.fromstring(template_xml)
            examples = []
            
            for param in root.findall(".//elementParameter"):
                field = param.get("field", "")
                name = param.get("name", "")
                value = param.get("value", "")
                show = param.get("show", "false")
                
                examples.append(f"- {name}: field='{field}', value='{value}', show={show}")
            
            return "\n".join(examples[:10])  # Limit to first 10 examples
        except Exception as e:
            print(f"Warning: Failed to parse XML template: {str(e)}")
            return "XML parsing failed"
    
    def _parse_llm_property_response(self, response: str, template_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse LLM response to extract Talend properties from XML"""
        try:
            # Extract XML elementParameter tags from response
            import xml.etree.ElementTree as ET
            
            # Clean the response and extract XML content
            xml_content = self._extract_xml_from_response(response)
            print(f"DEBUG: Extracted XML content: {xml_content[:200]}...")
            
            if not xml_content:
                raise ValueError("No XML content found in LLM response")
            
            # Parse XML and extract elementParameter elements
            root = ET.fromstring(xml_content)
            properties = []
            
            for param in root.findall(".//elementParameter"):
                field = param.get("field", "TEXT")
                name = param.get("name", "")
                value = param.get("value", "")
                show = param.get("show", "true")
                
                # Convert show attribute to boolean
                show_bool = show.lower() == "true" if isinstance(show, str) else bool(show)
                
                properties.append({
                    "field": field,
                    "name": name,
                    "value": value,
                    "show": show_bool
                })
            
            print(f"DEBUG: Parsed {len(properties)} properties from XML")
            
            # Validate properties against template
            validated_properties = self._validate_properties_against_template(properties, template_info)
            
            # Ensure all properties have required fields
            final_properties = []
            for prop in validated_properties:
                if isinstance(prop, dict) and all(key in prop for key in ["field", "name", "value"]):
                    final_properties.append(prop)
                else:
                    print(f"DEBUG: Skipping invalid property: {prop}")
            
            print(f"DEBUG: Final validated properties: {len(final_properties)}")
            return final_properties
                
        except Exception as e:
            print(f"Warning: Failed to parse LLM response: {str(e)}")
            print(f"Raw response: {response}")
            # Fallback to basic properties - use a default component type
            return self._create_basic_properties("tUnknown", {})
    
    def _extract_xml_from_response(self, response: str) -> str:
        """Extract XML content from LLM response"""
        print(f"DEBUG: Attempting to extract XML from response of length {len(response)}")
        
        # Look for XML content between <node> tags or just elementParameter tags
        xml_patterns = [
            r'<node[^>]*>.*?</node>',  # Full node with content
            r'<elementParameter[^>]*>.*?</elementParameter>',  # Just elementParameter tags
            r'<elementParameter[^>]*/>'  # Self-closing elementParameter tags
        ]
        
        for i, pattern in enumerate(xml_patterns):
            matches = re.findall(pattern, response, re.DOTALL | re.IGNORECASE)
            if matches:
                print(f"DEBUG: Found XML with pattern {i}: {len(matches)} matches")
                # If we found elementParameter tags, wrap them in a node for parsing
                if 'elementParameter' in matches[0] and '<node' not in matches[0]:
                    result = f'<node>{matches[0]}</node>'
                    print(f"DEBUG: Wrapped elementParameter tags in node")
                    return result
                print(f"DEBUG: Returning XML match as-is")
                return matches[0]
        
        # If no XML found, try to extract content between backticks or code blocks
        code_block_patterns = [
            r'```xml\s*(.*?)\s*```',
            r'```\s*(.*?)\s*```',
            r'`(.*?)`'
        ]
        
        for i, pattern in enumerate(code_block_patterns):
            matches = re.findall(pattern, response, re.DOTALL)
            if matches:
                print(f"DEBUG: Found code block with pattern {i}: {len(matches)} matches")
                return matches[0]
        
        print(f"DEBUG: No XML content found in response")
        return ""
    
    async def _generate_tmap_metadata_and_nodedata(self, stage_name: str, stage_properties: Dict[str, Any]) -> Dict[str, Any]:
        """Generate metadata and nodeData for tMap components"""
        # Extract column information from stage properties
        columns = self._extract_columns_from_stage_properties(stage_properties)
        
        # Generate metadata for output
        metadata = {
            "connector": "FLOW",
            "name": "target",
            "columns": []
        }
        
        for col in columns:
            metadata["columns"].append({
                "comment": "",
                "key": "false",
                "length": "-1",
                "name": col.get("name", "unknown"),
                "nullable": "true",
                "pattern": "",
                "precision": "-1",
                "sourceType": col.get("type", "VARCHAR"),
                "type": self._map_datastage_type_to_talend(col.get("type", "VARCHAR")),
                "originalLength": "-1",
                "usefulColumn": "true"
            })
        
        # Generate nodeData
        nodeData = {
            "uiProperties": {
                "shellMaximized": "true"
            },
            "varTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "Var",
                    "minimized": "true"
                }
            ],
            "outputTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "target",
                    "expressionFilter": "",
                    "activateExpressionFilter": "false",
                    "columnNameFilter": "",
                    "mapperTableEntries": []
                }
            ],
            "inputTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "row1",
                    "matchingMode": "UNIQUE_MATCH",
                    "lookupMode": "LOAD_ONCE",
                    "mapperTableEntries": []
                }
            ]
        }
        
        # Add mapper table entries for output
        for col in columns:
            nodeData["outputTables"][0]["mapperTableEntries"].append({
                "name": col.get("name", "unknown"),
                "expression": f"row1.{col.get('name', 'unknown')}",
                "type": self._map_datastage_type_to_talend(col.get("type", "VARCHAR")),
                "nullable": "true"
            })
        
        # Add mapper table entries for input
        for col in columns:
            nodeData["inputTables"][0]["mapperTableEntries"].append({
                "name": col.get("name", "unknown"),
                "type": self._map_datastage_type_to_talend(col.get("type", "VARCHAR")),
                "nullable": "true"
            })
        
        return {
            "metadata": [metadata],
            "nodeData": nodeData
        }
    
    def _extract_columns_from_stage_properties(self, stage_properties: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract column information from DataStage stage properties"""
        columns = []
        
        # Try different possible locations for column information
        possible_column_locations = [
            "columns",
            "schema",
            "outputschema",
            "inputschema",
            "subrecords",
            "recorddefinitions"
        ]
        
        for location in possible_column_locations:
            if location in stage_properties:
                col_data = stage_properties[location]
                if isinstance(col_data, list):
                    columns.extend(col_data)
                elif isinstance(col_data, dict):
                    # If it's a dict, look for nested column lists
                    for key, value in col_data.items():
                        if isinstance(value, list) and key.lower() in ['columns', 'fields', 'attributes']:
                            columns.extend(value)
        
        # If no columns found, try to extract from subrecords
        if not columns and "subrecords" in stage_properties:
            subrecords = stage_properties["subrecords"]
            if isinstance(subrecords, dict):
                for subrecord_name, subrecord_data in subrecords.items():
                    if isinstance(subrecord_data, dict) and "columns" in subrecord_data:
                        columns.extend(subrecord_data["columns"])
        
        # If still no columns, create default columns based on common patterns
        if not columns:
            # Look for any property that might contain column-like information
            for key, value in stage_properties.items():
                if isinstance(value, str) and any(col_indicator in key.lower() for col_indicator in ['column', 'field', 'attribute']):
                    # Try to parse as comma-separated column names
                    if ',' in value:
                        col_names = [name.strip() for name in value.split(',')]
                        for col_name in col_names:
                            if col_name:
                                columns.append({
                                    "name": col_name,
                                    "type": "VARCHAR"  # Default type
                                })
        
        # If no columns found at all, create a default column
        if not columns:
            columns = [{"name": "default_column", "type": "VARCHAR"}]
        
        return columns
    
    def _map_datastage_type_to_talend(self, datastage_type: str) -> str:
        """Map DataStage data types to Talend data types"""
        type_mapping = {
            "VARCHAR": "id_String",
            "CHAR": "id_String",
            "STRING": "id_String",
            "INT": "id_Integer",
            "INTEGER": "id_Integer",
            "BIGINT": "id_Long",
            "LONG": "id_Long",
            "DOUBLE": "id_Double",
            "FLOAT": "id_Float",
            "DECIMAL": "id_BigDecimal",
            "DATE": "id_Date",
            "TIMESTAMP": "id_Date",
            "BOOLEAN": "id_Boolean",
            "BOOL": "id_Boolean"
        }
        
        return type_mapping.get(datastage_type.upper(), "id_String")
    
    async def _generate_fileinput_metadata(self, stage_name: str, stage_properties: Dict[str, Any]) -> Dict[str, Any]:
        """Generate metadata for file input components"""
        columns = self._extract_columns_from_stage_properties(stage_properties)
        
        metadata = {
            "connector": "FLOW",
            "name": "row1",
            "columns": []
        }
        
        for col in columns:
            metadata["columns"].append({
                "comment": "",
                "key": "false",
                "length": "-1",
                "name": col.get("name", "unknown"),
                "nullable": "true",
                "pattern": "",
                "precision": "-1",
                "sourceType": col.get("type", "VARCHAR"),
                "type": self._map_datastage_type_to_talend(col.get("type", "VARCHAR")),
                "originalLength": "-1",
                "usefulColumn": "true"
            })
        
        return {"metadata": [metadata]}
    
    async def _generate_database_metadata(self, stage_name: str, stage_properties: Dict[str, Any]) -> Dict[str, Any]:
        """Generate metadata for database input components"""
        columns = self._extract_columns_from_stage_properties(stage_properties)
        
        metadata = {
            "connector": "FLOW",
            "name": "row1",
            "columns": []
        }
        
        for col in columns:
            metadata["columns"].append({
                "comment": "",
                "key": "false",
                "length": "-1",
                "name": col.get("name", "unknown"),
                "nullable": "true",
                "pattern": "",
                "precision": "-1",
                "sourceType": col.get("type", "VARCHAR"),
                "type": self._map_datastage_type_to_talend(col.get("type", "VARCHAR")),
                "originalLength": "-1",
                "usefulColumn": "true"
            })
        
        return {"metadata": [metadata]}
    
    def _validate_properties_against_template(self, properties: List[Dict[str, Any]], 
                                           template_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate and enhance properties against template requirements"""
        required_properties = template_info.get("property_definitions", {})
        validated_properties = []
        
        # Add required properties that might be missing
        for prop_name, prop_info in required_properties.items():
            # Check if property already exists
            existing_prop = next((p for p in properties if p.get("name") == prop_name), None)
            
            if not existing_prop:
                # Add missing required property with default value
                validated_properties.append({
                    "field": prop_info.get("type", "TEXT"),
                    "name": prop_name,
                    "value": prop_info.get("default", ""),
                    "show": prop_info.get("show", False)
                })
            else:
                validated_properties.append(existing_prop)
        
        # Add any additional properties from LLM response
        for prop in properties:
            if not any(p.get("name") == prop.get("name") for p in validated_properties):
                validated_properties.append(prop)
        
        return validated_properties
    
    def _create_basic_properties(self, component_type: str, stage_properties: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create basic properties as fallback"""
        properties = []
        
        # Common property mappings
        property_mappings = {
            "tMysqlInput": {
                "host": "DB_HOST",
                "port": "DB_PORT", 
                "database": "DB_NAME",
                "username": "DB_USER",
                "password": "DB_PASSWORD"
            },
            "tFileInputDelimited": {
                "filename": "FILE_PATH",
                "fieldSeparator": "FIELD_SEPARATOR",
                "rowSeparator": "ROW_SEPARATOR"
            },
            "tMap": {
                "expression": "MAPPING_EXPRESSION"
            }
        }
        
        # Get mapping for this component
        mapping = property_mappings.get(component_type, {})
        
        # Create properties
        for talend_prop, datastage_prop in mapping.items():
            if datastage_prop in stage_properties:
                properties.append({
                    "field": "TEXT",
                    "name": talend_prop,
                    "value": str(stage_properties[datastage_prop]),
                    "show": False
                })
        
        # Add default properties
        properties.extend([
            {"field": "TEXT", "name": "UNIQUE_NAME", "value": f"{component_type}_1", "show": False}
        ])
        
        return properties
    
    def _calculate_component_position(self, position: int, last_component_pos: Dict[str, int], layout_config: Dict[str, Any]) -> tuple[int, int]:
        """Calculate intelligent position for a component based on layout configuration"""
        max_per_row = layout_config["max_components_per_row"]
        row_spacing = layout_config["row_spacing"]
        component_spacing = layout_config["component_spacing"]
        
        # Calculate which row this component should be in
        row_number = position // max_per_row
        
        # Calculate position within the row
        col_in_row = position % max_per_row
        
        # Calculate X position (horizontal) - center the row if it's not full
        row_width = min(max_per_row, (position % max_per_row) + 1) * component_spacing
        start_x = 100  # Base X position
        pos_x = start_x + (col_in_row * component_spacing)
        
        # Calculate Y position (vertical) - each row gets its own Y level
        # Add some variation to make the layout more interesting
        base_y = 100
        pos_y = base_y + (row_number * row_spacing)
        
        # Add slight vertical offset for components in the same row to avoid overlap
        if col_in_row > 0:
            pos_y += (col_in_row * 20)  # Small vertical offset within same row
        
        return pos_x, pos_y
    
    def _select_layout_strategy(self, job: Dict[str, Any], base_config: Dict[str, Any]) -> Dict[str, Any]:
        """Select the best layout strategy based on job complexity and characteristics"""
        stages = job.get("stages", [])
        num_stages = len(stages)
        
        # Copy base config
        config = base_config.copy()
        
        # Adjust layout based on number of stages
        if num_stages <= 3:
            # Small jobs: single row layout
            config["max_components_per_row"] = num_stages
            config["row_spacing"] = 150
            config["component_spacing"] = 200
        elif num_stages <= 6:
            # Medium jobs: 2-row layout
            config["max_components_per_row"] = 3
            config["row_spacing"] = 180
            config["component_spacing"] = 220
        elif num_stages <= 12:
            # Large jobs: 3-4 row layout
            config["max_components_per_row"] = 4
            config["row_spacing"] = 200
            config["component_spacing"] = 250
        else:
            # Very large jobs: compact layout
            config["max_components_per_row"] = 5
            config["row_spacing"] = 180
            config["component_spacing"] = 200
        
        # Check for special component types that might need different spacing
        has_complex_components = any(
            stage.get("type") in ["Transformer", "Aggregator", "Join"] 
            for stage in stages
        )
        
        if has_complex_components:
            # Complex components need more space
            config["component_spacing"] += 50
            config["row_spacing"] += 30
        
        return config
    
    def _optimize_layout_for_flow(self, nodes: List[Dict[str, Any]], layout_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Optimize the layout to create better visual flow between components"""
        if len(nodes) <= 1:
            return nodes
        
        # Create a more natural flow by adjusting positions
        optimized_nodes = []
        
        for i, node in enumerate(nodes):
            # Keep the original position but make small adjustments for better flow
            pos_x = node["posX"]
            pos_y = node["posY"]
            
            # If this is not the first component, ensure it's positioned relative to the previous one
            if i > 0:
                prev_node = nodes[i-1]
                prev_x, prev_y = prev_node["posX"], prev_node["posY"]
                
                # Ensure minimum spacing between consecutive components
                min_spacing = layout_config["component_spacing"]
                if abs(pos_x - prev_x) < min_spacing:
                    pos_x = prev_x + min_spacing
                
                # If components are in the same row, ensure they don't overlap
                if abs(pos_y - prev_y) < 50:  # Same row threshold
                    pos_y = prev_y + (i * 30)  # Small vertical offset
            
            # Create optimized node
            optimized_node = node.copy()
            optimized_node["posX"] = pos_x
            optimized_node["posY"] = pos_y
            optimized_nodes.append(optimized_node)
        
        return optimized_nodes

    def _create_fallback_node(self, stage_name: str, stage_type: str, last_component_pos: Dict[str, int], component_spacing: Dict[str, int], layout_config: Dict[str, Any], position: int) -> Dict[str, Any]:
        """Create a fallback node for unknown components with intelligent dynamic positioning"""
        # Calculate intelligent position based on layout configuration
        pos_x, pos_y = self._calculate_component_position(
            position, last_component_pos, layout_config
        )
        
        return {
            "componentName": "tUnknown",
            "componentVersion": "0.102",
            "uniqueName": stage_name,
            "posX": pos_x,
            "posY": pos_y,
            "parameters": [
                {"field": "TEXT", "name": "UNIQUE_NAME", "value": f"tUnknown_{stage_name}", "show": False},
                {"field": "TEXT", "name": "ORIGINAL_TYPE", "value": stage_type, "show": False}
            ]
        }
    
    def _find_talend_component(self, stage: Dict[str, Any], mapping_lookup: Dict[str, Any]) -> Optional[str]:
        """Find Talend component for DataStage stage"""
        stage_name = stage.get("name", "")
        
        # First try exact name match
        if stage_name in mapping_lookup:
            return mapping_lookup[stage_name]["talend_component"]
        
        # Try type-based mapping
        stage_type = stage.get("type", "")
        for mapping in mapping_lookup.values():
            if mapping.get("datastage_type") == stage_type:
                return mapping["talend_component"]
        
        # Default fallback
        return "tUnknown"
    
    def _create_connection(self, source: str, target: str) -> Dict[str, Any]:
        """Create Talend connection between components"""
        return {
            "connectorName": "FLOW",
            "label": f"row{source}",
            "lineStyle": "0",
            "metaname": f"{source}",
            "source": source,
            "target": target,
            "parameters": [
                {"field": "CHECK", "name": "MONITOR_CONNECTION", "value": "false"},
                {"field": "TEXT", "name": "UNIQUE_NAME", "value": f"row{source}", "show": False}
            ]
        }
    
    def _create_intelligent_connection(self, source_node: Dict[str, Any], target_node: Dict[str, Any]) -> Dict[str, Any]:
        """Create intelligent connection with better visual flow based on component positions"""
        source_name = source_node["uniqueName"]
        target_name = target_node["uniqueName"]
        
        # Determine connection style based on relative positions
        source_x, source_y = source_node["posX"], source_node["posY"]
        target_x, target_y = target_node["posX"], target_node["posY"]
        
        # Choose line style based on flow direction
        if target_y > source_y:  # Flow going down (next row)
            line_style = "2"  # Curved line for vertical flow
        elif target_x > source_x:  # Flow going right (same row)
            line_style = "0"  # Straight line for horizontal flow
        else:  # Flow going left or complex pattern
            line_style = "1"  # Dashed line for complex flow
        
        return {
            "connectorName": "FLOW",
            "label": f"flow_{source_name}_to_{target_name}",
            "lineStyle": line_style,
            "metaname": f"{source_name}_to_{target_name}",
            "source": source_name,
            "target": target_name,
            "parameters": [
                {"field": "CHECK", "name": "MONITOR_CONNECTION", "value": "false"},
                {"field": "TEXT", "name": "UNIQUE_NAME", "value": f"flow_{source_name}_to_{target_name}", "show": False}
            ]
        } 