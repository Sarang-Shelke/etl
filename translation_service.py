# from openai import OpenAI
import json
import re
import os
import uuid
import zipfile
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
# from backend.db.models import ASG, TalendTemplate
# from backend.db.conf import get_settings
# from backend.schemas.translation import TranslateResponse
from translate import get_mappings
from uuid import UUID
from datetime import datetime

# --- Constants for Talend Versions (reused from ETL_Weaver) ---
TALEND_PRODUCT_VERSION = "8.0.1.20250218_0945-patch"
TALEND_PRODUCT_FULLNAME = "Talend Cloud Data Fabric"
TALEND_PROJECT_PRODUCT_VERSION = f"{TALEND_PRODUCT_FULLNAME}-{TALEND_PRODUCT_VERSION}"
PROJECT_TYPE = "DQ"
ITEMS_RELATION_VERSION = ""
MIGRATION_TASK_CLASS = "org.talend.repository.model.migration.CheckProductVersionMigrationTask"
MIGRATION_BREAKS_VERSION = "7.1.0"
MIGRATION_VERSION = "7.1.1"
AUTHOR_LOGIN = "etl.migrator@local"
USER_ID = f"_{uuid.uuid4().hex}"  # Single user ID for consistency

class TranslationService:
    def __init__(self, db: AsyncSession):
        self.db = db
        # self.settings = get_settings()
        # self.client = OpenAI(api_key=self.settings.openai_api_key, base_url=self.settings.llm_gateway_url)
    
    async def translate_logic(self, ir):
        """Translate DataStage logic to Talend using OpenAI and database templates"""
        # Get ASG
        # result = await self.db.execute(
        #     select(ASG).where(ASG.id == asg_id)
        # )
        # asg = result.scalar_one_or_none()
        
        # if not asg:
        #     raise ValueError(f"ASG {asg_id} not found")
        
        # Get reviewed mappings from ASG
        # mappings = asg.asg_json.get("reviewed_mappings", [])
        mappings = await get_mappings()
        print(f"Found {len(mappings)} reviewed mappings for IR")
        # print(f"ASG JSON keys: {list(asg.asg_json.keys())}")
        
        if not mappings:
            # Check if there are initial mappings that haven't been reviewed
            # initial_mappings = asg.asg_json.get("initial_mappings", [])
            # if initial_mappings:
                # raise ValueError(f"No reviewed mappings found for ASG {asg_id}. Found {len(initial_mappings)} initial mappings. Please review mappings first.")
            # else:
                # raise ValueError(f"No mappings found for ASG {asg_id}. Please parse and map the file first.")
            print("No mappings found for IR.")
        # Get Talend templates from database

        
        templates = await self._get_talend_templates()
        

        # print(f"DEBUG: Templates: {templates}")
        
        # Process each job in ASG
        # talend_jobs = []
        # for job in asg.asg_json.get("jobs", []):
        #     print(f"DEBUG: Processing job: {job.get('name', 'Unknown')}")
        #     print(f"DEBUG: Job stages: {len(job.get('stages', []))}")
        #     # talend_job = await self._translate_job_with_llm(job, mappings, templates)
        #     talend_jobs.append(talend_job)
        #     print(f"DEBUG: Generated Talend job with {len(talend_job.get('nodes', []))} nodes")
        
        await self.fill_jinja_templates(ir=ir, mappings=mappings)

        # # Prepare final translated logic
        # translated_logic = {
        #     "jobs": talend_jobs,
        #     "metadata": {
        #         "source": "datastage",
        #         "translated_at": datetime.now().isoformat(),
        #         "asg_id": str(asg_id),
        #         "translation_method": "llm_enhanced"
        #     }
        # }
        
        # # Update ASG with translated logic
        # asg_data = asg.asg_json.copy() if asg.asg_json else {}
        # asg_data["translated_logic"] = translated_logic
        # asg_data["translation_status"] = "completed"
        # asg_data["translated_at"] = datetime.now().isoformat()
        
        # # Force SQLAlchemy to detect the change
        # asg.asg_json = asg_data
        # self.db.add(asg)  # Explicitly add the object to the session
        # await self.db.commit()
        # await self.db.refresh(asg)
        
        # print(f"DEBUG: Translation completed and saved to ASG")
        # print(f"DEBUG: ASG keys after translation: {list(asg.asg_json.keys())}")
        # print(f"DEBUG: Translated logic keys: {list(translated_logic.keys())}")
        
        # return TranslateResponse(
        #     asg_id=asg.id,
        #     status="translated",
        #     translated_logic=translated_logic
        # )
    
    async def fill_jinja_templates(self, ir, mappings, output_base_dir: str = "new_generated_jobs_new", project_name: str = "NewMigratedProjectNew"):
        """Fill Jinja templates for Talend and create zip package (reused from ETL_Weaver structure)."""
        print("Filling templates and generating Talend artifacts")
        from jinja2 import Environment, FileSystemLoader

        env = Environment(loader=FileSystemLoader("templates"))
        item_template = env.get_template("talend_job.item.xmlt")
        properties_template = env.get_template("talend_job.properties.xmlt")
        project_template = env.get_template("talend.project.xmlt")

        # Get job information from IR
        job_name = ir.get('job', {}).get('name', 'IR_JOB')
        # Normalize job name - remove spaces, special characters, and numbers/versions
        job_name = job_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        # Remove version numbers and dots (e.g., "0.1", "1.0", etc.)
        job_name = re.sub(r'_\d+\.\d+$', '', job_name)  # Remove trailing _0.1, _1.0, etc.
        job_name = re.sub(r'\.\d+$', '', job_name)  # Remove trailing .0.1, .1.0, etc.
        # Remove any remaining numbers and dots that might cause issues
        job_name = re.sub(r'[^a-zA-Z0-9_]', '', job_name)  # Keep only alphanumeric and underscores
        # Use standard version format "0.1" for Talend metadata (not in filename)
        job_version = "0.1"
        # Use only job name for file basename (no version suffix)
        file_basename = job_name
        
        # Create directory structure similar to ETL_Weaver: output_base_dir/project_name/process/DataStage/
        workspace_dir = os.path.join(output_base_dir)
        project_dir = os.path.join(workspace_dir, project_name)
        process_dir = os.path.join(project_dir, "process")
        datastage_dir = os.path.join(process_dir, "DataStage")
        
        # Create all directories
        os.makedirs(datastage_dir, exist_ok=True)
        
        # 1. Generate the talend.project file at project level
        project_path = await self.fill_project_template(project_name, project_template, project_dir)
        
        # 2. Generate the .item file - pass the full IR with schemas
        item_content = await self.fill_item_template(ir, item_template, mappings)
        item_path = os.path.join(datastage_dir, f"{file_basename}.item")
        with open(item_path, "w", encoding="utf-8") as f:
            f.write(item_content)
        print(f"Generated .item file: {item_path}")
        
        # 3. Generate the .properties file (use file_basename to ensure consistency)
        properties_path = await self.fill_properties_template(job_name, job_version, properties_template, datastage_dir, file_basename)
        
        # 4. Create the zip package
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f"{project_name}_{run_id}.zip"
        zip_path = await self.create_zip_package(workspace_dir, zip_filename, output_base_dir)
        
        print(f"DEBUG: Generated project structure:")
        print(f"  Workspace Dir: {workspace_dir}")
        print(f"  Project Dir: {project_dir}")
        print(f"  Project File: {project_path}")
        print(f"  DataStage Dir: {datastage_dir}")
        print(f"  Item File: {item_path}")
        print(f"  Properties File: {properties_path}")
        print(f"  Zip File: {zip_path}")
        
        return {
            "project": project_path,
            "item": item_path,
            "properties": properties_path,
            "zip": zip_path,
            "workspace": workspace_dir,
        }

    async def fill_item_template(self, job, item_template, mappings):
        """Fill item template for a Talend job"""
        print(f"Filling item template for job")
        # Support two IR shapes:
        # 1) job is a wrapper: {'job': { ... }}
        # 2) job is already the inner job dict: { 'nodes': ..., 'links': ..., 'name': ... }
        if isinstance(job, dict) and 'job' in job and isinstance(job['job'], dict):
            job_body = job['job']
        else:
            job_body = job or {}

        job_name = job_body.get('name', 'UnknownJob')
        # Normalize job name - remove spaces, special characters, and numbers/versions
        job_name = job_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        # Remove version numbers and dots
        job_name = re.sub(r'_\d+\.\d+$', '', job_name)  # Remove trailing _0.1, _1.0, etc.
        job_name = re.sub(r'\.\d+$', '', job_name)  # Remove trailing .0.1, .1.0, etc.
        # Remove any remaining numbers and dots that might cause issues
        job_name = re.sub(r'[^a-zA-Z0-9_]', '', job_name)  # Keep only alphanumeric and underscores
        job_id = job_body.get('id', 'job_1')
        # Use standard version format "0.1" for Talend metadata (not in filename)
        job_version = "0.1"

        # Build Talend job structure from IR (pass inner job body)
        # Also check if nodes/links are at top level of IR
        if not job_body.get("nodes") and not job_body.get("links"):
            # Try top-level IR structure
            if isinstance(job, dict):
                if "nodes" in job or "links" in job:
                    job_body = job
                    print(f"DEBUG: Using top-level IR structure (nodes/links at root)")
        
        # Get schemas from the full IR structure (may be at top level or in job)
        schemas = {}
        if isinstance(job, dict):
            # Check if schemas are in the job wrapper or at top level
            if "schemas" in job:
                schemas = job.get("schemas", {})
            elif "schemas" in job_body:
                schemas = job_body.get("schemas", {})
        
        # Add schemas to job_body if not present
        if schemas and "schemas" not in job_body:
            job_body["schemas"] = schemas
        
        print(f"DEBUG: Building from job_body with {len(job_body.get('nodes', []))} nodes, {len(job_body.get('links', []))} links, and {len(schemas)} schemas")
        talend_job = self._build_talend_job_from_ir(job_body, mappings)
        print(f"DEBUG: Built Talend job with {len(talend_job.get('nodes', []))} nodes and {len(talend_job.get('connections', []))} connections")
        
        # Convert nodes to have raw_xml attribute for template
        nodes_with_xml = []
        for node in talend_job.get('nodes', []):
            if not node:
                continue
            node_copy = node.copy()
            raw_xml = self._node_to_xml(node)
            if not raw_xml or len(raw_xml.strip()) == 0:
                print(f"Warning: Node {node.get('uniqueName', 'unknown')} generated empty XML, skipping")
                continue
            node_copy['raw_xml'] = raw_xml
            nodes_with_xml.append(node_copy)
        
        # Ensure connections have proper structure with parameters
        connections_with_params = []
        for conn in talend_job.get('connections', []):
            if not conn:
                continue
            conn_copy = conn.copy()
            # Ensure connection has all required fields
            if 'connectorName' not in conn_copy:
                conn_copy['connectorName'] = "FLOW"
            if 'label' not in conn_copy:
                conn_copy['label'] = f"row{conn_copy.get('source', '')}"
            if 'lineStyle' not in conn_copy:
                conn_copy['lineStyle'] = "0"
            if 'metaname' not in conn_copy:
                conn_copy['metaname'] = conn_copy.get('source', '')
            # Ensure offsetLabelX and offsetLabelY are set
            if 'offsetLabelX' not in conn_copy:
                conn_copy['offsetLabelX'] = "0"
            if 'offsetLabelY' not in conn_copy:
                conn_copy['offsetLabelY'] = "0"
            # Ensure connection has parameters
            # MONITOR_CONNECTION should NOT have show attribute (omit it)
            # UNIQUE_NAME should have show="false"
            if 'parameters' not in conn_copy:
                conn_copy['parameters'] = [
                    {"field": "CHECK", "name": "MONITOR_CONNECTION", "value": "false"},  # No show attribute
                    {"field": "TEXT", "name": "UNIQUE_NAME", "value": conn_copy.get('label', f"row{conn_copy.get('source', '')}"), "show": False}
                ]
            connections_with_params.append(conn_copy)
        
        # Validate that we have at least some content
        if not nodes_with_xml and not connections_with_params:
            print("Warning: Job has no nodes or connections, generating empty job structure")

        # Generate subjob element (required by Talend)
        # Subjob typically references the first node
        subjobs = []
        if nodes_with_xml:
            first_node_name = nodes_with_xml[0].get('uniqueName', '')
            if first_node_name:
                subjobs.append({
                    'uniqueName': first_node_name,
                    'titleColor': '0;93;185',
                    'color': '0;93;185'
                })

        # Prepare template context
        template_context = {
            'job': {
                'id': job_id,
                'name': job_name,
                'version': job_version,
                'nodes': nodes_with_xml,
                'connections': connections_with_params,
                'subjobs': subjobs
            }
        }
        
        # Render the template
        rendered_content = item_template.render(**template_context)
        
        # Validate rendered content
        if not rendered_content or len(rendered_content.strip()) == 0:
            raise ValueError("Generated item file content is empty")
        
        # Ensure it starts with XML declaration
        if not rendered_content.strip().startswith('<?xml'):
            raise ValueError("Generated item file does not start with XML declaration")
        
        # Validate XML structure
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(rendered_content)
            print("DEBUG: XML validation passed")
            # Check for required elements
            if root.tag.endswith('ProcessType'):
                print("DEBUG: Root element is ProcessType (correct)")
            else:
                print(f"WARNING: Root element is {root.tag}, expected ProcessType")
        except ET.ParseError as e:
            print(f"ERROR: Generated XML is invalid: {e}")
            print(f"DEBUG: XML content preview (first 1000 chars):\n{rendered_content[:1000]}")
            # Try to find the line with the error
            error_line = getattr(e, 'lineno', 'unknown')
            print(f"DEBUG: XML parse error at line {error_line}")
            raise ValueError(f"Generated item file has invalid XML structure: {e}")
        
        print(f"Generated item content length: {len(rendered_content)} characters")
        print(f"Item file contains {len(nodes_with_xml)} nodes and {len(connections_with_params)} connections")
        
        return rendered_content

    async def fill_properties_template(self, job_name: str, job_version: str, properties_template, output_dir: str, file_basename: str = None) -> str:
        """Fill properties template for a Talend job (reused from ETL_Weaver)."""
        print(f"Filling properties template for job: {job_name}")
        
        # Generate unique IDs for the template variables
        uuid1 = f"_{uuid.uuid4().hex}"  # Property xmi:id
        uuid2 = f"_{uuid.uuid4().hex}"  # Property id
        uuid3 = f"_{uuid.uuid4().hex}"  # ProcessItem xmi:id and Property item
        uuid4 = f"_{uuid.uuid4().hex}"  # created_product_fullname xmi:id
        uuid5 = f"_{uuid.uuid4().hex}"  # created_product_version xmi:id
        uuid6 = f"_{uuid.uuid4().hex}"  # created_date xmi:id
        uuid7 = f"_{uuid.uuid4().hex}"  # modified_product_fullname xmi:id
        uuid8 = f"_{uuid.uuid4().hex}"  # modified_product_version xmi:id
        uuid9 = f"_{uuid.uuid4().hex}"  # modified_date xmi:id
        uuid10 = f"_{uuid.uuid4().hex}" # item_key xmi:id
        uuid11 = f"_{uuid.uuid4().hex}" # ItemState xmi:id

        if file_basename is None:
            file_basename = f"{job_name}_{job_version}"
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        
        render_context = {
            # UUID variables for template
            "uuid1": uuid1,
            "uuid2": uuid2,
            "uuid3": uuid3,
            "uuid4": uuid4,
            "uuid5": uuid5,
            "uuid6": uuid6,
            "uuid7": uuid7,
            "uuid8": uuid8,
            "uuid9": uuid9,
            "uuid10": uuid10,
            "uuid11": uuid11,
            
            # Content variables
            "label": job_name,
            "display_name": job_name,
            "version": "",  # Empty version to avoid "0.1" in display name
            "user_id": USER_ID,  # Use the consistent user ID
            "product_version": TALEND_PRODUCT_VERSION,
            "created_date": timestamp,
            "modified_date": timestamp,
            # "item_key": "",
            "process_href": f"{file_basename}.item#/",  # file_basename is just job_name without version
        }

        content = properties_template.render(render_context)
        file_path = os.path.join(output_dir, f"{file_basename}.properties")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        print(f"Generated .properties file: {file_path}")
        return file_path

    async def fill_project_template(self, project_name: str, project_template, output_dir: str) -> str:
        """Fill project template for Talend (reused from ETL_Weaver)."""
        print(f"Filling project template for project: {project_name}")
        
        # Generate consistent UUIDs for the project
        project_id = f"_{uuid.uuid4().hex}"
        migration_task_id = f"_{uuid.uuid4().hex}"
        
        render_context = {
            "project_id": project_id,
            "project_label": project_name,
            "project_technical_label": project_name.upper(),
            "author_id": USER_ID,  # Project's author attribute points to the User ID
            "product_version": TALEND_PROJECT_PRODUCT_VERSION,
            "project_type": PROJECT_TYPE,
            "items_relation_version": ITEMS_RELATION_VERSION,
            
            "migration_task_id": migration_task_id,
            "migration_task_class": MIGRATION_TASK_CLASS,
            "breaks_version": MIGRATION_BREAKS_VERSION,
            "migration_version": MIGRATION_VERSION,

            "user_id": USER_ID,  # The User element's own ID
            "user_login": AUTHOR_LOGIN,
        }
        
        content = project_template.render(render_context)
        file_path = os.path.join(output_dir, "talend.project")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        print(f"Generated talend.project file: {file_path}")
        return file_path

    async def create_zip_package(self, workspace_dir: str, zip_filename: str, output_base_dir: str = "generated_jobs1") -> str:
        """Creates a zip archive of the generated project directory, skipping .md files (reused from ETL_Weaver)."""
        print(f"Creating zip package: {zip_filename}")
        # Create target directory named `new_generated_job` under output_base_dir
        target_dir = os.path.join(output_base_dir, "new_generated_job")
        os.makedirs(target_dir, exist_ok=True)

        zip_path = os.path.join(target_dir, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Walk the workspace_dir and add files preserving relative paths
            for root, _, files in os.walk(workspace_dir):
                for file in files:
                    # Skip markdown files
                    if file.lower().endswith('.md'):
                        continue
                    file_path = os.path.join(root, file)
                    # Compute archive name relative to the workspace_dir
                    arcname = os.path.relpath(file_path, start=workspace_dir)
                    zipf.write(file_path, arcname)

        print(f"Created zip package: {zip_path}")
        return zip_path

    def _build_talend_job_from_ir(self, job_ir: Dict[str, Any], mappings: Dict[tuple, str]) -> Dict[str, Any]:
        """Build Talend job structure from IR nodes and links."""
        nodes = job_ir.get("nodes", [])
        links = job_ir.get("links", [])
        schemas = job_ir.get("schemas", {})  # Get schemas from IR
        
        # Note: mappings from get_mappings() may not be used here since we have
        # self.ir_to_talend_mappings already defined with tuple keys
        # If needed in future, can augment with mapping_lookup
        
        # Filter out DB components (tDBInput, tDBOutput) - only keep file-based and transform components
        # First, identify which nodes are DB components and should be excluded
        excluded_node_ids = set()
        filtered_nodes = []
        
        for node in nodes:
            ir_type = node.get("type", "")
            ir_subtype = node.get("subtype", "")
            talend_component = mappings.get((ir_type, ir_subtype), "tUnknown")
            
            # Skip DB components
            if talend_component in ("tDBInput", "tDBOutput"):
                excluded_node_ids.add(node.get("id"))
                print(f"  Excluding DB component: {node.get('name')} ({ir_type}/{ir_subtype}) → {talend_component}")
            else:
                filtered_nodes.append(node)
        
        # Update nodes list to only include non-DB components
        nodes = filtered_nodes
        print(f"  Filtered {len(job_ir.get('nodes', []))} nodes to {len(nodes)} (removed {len(excluded_node_ids)} DB components)")
        
        # Filter links to exclude those connected to DB components
        filtered_links = []
        for link in links:
            from_node_id = link.get("from", {}).get("nodeId")
            to_node_id = link.get("to", {}).get("nodeId")
            
            # Skip links that connect to/from excluded DB components
            if from_node_id in excluded_node_ids or to_node_id in excluded_node_ids:
                print(f"  Excluding link from {from_node_id} to {to_node_id} (connected to DB component)")
                continue
            
            filtered_links.append(link)
        
        links = filtered_links
        print(f"  Filtered {len(job_ir.get('links', []))} links to {len(links)} (removed links to DB components)")
        
        # Build Talend nodes
        talend_nodes = []
        pos_config = {"max_per_row": 3, "row_spacing": 200, "col_spacing": 250}
        
        for idx, node in enumerate(nodes):
            ir_type = node.get("type", "")
            ir_subtype = node.get("subtype", "")
            node_name = node.get("name", f"node_{idx}")
            
            talend_component = mappings.get(
                (ir_type, ir_subtype), "tUnknown"
            )
            
            print(f"  Node {idx}: {node_name} ({ir_type}/{ir_subtype}) → {talend_component}")
            
            # Calculate position
            row = idx // pos_config["max_per_row"]
            col = idx % pos_config["max_per_row"]
            pos_x = 100 + (col * pos_config["col_spacing"])
            pos_y = 100 + (row * pos_config["row_spacing"])
            
            # Build basic parameters from node properties
            params = self._create_node_parameters(talend_component, node.get("props", {}), node_name)
            
            # Get schema for this node
            schema_ref = node.get("schemaRef")
            schema_columns = schemas.get(schema_ref, []) if schema_ref else []
            
            # Build metadata and nodeData
            metadata, node_data = self._build_metadata_and_node_data(
                talend_component, node, schema_columns
            )
            
            # Set correct component version (tMap uses 2.1, others use 0.102)
            component_version = "2.1" if talend_component == "tMap" else "0.102"
            
            talend_node = {
                "componentName": talend_component,
                "componentVersion": component_version,
                "uniqueName": node_name,
                "posX": pos_x,
                "posY": pos_y,
                "parameters": params,
                "metadata": metadata,
            }
            
            # Add nodeData if present (for tMap, etc.)
            if node_data:
                talend_node["nodeData"] = node_data
            
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
                    "offsetLabelX": "0",
                    "offsetLabelY": "0",
                }
                talend_connections.append(connection)
        
        return {
            "name": job_ir.get("name", "MigratedJob"),
            "nodes": talend_nodes,
            "connections": talend_connections,
        }

    def _build_metadata_and_node_data(
        self,
        component_name: str,
        ir_node: Dict[str, Any],
        schema_columns: List[Dict[str, Any]],
    ) -> tuple:
        """Build metadata and nodeData for a Talend node from IR schema."""
        ir_type = ir_node.get("type", "")
        ir_subtype = ir_node.get("subtype", "")
        
        if component_name == "tMap" and ir_type == "Transform" and ir_subtype == "Map":
            return self._generate_tmap_metadata_and_nodedata_dict(schema_columns)
        
        if component_name.startswith("tDB") or component_name == "tFileInputDelimited":
            return (
                self._generate_simple_metadata(schema_columns, name=ir_node.get("name", "row1")),
                None,
            )
        
        if component_name.startswith("tDBOutput") or component_name == "tFileOutputDelimited":
            return (
                self._generate_simple_metadata(schema_columns, name=ir_node.get("name", "target")),
                None,
            )
        
        if component_name == "tFilterRow" and ir_type == "Transform":
            return (
                self._generate_simple_metadata(schema_columns, name="row1"),
                None,
            )
        
        if component_name == "tAggregateRow" and ir_type == "Transform":
            return (
                self._generate_simple_metadata(schema_columns, name="target"),
                None,
            )
        
        # Default: return empty metadata if no schema columns
        if schema_columns:
            return (
                self._generate_simple_metadata(schema_columns, name="row1"),
                None,
            )
        
        return [], None

    def _generate_tmap_metadata_and_nodedata_dict(
        self, schema_columns: List[Dict[str, Any]]
    ) -> tuple:
        """Generate metadata and nodeData for tMap component."""
        talend_columns = [self._ir_column_to_talend(col) for col in schema_columns]
        
        # tMap only has metadata for output connector, input is defined in nodeData
        metadata = [
            {"connector": "FLOW", "name": "out1", "columns": talend_columns},
        ]
        
        output_entries = [
            {
                "name": col.get("name", "unknown"),
                "expression": f"row1.{col.get('name', 'unknown')}",
                "type": self._map_ir_type_to_talend(col.get("type", "string")),
                "nullable": "true",
            }
            for col in schema_columns
        ]
        
        node_data = {
            "uiProperties": {},
            "varTables": [
                {"sizeState": "INTERMEDIATE", "name": "Var", "minimized": True}
            ],
            "outputTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "out1",
                    "mapperTableEntries": output_entries,
                }
            ],
            "inputTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "row1",
                    "matchingMode": "UNIQUE_MATCH",
                    "lookupMode": "LOAD_ONCE",
                    "mapperTableEntries": [
                        {
                            "name": col.get("name", "unknown"),
                            "type": self._map_ir_type_to_talend(col.get("type", "string")),
                            "nullable": "true",
                        }
                        for col in schema_columns
                    ],
                }
            ],
        }
        
        return metadata, node_data

    def _generate_simple_metadata(
        self, schema_columns: List[Dict[str, Any]], name: str
    ) -> List[Dict[str, Any]]:
        """Generate simple metadata for input/output components."""
        if not schema_columns:
            return []
        talend_columns = [self._ir_column_to_talend(col) for col in schema_columns]
        return [{"connector": "FLOW", "name": name, "columns": talend_columns}]

    def _ir_column_to_talend(self, column: Dict[str, Any]) -> Dict[str, Any]:
        """Convert IR column to Talend column format."""
        return {
            "comment": column.get("comment", ""),
            "key": "false",
            "length": str(column.get("length", -1)),
            "name": column.get("name", "unknown"),
            "nullable": "true" if column.get("nullable", True) else "false",
            "pattern": column.get("pattern", ""),
            "precision": str(column.get("precision", -1)),
            "sourceType": column.get("sourceType", ""),
            "type": self._map_ir_type_to_talend(column.get("type", "string")),
            "originalLength": str(column.get("originalLength", -1)),
            "usefulColumn": "true",
        }

    def _map_ir_type_to_talend(self, ir_type: str) -> str:
        """Map IR data type to Talend type."""
        mapping = {
            "string": "id_String",
            "integer": "id_Integer",
            "int": "id_Integer",
            "number": "id_Double",
            "decimal": "id_BigDecimal",
            "float": "id_Float",
            "double": "id_Double",
            "date": "id_Date",
            "timestamp": "id_Date",
            "boolean": "id_Boolean",
            "bool": "id_Boolean",
        }
        return mapping.get(ir_type.lower(), "id_String")

    def _create_node_parameters(self, component_type: str, properties: Dict[str, Any], unique_name: str = None) -> List[Dict[str, Any]]:
        """Create Talend element parameters from IR node properties."""
        if unique_name is None:
            unique_name = component_type
        params = [
            {"field": "TEXT", "name": "UNIQUE_NAME", "value": unique_name, "show": False}
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
            # tMap requires specific parameters for Talend validation
            # Some parameters should NOT have show="false" (omit show attribute)
            params.extend([
                {"field": "EXTERNAL", "name": "MAP", "value": ""},  # No show attribute
                {"field": "CLOSED_LIST", "name": "LINK_STYLE", "value": "AUTO"},  # No show attribute
                {"field": "DIRECTORY", "name": "TEMPORARY_DATA_DIRECTORY", "value": ""},  # No show attribute
                {"field": "IMAGE", "name": "PREVIEW", "value": ""},  # No show attribute
                {"field": "CHECK", "name": "DIE_ON_ERROR", "value": "true", "show": False},
                {"field": "CHECK", "name": "LKUP_PARALLELIZE", "value": "false", "show": False},
                {"field": "TEXT", "name": "LEVENSHTEIN", "value": "0", "show": False},
                {"field": "TEXT", "name": "JACCARD", "value": "0", "show": False},
                {"field": "CHECK", "name": "ENABLE_AUTO_CONVERT_TYPE", "value": "false", "show": False},
                {"field": "TEXT", "name": "ROWS_BUFFER_SIZE", "value": "2000000"},  # No show attribute
                {"field": "CHECK", "name": "CHANGE_HASH_AND_EQUALS_FOR_BIGDECIMAL", "value": "true"},  # No show attribute
                {"field": "TEXT", "name": "CONNECTION_FORMAT", "value": "row"},  # No show attribute
            ])
        
        return params
    
    def _node_to_xml(self, node: Dict[str, Any]) -> str:
        """Convert a Talend node to XML element with full support for metadata and nodeData."""
        xml_lines = [
            f'  <node componentName="{node["componentName"]}" '
            f'componentVersion="{node.get("componentVersion", "0.102")}" '
            f'offsetLabelX="0" offsetLabelY="0" '
            f'posX="{node["posX"]}" posY="{node["posY"]}">'
        ]
        
        # Add elementParameters
        for param in node.get("parameters", []):
            value = str(param.get("value", ""))
            # Escape XML special characters
            value = (
                value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )
            # Only include show attribute if it's explicitly False
            # If show is True or not specified, omit the attribute (defaults to visible)
            show_attr = ""
            if "show" in param and param.get("show") is False:
                show_attr = ' show="false"'
            elif "show" in param and param.get("show") is True:
                show_attr = ' show="true"'
            
            xml_lines.append(
                f'    <elementParameter field="{param.get("field", "TEXT")}" '
                f'name="{param.get("name", "")}" value="{value}"{show_attr}/>'
            )
        
        # Add metadata (for schema/column definitions)
        for metadata in node.get("metadata", []):
            xml_lines.append(
                f'    <metadata connector="{metadata.get("connector", "FLOW")}" '
                f'name="{metadata.get("name", "row1")}">'
            )
            for column in metadata.get("columns", []):
                xml_lines.append(
                    '      <column comment="{comment}" key="{key}" length="{length}" '
                    'name="{name}" nullable="{nullable}" pattern="{pattern}" '
                    'precision="{precision}" sourceType="{sourceType}" type="{type}" '
                    'originalLength="{originalLength}" usefulColumn="{usefulColumn}"/>'.format(
                        comment=column.get("comment", ""),
                        key=column.get("key", "false"),
                        length=column.get("length", "-1"),
                        name=column.get("name", "unknown"),
                        nullable=column.get("nullable", "true"),
                        pattern=column.get("pattern", ""),
                        precision=column.get("precision", "-1"),
                        sourceType=column.get("sourceType", "VARCHAR"),
                        type=column.get("type", "id_String"),
                        originalLength=column.get("originalLength", "-1"),
                        usefulColumn=column.get("usefulColumn", "true")
                    )
                )
            xml_lines.append("    </metadata>")
        
        # Add nodeData (for complex components like tMap)
        node_data = node.get("nodeData")
        if isinstance(node_data, dict):
            # For tMap we emit real TalendMapper:MapperData XML
            if node.get("componentName") == "tMap":
                xml_lines.append('    <nodeData xsi:type="TalendMapper:MapperData">')
                
                # uiProperties
                ui_props = node_data.get("uiProperties", {})
                if ui_props:
                    xml_lines.append("      <uiProperties/>")
                else:
                    xml_lines.append("      <uiProperties/>")
                
                # varTables
                for var in node_data.get("varTables", []):
                    xml_lines.append(
                        f'      <varTables sizeState="{var.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{var.get("name", "Var")}" '
                        f'minimized="{str(var.get("minimized", True)).lower()}"/>'
                    )
                
                # outputTables
                for out_tbl in node_data.get("outputTables", []):
                    xml_lines.append(
                        f'      <outputTables sizeState="{out_tbl.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{out_tbl.get("name", "target")}">'
                    )
                    for entry in out_tbl.get("mapperTableEntries", []):
                        expression = entry.get("expression", "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                        xml_lines.append(
                            '        <mapperTableEntries name="{name}" expression="{expression}" '
                            'type="{type}" nullable="{nullable}"/>'.format(
                                name=entry.get("name", ""),
                                expression=expression,
                                type=entry.get("type", "id_String"),
                                nullable=entry.get("nullable", "true")
                            )
                        )
                    xml_lines.append("      </outputTables>")
                
                # inputTables
                for in_tbl in node_data.get("inputTables", []):
                    xml_lines.append(
                        f'      <inputTables sizeState="{in_tbl.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{in_tbl.get("name", "row1")}" '
                        f'matchingMode="{in_tbl.get("matchingMode", "UNIQUE_MATCH")}" '
                        f'lookupMode="{in_tbl.get("lookupMode", "LOAD_ONCE")}">'
                    )
                    for entry in in_tbl.get("mapperTableEntries", []):
                        xml_lines.append(
                            '        <mapperTableEntries name="{name}" type="{type}" '
                            'nullable="{nullable}"/>'.format(
                                name=entry.get("name", ""),
                                type=entry.get("type", "id_String"),
                                nullable=entry.get("nullable", "true")
                            )
                        )
                    xml_lines.append("      </inputTables>")
                
                xml_lines.append("    </nodeData>")
            else:
                # For non-tMap nodes, use JSON-in-CDATA representation
                node_data_json = json.dumps(node_data, indent=2)
                xml_lines.append(f"    <nodeData><![CDATA[{node_data_json}]]></nodeData>")
        
        xml_lines.append("  </node>")
        return "\n".join(xml_lines)
    
    def _connection_to_xml(self, conn: Dict[str, Any]) -> str:
        """Convert a Talend connection to XML element."""
        # Include offsetLabelX and offsetLabelY if present
        offset_attrs = ""
        if "offsetLabelX" in conn or "offsetLabelY" in conn:
            offset_x = conn.get("offsetLabelX", "0")
            offset_y = conn.get("offsetLabelY", "0")
            offset_attrs = f' offsetLabelX="{offset_x}" offsetLabelY="{offset_y}"'
        
        xml_lines = [
            f'  <connection connectorName="{conn.get("connectorName", "FLOW")}" '
            f'label="{conn.get("label", "")}" lineStyle="{conn.get("lineStyle", "0")}" '
            f'metaname="{conn.get("metaname", "")}"{offset_attrs} source="{conn.get("source", "")}" '
            f'target="{conn.get("target", "")}">'
        ]
        
        # Add connection parameters
        for param in conn.get("parameters", []):
            value = str(param.get("value", ""))
            # Escape XML special characters
            value = (
                value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )
            show_str = "true" if param.get("show", False) else "false"
            xml_lines.append(
                f'    <elementParameter field="{param.get("field", "TEXT")}" '
                f'name="{param.get("name", "")}" value="{value}" show="{show_str}"/>'
            )
        
        xml_lines.append("  </connection>")
        return "\n".join(xml_lines)
    

    async def _get_talend_templates(self) -> Dict[str, Dict[str, Any]]:
        """Get Talend component templates from database with enhanced metadata"""
        templates = {}
        with open("templates/talend_job.item.xmlt", 'r') as f:
            item_xml = f.read()
            templates['item_template'] = item_xml
        with open("templates/talend_job.properties.xmlt", 'r') as f:
            properties_xml = f.read()
            templates['properties_template'] = properties_xml
        with open("templates/talend.project.xmlt", 'r') as f:
            project_xml = f.read()
            templates['project_template'] = project_xml
        return templates
        # result = await self.db.execute(
        #     select(TalendTemplate).where(TalendTemplate.tenant_id == "default")
        # )
        # templates = result.scalars().all()
        # return {
        #     t.component_name: {
        #         "template_xml": t.template_xml,
        #         "property_definitions": t.property_definitions or {},
        #         "description": t.description or "",
        #         "category": t.category or "unknown",
        #         "additional_context": t.additional_context or ""
        #     } 
        #     for t in templates
        # }
    
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