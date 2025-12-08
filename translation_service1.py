"""
Second IR-based translation service.

This version:
- **Does not use the database or LLMs**
- **Only uses the IR JSON structure** plus simple hard‑coded mappings
- Generates Talend `talend.project`, `.item`, `.properties` files
  in a TEST_6‑style layout that Talend can import.

Intended usage (example):
    from translation_service1 import TranslationService1
    import json, asyncio

    async def main():
        svc = TranslationService1()
        with open("new_ir.json", "r", encoding="utf-8") as f:
            ir = json.load(f)
        translated = await svc.translate_logic(ir)
        paths = svc.render_first_job(translated, "generated_jobs_test")
        print(paths)

    asyncio.run(main())
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader


class TranslationService1:
    """
    IR → Talend translation without DB / LLM dependencies.

    This is intentionally simpler and self‑contained so that you can
    iterate on IR‑to‑Talend mapping logic without touching the shared DB.
    """

    def __init__(self, templates_dir: str = "templates") -> None:
        self.templates_dir = templates_dir
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    async def translate_logic(self, ir_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Translate IR JSON to an in‑memory Talend job description.

        No DB, no LLM – just deterministic rules based on IR node
        `type` / `subtype` and schemas.
        """
        job_name = ir_data.get("job", {}).get("name", "IR_JOB")

        ir_nodes: List[Dict[str, Any]] = ir_data.get("nodes", [])
        ir_links: List[Dict[str, Any]] = ir_data.get("links", [])
        ir_schemas: Dict[str, List[Dict[str, Any]]] = ir_data.get("schemas", {})

        layout_config = self._select_layout_strategy(len(ir_nodes))

        talend_nodes: List[Dict[str, Any]] = []
        for idx, ir_node in enumerate(ir_nodes):
            component_name = self._map_ir_node_to_component(ir_node)
            node = self._create_talend_node_from_ir(
                ir_node=ir_node,
                component_name=component_name,
                position=idx,
                layout_config=layout_config,
                ir_schemas=ir_schemas,
            )
            talend_nodes.append(node)

        talend_nodes = self._optimize_layout_for_flow(talend_nodes, layout_config)
        raw_connections = self._build_connections(ir_links, ir_nodes)
        connections = self._enhance_connections_with_positions(talend_nodes, raw_connections)

        job = {
            "name": job_name,
            "nodes": talend_nodes,
            "connections": connections,
            "metadata": {
                "ir_job": ir_data.get("job", {}),
                "source": "ir",
                "translated_at": datetime.utcnow().isoformat(),
                "ir_version": ir_data.get("irVersion", "unknown"),
            },
        }

        return {"jobs": [job], "metadata": job["metadata"]}

    def render_first_job(
        self,
        translated_logic: Dict[str, Any],
        output_base_dir: str = "generated_jobs",
        project_name: str = "MigratedProject",
    ) -> Dict[str, str]:
        """
        Convenience helper: render the first job in `translated_logic`
        using the same template pipeline as `translation_service.py`.
        """
        jobs = translated_logic.get("jobs") or []
        if not jobs:
            raise ValueError("No jobs found in translated_logic")
        return self._render_talend_artifacts(jobs[0], output_base_dir, project_name)

    # ------------------------------------------------------------------
    # IR → Talend mapping helpers
    # ------------------------------------------------------------------

    def _map_ir_node_to_component(self, ir_node: Dict[str, Any]) -> str:
        """Very small, deterministic mapping from IR type/subtype → Talend component."""
        ir_type = (ir_node.get("type") or "").lower()
        ir_subtype = (ir_node.get("subtype") or "").lower()

        if ir_type == "source" and ir_subtype == "database":
            return "tDBInput"
        if ir_type == "source" and ir_subtype == "file":
            return "tFileInputDelimited"
        if ir_type == "transform" and ir_subtype == "map":
            return "tMap"
        if ir_type == "transform" and ir_subtype == "filter":
            return "tFilterRow"
        if ir_type == "transform" and ir_subtype == "aggregate":
            return "tAggregateRow"
        if ir_type == "sink" and ir_subtype == "database":
            return "tDBOutput"

        return "tUnknown"

    def _select_layout_strategy(self, num_nodes: int) -> Dict[str, Any]:
        """Simple layout config roughly matching the existing service."""
        config: Dict[str, Any] = {
            "max_components_per_row": 3,
            "row_spacing": 200,
            "component_spacing": 250,
        }

        if num_nodes <= 3:
            config["max_components_per_row"] = max(1, num_nodes)
            config["row_spacing"] = 150
            config["component_spacing"] = 200
        elif num_nodes <= 6:
            config["max_components_per_row"] = 3
            config["row_spacing"] = 180
            config["component_spacing"] = 220
        elif num_nodes <= 12:
            config["max_components_per_row"] = 4
            config["row_spacing"] = 200
            config["component_spacing"] = 250
        else:
            config["max_components_per_row"] = 5
            config["row_spacing"] = 180
            config["component_spacing"] = 200

        return config

    def _calculate_component_position(
        self, position: int, layout_config: Dict[str, Any]
    ) -> Tuple[int, int]:
        max_per_row = layout_config["max_components_per_row"]
        row_spacing = layout_config["row_spacing"]
        component_spacing = layout_config["component_spacing"]

        row_number = position // max_per_row
        col_in_row = position % max_per_row

        base_x = 100
        base_y = 100

        pos_x = base_x + (col_in_row * component_spacing)
        pos_y = base_y + (row_number * row_spacing)

        if col_in_row > 0:
            pos_y += col_in_row * 20

        return pos_x, pos_y

    def _create_talend_node_from_ir(
        self,
        ir_node: Dict[str, Any],
        component_name: str,
        position: int,
        layout_config: Dict[str, Any],
        ir_schemas: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        node_name = ir_node.get("name", f"node_{position}")
        pos_x, pos_y = self._calculate_component_position(position, layout_config)

        parameters = self._create_parameter_block(component_name, ir_node.get("props", {}), node_name)

        schema_ref = ir_node.get("schemaRef")
        schema_columns = ir_schemas.get(schema_ref, []) if schema_ref else []

        metadata, node_data = self._build_metadata_and_node_data(
            component_name, ir_node, schema_columns
        )

        return {
            "componentName": component_name,
            "componentVersion": "0.102",
            "uniqueName": node_name,
            "posX": pos_x,
            "posY": pos_y,
            "parameters": parameters,
            "metadata": metadata,
            "nodeData": node_data,
        }

    def _create_parameter_block(
        self,
        component_type: str,
        ir_props: Dict[str, Any],
        unique_name: str,
    ) -> List[Dict[str, Any]]:
        """Create Talend elementParameters with some component-specific defaults."""
        params: List[Dict[str, Any]] = [
            {"field": "TEXT", "name": "UNIQUE_NAME", "value": unique_name, "show": False}
        ]

        config = ir_props.get("configuration", {})

        if component_type.startswith("tDB"):
            table_name = config.get("table") or ir_props.get("table")
            if table_name:
                params.append(
                    {
                        "field": "DBTABLE",
                        "name": "TABLE",
                        "value": str(table_name),
                        "show": True,
                    }
                )
        elif component_type == "tFileInputDelimited":
            file_path = config.get("file") or ir_props.get("path")
            if file_path:
                params.append(
                    {
                        "field": "FILE",
                        "name": "FILENAME",
                        "value": str(file_path),
                        "show": True,
                    }
                )
            delimiter = config.get("delimiter", ",")
            params.append(
                {
                    "field": "TEXT",
                    "name": "FIELDSEPARATOR",
                    "value": str(delimiter),
                    "show": True,
                }
            )

        # tMap has a fairly strict set of required parameters; we mirror
        # the ones from a known-good export (Fullnamefilter_0.1).
        if component_type == "tMap":
            params.extend(
                [
                    {"field": "EXTERNAL", "name": "MAP", "value": "", "show": False},
                    {
                        "field": "CLOSED_LIST",
                        "name": "LINK_STYLE",
                        "value": "AUTO",
                        "show": False,
                    },
                    {
                        "field": "DIRECTORY",
                        "name": "TEMPORARY_DATA_DIRECTORY",
                        "value": "",
                        "show": False,
                    },
                    {
                        "field": "IMAGE",
                        "name": "PREVIEW",
                        "value": "",
                        "show": False,
                    },
                    {
                        "field": "CHECK",
                        "name": "DIE_ON_ERROR",
                        "value": "true",
                        "show": False,
                    },
                    {
                        "field": "CHECK",
                        "name": "LKUP_PARALLELIZE",
                        "value": "false",
                        "show": False,
                    },
                    {
                        "field": "TEXT",
                        "name": "LEVENSHTEIN",
                        "value": "0",
                        "show": False,
                    },
                    {
                        "field": "TEXT",
                        "name": "JACCARD",
                        "value": "0",
                        "show": False,
                    },
                    {
                        "field": "CHECK",
                        "name": "ENABLE_AUTO_CONVERT_TYPE",
                        "value": "false",
                        "show": False,
                    },
                    {
                        "field": "TEXT",
                        "name": "ROWS_BUFFER_SIZE",
                        "value": "2000000",
                        "show": False,
                    },
                    {
                        "field": "CHECK",
                        "name": "CHANGE_HASH_AND_EQUALS_FOR_BIGDECIMAL",
                        "value": "true",
                        "show": False,
                    },
                    {
                        "field": "TEXT",
                        "name": "CONNECTION_FORMAT",
                        "value": "row",
                        "show": False,
                    },
                ]
            )

        return params

    def _build_metadata_and_node_data(
        self,
        component_name: str,
        ir_node: Dict[str, Any],
        schema_columns: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        ir_type = ir_node.get("type")
        ir_subtype = ir_node.get("subtype")

        if component_name == "tMap" and ir_type == "Transform" and ir_subtype == "Map":
            return self._generate_tmap_metadata_and_nodedata_dict(schema_columns)

        if component_name.startswith("tDB") or component_name == "tFileInputDelimited":
            return (
                self._generate_simple_metadata(schema_columns, name="row1"),
                None,
            )

        if component_name.startswith("tDBOutput") or component_name == "tFileOutputDelimited":
            return (
                self._generate_simple_metadata(schema_columns, name="target"),
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

        return [], None

    def _generate_tmap_metadata_and_nodedata_dict(
        self, schema_columns: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        talend_columns = [self._ir_column_to_talend(col) for col in schema_columns]

        metadata = [
            {"connector": "FLOW", "name": "row1", "columns": talend_columns},
            {"connector": "FLOW", "name": "target", "columns": talend_columns},
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
            "uiProperties": {"shellMaximized": "true"},
            "varTables": [
                {"sizeState": "INTERMEDIATE", "name": "Var", "minimized": "true"}
            ],
            "outputTables": [
                {
                    "sizeState": "INTERMEDIATE",
                    "name": "target",
                    "expressionFilter": "",
                    "activateExpressionFilter": "false",
                    "columnNameFilter": "",
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
                            "type": self._map_ir_type_to_talend(
                                col.get("type", "string")
                            ),
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
        talend_columns = [self._ir_column_to_talend(col) for col in schema_columns]
        return [{"connector": "FLOW", "name": name, "columns": talend_columns}]

    def _ir_column_to_talend(self, column: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "comment": "",
            "key": "false",
            "length": "-1",
            "name": column.get("name", "unknown"),
            "nullable": "true" if column.get("nullable", True) else "false",
            "pattern": "",
            "precision": "-1",
            "sourceType": column.get("type", "string").upper(),
            "type": self._map_ir_type_to_talend(column.get("type", "string")),
            "originalLength": "-1",
            "usefulColumn": "true",
        }

    def _map_ir_type_to_talend(self, ir_type: str) -> str:
        mapping = {
            "string": "id_String",
            "integer": "id_Integer",
            "number": "id_Double",
            "decimal": "id_BigDecimal",
            "float": "id_Float",
            "double": "id_Double",
            "date": "id_Date",
            "timestamp": "id_Date",
            "boolean": "id_Boolean",
        }
        return mapping.get(ir_type.lower(), "id_String")

    # ------------------------------------------------------------------
    # Connections & layout
    # ------------------------------------------------------------------

    def _build_connections(
        self, ir_links: List[Dict[str, Any]], ir_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        id_to_name = {
            node["id"]: node.get("name", f"node_{idx}")
            for idx, node in enumerate(ir_nodes)
            if node.get("id")
        }

        connections: List[Dict[str, Any]] = []
        for link in ir_links:
            source_id = link.get("from", {}).get("nodeId")
            target_id = link.get("to", {}).get("nodeId")
            if not source_id or not target_id:
                continue

            source_name = id_to_name.get(source_id)
            target_name = id_to_name.get(target_id)
            if not source_name or not target_name:
                continue

            connections.append(
                {
                    "connectorName": "FLOW",
                    "label": f"row{source_name}",
                    "lineStyle": "0",
                    "metaname": source_name,
                    "source": source_name,
                    "target": target_name,
                    "parameters": [
                        {
                            "field": "CHECK",
                            "name": "MONITOR_CONNECTION",
                            "value": "false",
                            "show": False,
                        },
                        {
                            "field": "TEXT",
                            "name": "UNIQUE_NAME",
                            "value": f"row{source_name}",
                            "show": False,
                        },
                    ],
                }
            )

        return connections

    def _enhance_connections_with_positions(
        self,
        nodes: List[Dict[str, Any]],
        connections: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        node_lookup = {node["uniqueName"]: node for node in nodes}
        enhanced: List[Dict[str, Any]] = []

        for connection in connections:
            source_node = node_lookup.get(connection["source"])
            target_node = node_lookup.get(connection["target"])
            if not source_node or not target_node:
                enhanced.append(connection)
                continue

            enhanced.append(
                self._create_intelligent_connection(source_node, target_node, connection)
            )

        return enhanced

    def _create_intelligent_connection(
        self,
        source_node: Dict[str, Any],
        target_node: Dict[str, Any],
        base_connection: Dict[str, Any],
    ) -> Dict[str, Any]:
        source_x, source_y = source_node["posX"], source_node["posY"]
        target_x, target_y = target_node["posX"], target_node["posY"]

        if target_y > source_y:
            line_style = "2"
        elif target_x > source_x:
            line_style = "0"
        else:
            line_style = "1"

        connection = dict(base_connection)
        connection["lineStyle"] = line_style
        connection["label"] = (
            f"flow_{source_node['uniqueName']}_to_{target_node['uniqueName']}"
        )
        connection["metaname"] = (
            f"{source_node['uniqueName']}_to_{target_node['uniqueName']}"
        )
        connection["parameters"] = [
            {
                "field": "CHECK",
                "name": "MONITOR_CONNECTION",
                "value": "false",
                "show": False,
            },
            {
                "field": "TEXT",
                "name": "UNIQUE_NAME",
                "value": connection["label"],
                "show": False,
            },
        ]
        return connection

    def _optimize_layout_for_flow(
        self, nodes: List[Dict[str, Any]], layout_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        if len(nodes) <= 1:
            return nodes

        optimized: List[Dict[str, Any]] = []
        for idx, node in enumerate(nodes):
            pos_x, pos_y = node["posX"], node["posY"]
            if idx > 0:
                prev = optimized[-1]
                min_spacing = layout_config["component_spacing"]
                if abs(pos_x - prev["posX"]) < min_spacing:
                    pos_x = prev["posX"] + min_spacing
                if abs(pos_y - prev["posY"]) < 50:
                    pos_y = prev["posY"] + (idx * 30)

            optimized.append(
                {
                    **node,
                    "posX": pos_x,
                    "posY": pos_y,
                }
            )

        return optimized

    # ------------------------------------------------------------------
    # Rendering Talend artifacts (project / item / properties)
    # ------------------------------------------------------------------

    def _render_talend_artifacts(
        self, job: Dict[str, Any], output_base_dir: str, project_name: str
    ) -> Dict[str, str]:
        """
        Render Talend artifacts using the same templates as the main service,
        but with a TEST_6‑style layout:

        <output_base_dir>/<project_name>/
          talend.project
          process/
            DataStage/
              <JOB_NAME>_0.1.item
              <JOB_NAME>_0.1.properties
        """
        workspace_dir = os.path.join(output_base_dir)
        project_dir = os.path.join(workspace_dir, project_name)
        process_dir = os.path.join(project_dir, "process", "DataStage")
        os.makedirs(process_dir, exist_ok=True)

        # Use a single user ID consistently across project and properties.
        user_id = f"_{uuid.uuid4().hex}"

        project_ctx = {
            "project_id": f"_{uuid.uuid4().hex}",
            "project_label": project_name,
            "project_technical_label": project_name.upper(),
            "author_id": user_id,
            "product_version": "8.0.1.20250218_0945-patch",
            "project_type": "DQ",
            "items_relation_version": "1.3",
            "migration_task_id": f"_{uuid.uuid4().hex}",
            "migration_task_class": "org.talend.repository.model.migration.CheckProductVersionMigrationTask",
            "breaks_version": "7.1.0",
            "migration_version": "7.1.1",
            "user_id": user_id,
            "user_login": "etl.migrator@local",
        }

        project_tpl = self.jinja_env.get_template("talend.project.xmlt")
        project_path = os.path.join(project_dir, "talend.project")
        with open(project_path, "w", encoding="utf-8") as outfile:
            outfile.write(project_tpl.render(project_ctx))

        base_name = f"{job['name']}_0.1"
        item_tpl = self.jinja_env.get_template("talend_job.item.xmlt")
        nodes_payload = [
            {
                "componentName": node["componentName"],
                "uniqueName": node["uniqueName"],
                "posX": node["posX"],
                "posY": node["posY"],
                "raw_xml": self._node_to_raw_xml(node),
            }
            for node in job["nodes"]
        ]
        item_ctx = {
            "job": {
                "id": f"job_{uuid.uuid4().hex[:8]}",
                "name": job["name"],
                "version": "0.1",
                "nodes": nodes_payload,
                "connections": job["connections"],
                "subjobs": [],
            }
        }
        item_path = os.path.join(process_dir, f"{base_name}.item")
        with open(item_path, "w", encoding="utf-8") as outfile:
            outfile.write(item_tpl.render(item_ctx))

        props_tpl = self.jinja_env.get_template("talend_job.properties.xmlt")
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        props_ctx = {
            "uuid1": f"_{uuid.uuid4().hex}",
            "uuid2": f"_{uuid.uuid4().hex}",
            "uuid3": f"_{uuid.uuid4().hex}",
            "uuid4": f"_{uuid.uuid4().hex}",
            "uuid5": f"_{uuid.uuid4().hex}",
            "uuid6": f"_{uuid.uuid4().hex}",
            "uuid7": f"_{uuid.uuid4().hex}",
            "uuid8": f"_{uuid.uuid4().hex}",
            "uuid9": f"_{uuid.uuid4().hex}",

            "uuid11": f"_{uuid.uuid4().hex}",
            "label": job["name"],
            "display_name": job["name"],
            "user_id": project_ctx["user_id"],
            "product_version": project_ctx["product_version"],
            "created_date": timestamp,
            "modified_date": timestamp,

            "process_href": f"{base_name}.item#/",
        }
        props_path = os.path.join(process_dir, f"{base_name}.properties")
        with open(props_path, "w", encoding="utf-8") as outfile:
            outfile.write(props_tpl.render(props_ctx))

        return {
            "project": project_path,
            "item": item_path,
            "properties": props_path,
            "workspace": workspace_dir,
        }

    def _node_to_raw_xml(self, node: Dict[str, Any]) -> str:
        lines = [
            f'<node componentName="{node["componentName"]}" componentVersion="{node.get("componentVersion", "0.102")}" '
            f'offsetLabelX="0" offsetLabelY="0" posX="{node["posX"]}" posY="{node["posY"]}">'
        ]

        for param in node.get("parameters", []):
            value = str(param.get("value", ""))
            value = (
                value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )
            show = "true" if param.get("show") else "false"
            lines.append(
                f'  <elementParameter field="{param.get("field", "TEXT")}" '
                f'name="{param.get("name", "")}" value="{value}" show="{show}"/>'
            )

        for metadata in node.get("metadata", []):
            lines.append(
                f'  <metadata connector="{metadata.get("connector", "FLOW")}" '
                f'name="{metadata.get("name", "row1")}">'
            )
            for column in metadata.get("columns", []):
                lines.append(
                    '    <column comment="{comment}" key="{key}" length="{length}" '
                    'name="{name}" nullable="{nullable}" pattern="{pattern}" '
                    'precision="{precision}" sourceType="{sourceType}" type="{type}" '
                    'originalLength="{originalLength}" usefulColumn="{usefulColumn}"/>'.format(
                        **column
                    )
                )
            lines.append("  </metadata>")

        node_data = node.get("nodeData")
        if isinstance(node_data, dict):
            # For tMap we emit real TalendMapper:MapperData XML (no JSON/CDATA),
            # mirroring what Talend Studio exports so Fabric validation succeeds.
            if node.get("componentName") == "tMap":
                lines.append('  <nodeData xsi:type="TalendMapper:MapperData">')

                # uiProperties (can be empty element)
                lines.append("    <uiProperties/>")

                # varTables
                for var in node_data.get("varTables", []):
                    lines.append(
                        f'    <varTables sizeState="{var.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{var.get("name", "Var")}" '
                        f'minimized="{str(var.get("minimized", True)).lower()}"/>'
                    )

                # outputTables
                for out_tbl in node_data.get("outputTables", []):
                    lines.append(
                        f'    <outputTables sizeState="{out_tbl.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{out_tbl.get("name", "target")}">'
                    )
                    for entry in out_tbl.get("mapperTableEntries", []):
                        lines.append(
                            '      <mapperTableEntries name="{name}" expression="{expression}" '
                            'type="{type}" nullable="{nullable}"/>'.format(**entry)
                        )
                    lines.append("    </outputTables>")

                # inputTables
                for in_tbl in node_data.get("inputTables", []):
                    lines.append(
                        f'    <inputTables sizeState="{in_tbl.get("sizeState", "INTERMEDIATE")}" '
                        f'name="{in_tbl.get("name", "row1")}" '
                        f'matchingMode="{in_tbl.get("matchingMode", "UNIQUE_MATCH")}" '
                        f'lookupMode="{in_tbl.get("lookupMode", "LOAD_ONCE")}">'
                    )
                    for entry in in_tbl.get("mapperTableEntries", []):
                        lines.append(
                            '      <mapperTableEntries name="{name}" type="{type}" '
                            'nullable="{nullable}"/>'.format(**entry)
                        )
                    lines.append("    </inputTables>")

                lines.append("  </nodeData>")
            else:
                # For non-tMap nodes we keep the simpler JSON-in-CDATA representation,
                # which Talend happily ignores.
                node_data_json = json.dumps(node_data, indent=2)
                lines.append(f"  <nodeData><![CDATA[{node_data_json}]]></nodeData>")

        lines.append("</node>")
        return "\n".join(lines)



