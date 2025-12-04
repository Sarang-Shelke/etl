"""
Production-level DSX file parser for DataStage jobs.

This module provides functionality to parse .dsx files and convert them
into structured Job and ASG representations.
"""
import xml.etree.ElementTree as ET
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from schemas import (
    Job,
    Stage,
    StageProperties,
    StageType,
    FieldDefinition,
    DataType,
    Transformation,
    Link,
    EdgeType
)
from exceptions import (
    DSXParseError,
    DSXValidationError,
    StageNotFoundError,
    SchemaValidationError
)


logger = logging.getLogger(__name__)


class DSXParser:
    """
    Production-level parser for DataStage DSX files.
    
    This parser validates and converts DSX XML files into structured
    Job and ASG objects with comprehensive error handling.
    """
    
    def __init__(self, strict_validation: bool = True):
        """
        Initialize the DSX parser.
        
        Args:
            strict_validation: If True, raises exceptions on validation errors.
                              If False, logs warnings and continues.
        """
        self.strict_validation = strict_validation
    
    def parse(self, file_path: str) -> Job:
        """
        Parse a DSX file and return a Job object.
        
        Args:
            file_path: Path to the .dsx file
        
        Returns:
            Job object representing the parsed DSX file
        
        Raises:
            DSXParseError: If file cannot be parsed
            DSXValidationError: If file structure is invalid
        """
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            error_msg = f"DSX file not found: {file_path}"
            logger.error(error_msg)
            raise DSXParseError(error_msg)
        
        if not file_path_obj.is_file():
            error_msg = f"Path is not a file: {file_path}"
            logger.error(error_msg)
            raise DSXParseError(error_msg)
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            error_msg = f"Failed to parse XML file: {e}"
            logger.error(error_msg)
            raise DSXParseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error reading file: {e}"
            logger.error(error_msg)
            raise DSXParseError(error_msg) from e
        
        # Validate root element
        if root.tag != "DataStageJob":
            error_msg = f"Invalid root element. Expected 'DataStageJob', got '{root.tag}'"
            logger.error(error_msg)
            if self.strict_validation:
                raise DSXValidationError(error_msg)
            else:
                logger.warning(error_msg)
        
        return self._parse_job(root, file_path)
    
    def _parse_job(self, root: ET.Element, file_path: str) -> Job:
        """Parse the root element into a Job object."""
        job_name = root.findtext("JobName", default="Unknown_Job")
        description = root.findtext("Description", default=None)
        
        logger.info(f"Parsing job: {job_name}")
        
        # Parse stages
        stages_elem = root.find("Stages")
        stages: List[Stage] = []
        if stages_elem is not None:
            for stage_elem in stages_elem.findall("Stage"):
                try:
                    stage = self._parse_stage(stage_elem)
                    stages.append(stage)
                    logger.debug(f"Parsed stage: {stage.name} ({stage.stage_type.value})")
                except Exception as e:
                    error_msg = f"Failed to parse stage: {e}"
                    logger.error(error_msg)
                    if self.strict_validation:
                        raise DSXValidationError(error_msg) from e
                    else:
                        logger.warning(f"Skipping stage due to error: {error_msg}")
        
        # Parse links
        links_elem = root.find("Links")
        links: List[Link] = []
        if links_elem is not None:
            for link_elem in links_elem.findall("Link"):
                try:
                    link = self._parse_link(link_elem, stages)
                    links.append(link)
                    logger.debug(f"Parsed link: {link.source_stage} -> {link.target_stage}")
                except Exception as e:
                    error_msg = f"Failed to parse link: {e}"
                    logger.error(error_msg)
                    if self.strict_validation:
                        raise DSXValidationError(error_msg) from e
                    else:
                        logger.warning(f"Skipping link due to error: {error_msg}")
        
        job = Job(
            job_name=job_name.strip(),
            stages=stages,
            links=links,
            description=description.strip() if description else None,
            metadata={"source_file": str(file_path)}
        )
        
        # Validate job structure
        self._validate_job(job)
        
        logger.info(f"Successfully parsed job '{job_name}' with {len(stages)} stages and {len(links)} links")
        return job
    
    def _parse_stage(self, stage_elem: ET.Element) -> Stage:
        """Parse a Stage element into a Stage object."""
        name_elem = stage_elem.find("Name")
        if name_elem is None or name_elem.text is None:
            raise DSXValidationError("Stage missing required 'Name' element")
        
        name = name_elem.text.strip()
        if not name:
            raise DSXValidationError("Stage name cannot be empty")
        
        stage_type_str = stage_elem.findtext("StageType", default="Generic")
        try:
            stage_type = StageType(stage_type_str)
        except ValueError:
            logger.warning(f"Unknown stage type: {stage_type_str}, defaulting to Generic")
            stage_type = StageType.GENERIC
        
        # Parse properties
        properties_elem = stage_elem.find("Properties")
        properties = self._parse_stage_properties(properties_elem, stage_type)
        
        return Stage(
            name=name,
            stage_type=stage_type,
            properties=properties
        )
    
    def _parse_stage_properties(
        self, 
        properties_elem: Optional[ET.Element],
        stage_type: StageType
    ) -> StageProperties:
        """Parse Stage properties element."""
        props = StageProperties()
        
        if properties_elem is None:
            return props
        
        for prop_elem in properties_elem:
            tag = prop_elem.tag
            
            if tag == "FilePath":
                file_path = prop_elem.text.strip() if prop_elem.text else None
                props.file_path = file_path
            
            elif tag == "FieldDefinitions":
                props.field_definitions = self._parse_field_definitions(prop_elem)
            
            elif tag == "InputFields":
                props.input_fields = self._parse_field_list(prop_elem)
            
            elif tag == "OutputFields":
                props.output_fields = self._parse_field_list(prop_elem)
            
            elif tag == "Transformations":
                props.transformations = self._parse_transformations(prop_elem)
            
            elif tag == "ConnectionString":
                props.connection_string = prop_elem.text.strip() if prop_elem.text else None
            
            elif tag == "TableName":
                props.table_name = prop_elem.text.strip() if prop_elem.text else None
            
            elif tag == "Query":
                props.query = prop_elem.text.strip() if prop_elem.text else None
            
            else:
                # Store as custom property
                try:
                    value = prop_elem.text.strip() if prop_elem.text else None
                    props.add_custom_property(tag, value)
                except Exception as e:
                    logger.warning(f"Failed to parse custom property '{tag}': {e}")
        
        return props
    
    def _parse_field_definitions(self, field_defs_elem: ET.Element) -> List[FieldDefinition]:
        """Parse FieldDefinitions element."""
        field_definitions = []
        
        for field_elem in field_defs_elem.findall("Field"):
            name_elem = field_elem.find("Name")
            if name_elem is None or name_elem.text is None:
                logger.warning("Field definition missing 'Name', skipping")
                continue
            
            name = name_elem.text.strip()
            
            # Parse data type
            data_type_str = field_elem.findtext("DataType", default="Unknown")
            try:
                data_type = DataType(data_type_str)
            except ValueError:
                logger.warning(f"Unknown data type: {data_type_str}, defaulting to Unknown")
                data_type = DataType.UNKNOWN
            
            # Parse optional attributes
            length = None
            length_elem = field_elem.find("Length")
            if length_elem is not None and length_elem.text:
                try:
                    length = int(length_elem.text.strip())
                except ValueError:
                    logger.warning(f"Invalid Length value: {length_elem.text}")
            
            precision = None
            precision_elem = field_elem.find("Precision")
            if precision_elem is not None and precision_elem.text:
                try:
                    precision = int(precision_elem.text.strip())
                except ValueError:
                    logger.warning(f"Invalid Precision value: {precision_elem.text}")
            
            scale = None
            scale_elem = field_elem.find("Scale")
            if scale_elem is not None and scale_elem.text:
                try:
                    scale = int(scale_elem.text.strip())
                except ValueError:
                    logger.warning(f"Invalid Scale value: {scale_elem.text}")
            
            nullable_elem = field_elem.find("Nullable")
            nullable = True  # Default
            if nullable_elem is not None and nullable_elem.text:
                nullable_str = nullable_elem.text.strip().lower()
                nullable = nullable_str in ("true", "1", "yes")
            
            default_value = field_elem.findtext("DefaultValue", default=None)
            description = field_elem.findtext("Description", default=None)
            
            try:
                field_def = FieldDefinition(
                    name=name,
                    data_type=data_type,
                    length=length,
                    precision=precision,
                    scale=scale,
                    nullable=nullable,
                    default_value=default_value.strip() if default_value else None,
                    description=description.strip() if description else None
                )
                field_definitions.append(field_def)
            except ValueError as e:
                error_msg = f"Invalid field definition for '{name}': {e}"
                if self.strict_validation:
                    raise SchemaValidationError(error_msg) from e
                else:
                    logger.warning(error_msg)
        
        return field_definitions
    
    def _parse_field_list(self, fields_elem: ET.Element) -> List[str]:
        """Parse a list of field names."""
        fields = []
        for field_elem in fields_elem.findall("Field"):
            if field_elem.text:
                field_name = field_elem.text.strip()
                if field_name:
                    fields.append(field_name)
        return fields
    
    def _parse_transformations(self, trans_elem: ET.Element) -> List[Transformation]:
        """Parse Transformations element."""
        transformations = []
        
        for trans_item in trans_elem.findall("Transformation"):
            output_field_elem = trans_item.find("OutputField")
            expression_elem = trans_item.find("Expression")
            
            if output_field_elem is None or not output_field_elem.text:
                logger.warning("Transformation missing 'OutputField', skipping")
                continue
            
            if expression_elem is None or not expression_elem.text:
                logger.warning("Transformation missing 'Expression', skipping")
                continue
            
            output_field = output_field_elem.text.strip()
            expression = expression_elem.text.strip()
            
            trans_type = trans_item.findtext("TransformationType", default=None)
            description = trans_item.findtext("Description", default=None)
            
            try:
                transformation = Transformation(
                    output_field=output_field,
                    expression=expression,
                    transformation_type=trans_type.strip() if trans_type else None,
                    description=description.strip() if description else None
                )
                transformations.append(transformation)
            except ValueError as e:
                error_msg = f"Invalid transformation for '{output_field}': {e}"
                if self.strict_validation:
                    raise SchemaValidationError(error_msg) from e
                else:
                    logger.warning(error_msg)
        
        return transformations
    
    def _parse_link(self, link_elem: ET.Element, stages: List[Stage]) -> Link:
        """Parse a Link element into a Link object."""
        from_elem = link_elem.find("From")
        to_elem = link_elem.find("To")
        
        if from_elem is None or from_elem.text is None:
            raise DSXValidationError("Link missing required 'From' element")
        
        if to_elem is None or to_elem.text is None:
            raise DSXValidationError("Link missing required 'To' element")
        
        source_stage = from_elem.text.strip()
        target_stage = to_elem.text.strip()
        
        # Validate that referenced stages exist
        stage_names = {stage.name for stage in stages}
        if source_stage not in stage_names:
            error_msg = f"Link references non-existent source stage: {source_stage}"
            if self.strict_validation:
                raise StageNotFoundError(error_msg)
            else:
                logger.warning(error_msg)
        
        if target_stage not in stage_names:
            error_msg = f"Link references non-existent target stage: {target_stage}"
            if self.strict_validation:
                raise StageNotFoundError(error_msg)
            else:
                logger.warning(error_msg)
        
        # Parse optional link properties
        link_properties = {}
        edge_type = EdgeType.DATA_FLOW  # Default
        
        edge_type_elem = link_elem.find("EdgeType")
        if edge_type_elem is not None and edge_type_elem.text:
            try:
                edge_type = EdgeType(edge_type_elem.text.strip())
            except ValueError:
                logger.warning(f"Unknown edge type: {edge_type_elem.text}, defaulting to DataFlow")
        
        # Parse any other link properties
        for child in link_elem:
            if child.tag not in ("From", "To", "EdgeType"):
                if child.text:
                    link_properties[child.tag] = child.text.strip()
        
        return Link(
            source_stage=source_stage,
            target_stage=target_stage,
            edge_type=edge_type,
            link_properties=link_properties
        )
    
    def _validate_job(self, job: Job) -> None:
        """
        Validate job structure.
        
        Raises:
            DSXValidationError: If validation fails
        """
        # Check for duplicate stage names
        stage_names = [stage.name for stage in job.stages]
        duplicates = [name for name in set(stage_names) if stage_names.count(name) > 1]
        if duplicates:
            error_msg = f"Duplicate stage names found: {', '.join(duplicates)}"
            if self.strict_validation:
                raise DSXValidationError(error_msg)
            else:
                logger.warning(error_msg)
        
        # Validate links reference existing stages
        stage_name_set = {stage.name for stage in job.stages}
        invalid_links = []
        
        for link in job.links:
            if link.source_stage not in stage_name_set:
                invalid_links.append(f"Source: {link.source_stage}")
            if link.target_stage not in stage_name_set:
                invalid_links.append(f"Target: {link.target_stage}")
        
        if invalid_links:
            error_msg = f"Links reference non-existent stages: {', '.join(invalid_links)}"
            if self.strict_validation:
                raise DSXValidationError(error_msg)
            else:
                logger.warning(error_msg)


def collect_tags(elem: ET.Element, tags: Optional[List] = None, level: int = 0) -> List[tuple]:
    """
    Collect all XML tags with their hierarchy levels.
    
    Args:
        elem: Root XML element
        tags: Accumulated list of (tag, level) tuples
        level: Current hierarchy level
    
    Returns:
        List of (tag, level) tuples
    """
    if tags is None:
        tags = []
    tags.append((elem.tag, level))
    for child in elem:
        collect_tags(child, tags, level + 1)
    return tags


def parse_dsx_file(file_path: str, strict_validation: bool = True) -> Job:
    """
    Convenience function to parse a DSX file.
    
    Args:
        file_path: Path to the .dsx file
        strict_validation: If True, raises exceptions on validation errors
    
    Returns:
        Job object representing the parsed DSX file
    
    Raises:
        DSXParseError: If file cannot be parsed
        DSXValidationError: If file structure is invalid
    """
    parser = DSXParser(strict_validation=strict_validation)
    return parser.parse(file_path)
