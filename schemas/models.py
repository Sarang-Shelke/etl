"""Models for this service."""

import datetime
import enum
from typing import Optional

from uuid import UUID, uuid4
from sqlalchemy import (
    ForeignKey,
    UniqueConstraint,
    Index,
    Enum as SQLEnum,
    Text,
    Float,
    Integer,
    Boolean,
    String,
    Date,
    DateTime,
    JSON,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PostgresUUID
from sqlalchemy.orm import relationship, Mapped, mapped_column

from db import Base


# Enums
class ToolType(str, enum.Enum):
    """ETL tool type enum."""

    SOURCE = "source"
    TARGET = "target"
    BOTH = "both"


class JobStatus(str, enum.Enum):
    """Migration job status enum."""

    PENDING = "pending"
    PARSING = "parsing"
    MAPPING = "mapping"
    REVIEWING = "reviewing"
    TRANSLATING = "translating"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProcessingStage(str, enum.Enum):
    """Processing stage enum."""

    UPLOADED = "uploaded"
    PARSED = "parsed"
    MAPPED = "mapped"
    REVIEWED = "reviewed"
    TRANSLATED = "translated"
    GENERATED = "generated"


class BatchJobStatus(str, enum.Enum):
    """Batch job status enum."""

    PENDING = "pending"
    FETCHING = "fetching"
    PROCESSING = "processing"
    REVIEWING = "reviewing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StorageType(str, enum.Enum):
    """Storage connection type enum."""

    SHAREPOINT = "sharepoint"
    S3 = "s3"
    FTP = "ftp"
    SFTP = "sftp"


class MappingConfidence(str, enum.Enum):
    """Mapping confidence level enum."""

    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class ExecutionStage(str, enum.Enum):
    """Job execution stage enum."""

    PARSING = "parsing"
    MAPPING = "mapping"
    TRANSLATION = "translation"
    GENERATION = "generation"


class ExecutionStatus(str, enum.Enum):
    """Execution status enum."""

    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


class EventStatus(str, enum.Enum):
    """Event status enum."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class EntityType(str, enum.Enum):
    """Entity type enum."""

    JOB = "job"
    BATCH = "batch"
    PROJECT = "project"


# Models
class ETLTool(Base):
    """ETL tool version model."""

    __tablename__ = "etl_tools"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    tool_name: Mapped[str] = mapped_column(String(100), nullable=False)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    tool_type: Mapped[ToolType] = mapped_column(SQLEnum(ToolType), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_source_supported: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_target_supported: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    parser_available: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    generator_available: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    release_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    end_of_support_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB, name="metadata", nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    source_projects: Mapped[list["Project"]] = relationship(
        "Project", foreign_keys="Project.source_tool_id", back_populates="source_tool"
    )
    target_projects: Mapped[list["Project"]] = relationship(
        "Project", foreign_keys="Project.target_tool_id", back_populates="target_tool"
    )
    mapping_dictionaries: Mapped[list["MappingDictionary"]] = relationship(
        "MappingDictionary", back_populates="target_tool"
    )
    ir_property_mappings: Mapped[list["IRPropertyMapping"]] = relationship(
        "IRPropertyMapping", back_populates="tool"
    )
    target_component_templates: Mapped[list["TargetComponentTemplate"]] = relationship(
        "TargetComponentTemplate", back_populates="target_tool"
    )

    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint("tool_name", "version", name="_etl_tool_name_version_uc"),
        Index("idx_etl_tools_tool_name", "tool_name"),
        Index("idx_etl_tools_tool_name_version", "tool_name", "version"),
        Index("idx_etl_tools_is_active", "is_active"),
        Index("idx_etl_tools_tool_type", "tool_type"),
    )


class Project(Base):
    """Migration project model."""

    __tablename__ = "projects"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_tool_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("etl_tools.id"), nullable=False
    )
    target_tool_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("etl_tools.id"), nullable=False
    )
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB, name="metadata", nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    source_tool: Mapped["ETLTool"] = relationship("ETLTool", foreign_keys=[source_tool_id], back_populates="source_projects")
    target_tool: Mapped["ETLTool"] = relationship("ETLTool", foreign_keys=[target_tool_id], back_populates="target_projects")
    migration_jobs: Mapped[list["MigrationJob"]] = relationship("MigrationJob", back_populates="project")
    batch_jobs: Mapped[list["BatchJob"]] = relationship("BatchJob", back_populates="project")

    # Indexes
    __table_args__ = (
        Index("idx_projects_source_tool_id", "source_tool_id"),
        Index("idx_projects_target_tool_id", "target_tool_id"),
        Index("idx_projects_source_target_tool", "source_tool_id", "target_tool_id"),
    )


class BatchJob(Base):
    """Batch migration job model."""

    __tablename__ = "batch_jobs"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("projects.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[BatchJobStatus] = mapped_column(SQLEnum(BatchJobStatus), nullable=False, default=BatchJobStatus.PENDING)
    storage_connection_id: Mapped[Optional[UUID]] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("storage_connections.id"), nullable=True
    )
    source_path_pattern: Mapped[str] = mapped_column(String(500), nullable=False)
    file_filter_pattern: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    confidence_threshold: Mapped[float] = mapped_column(Float, default=0.8, nullable=False)
    auto_approve_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    total_files: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completed_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    pending_approval_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    batch_report_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    error_summary: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    project: Mapped["Project"] = relationship("Project", back_populates="batch_jobs")
    storage_connection: Mapped[Optional["StorageConnection"]] = relationship(
        "StorageConnection", back_populates="batch_jobs"
    )
    migration_jobs: Mapped[list["MigrationJob"]] = relationship("MigrationJob", back_populates="batch_job")

    # Indexes
    __table_args__ = (
        Index("idx_batch_jobs_project_id", "project_id"),
        Index("idx_batch_jobs_status", "status"),
    )


class StorageConnection(Base):
    """Storage connection model."""

    __tablename__ = "storage_connections"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    type: Mapped[StorageType] = mapped_column(SQLEnum(StorageType), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connection_config: Mapped[dict] = mapped_column(JSONB, nullable=False)  # Encrypted credentials
    base_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )

    # Relationships
    batch_jobs: Mapped[list["BatchJob"]] = relationship("BatchJob", back_populates="storage_connection")


class MigrationJob(Base):
    """Migration job model."""

    __tablename__ = "migration_jobs"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("projects.id"), nullable=False
    )
    batch_job_id: Mapped[Optional[UUID]] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("batch_jobs.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(500), nullable=False)
    source_file_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    source_storage_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    status: Mapped[JobStatus] = mapped_column(SQLEnum(JobStatus), nullable=False, default=JobStatus.PENDING)
    processing_stage: Mapped[Optional[ProcessingStage]] = mapped_column(
        SQLEnum(ProcessingStage), nullable=True
    )
    design_doc_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    asg_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    ir_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    mappings: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    translation_results: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    generated_files_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    report_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    requires_approval: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    approved_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_stack_trace: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_order: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    processing_started_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_completed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    project: Mapped["Project"] = relationship("Project", back_populates="migration_jobs")
    batch_job: Mapped[Optional["BatchJob"]] = relationship("BatchJob", back_populates="migration_jobs")
    execution_logs: Mapped[list["JobExecutionLog"]] = relationship("JobExecutionLog", back_populates="migration_job")

    # Indexes
    __table_args__ = (
        Index("idx_migration_jobs_batch_job_id", "batch_job_id"),
        Index("idx_migration_jobs_status", "status"),
        Index("idx_migration_jobs_project_id", "project_id"),
        Index("idx_migration_jobs_batch_status", "batch_job_id", "status"),
    )


class MappingDictionary(Base):
    """Component mapping dictionary model."""

    __tablename__ = "mapping_dictionary"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    ir_type: Mapped[str] = mapped_column(String(100), nullable=False)
    ir_subtype: Mapped[str] = mapped_column(String(100), nullable=False)
    target_tool_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("etl_tools.id"), nullable=False
    )
    target_component: Mapped[str] = mapped_column(String(255), nullable=False)
    default_properties: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    confidence: Mapped[MappingConfidence] = mapped_column(SQLEnum(MappingConfidence), nullable=False)
    usage_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_used_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    target_tool: Mapped["ETLTool"] = relationship("ETLTool", back_populates="mapping_dictionaries")

    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint(
            "ir_type", "ir_subtype", "target_tool_id", "target_component", name="_mapping_dict_uc"
        ),
        Index("idx_mapping_dict_target_tool_id", "target_tool_id"),
        Index("idx_mapping_dict_ir_type_subtype", "ir_type", "ir_subtype"),
        Index("idx_mapping_dict_ir_target", "ir_type", "ir_subtype", "target_tool_id"),
        Index("idx_mapping_dict_is_active", "is_active"),
    )


class IRPropertySchema(Base):
    """IR property schema model."""

    __tablename__ = "ir_property_schemas"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    ir_type: Mapped[str] = mapped_column(String(100), nullable=False)
    ir_subtype: Mapped[str] = mapped_column(String(100), nullable=False)
    ir_property_name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(50), nullable=False)
    required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    default_value: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    value_transformer: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)


class IRPropertyMapping(Base):
    """IR property mapping model."""

    __tablename__ = "ir_property_mappings"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    tool_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("etl_tools.id"), nullable=False
    )
    component: Mapped[str] = mapped_column(String(255), nullable=False)
    ir_type: Mapped[str] = mapped_column(String(100), nullable=False)
    ir_subtype: Mapped[str] = mapped_column(String(100), nullable=False)
    ir_property_name: Mapped[str] = mapped_column(String(255), nullable=False)
    tool_property_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value_transformer: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    default_value: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )

    # Relationships
    tool: Mapped["ETLTool"] = relationship("ETLTool", back_populates="ir_property_mappings")

    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint(
            "tool_id", "component", "ir_type", "ir_subtype", "ir_property_name", name="_ir_prop_mapping_uc"
        ),
        Index("idx_ir_prop_mapping_tool_id", "tool_id"),
        Index("idx_ir_prop_mapping_ir_prop", "ir_type", "ir_subtype", "ir_property_name"),
        Index("idx_ir_prop_mapping_tool_component", "tool_id", "component"),
    )


class TargetComponentTemplate(Base):
    """Target component template model."""

    __tablename__ = "target_component_templates"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    target_tool_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("etl_tools.id"), nullable=False
    )
    component_type: Mapped[str] = mapped_column(String(255), nullable=False)
    template_content: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    app_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    target_tool: Mapped["ETLTool"] = relationship("ETLTool", back_populates="target_component_templates")

    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint("target_tool_id", "component_type", name="_target_template_uc"),
        Index("idx_target_template_tool_id", "target_tool_id"),
        Index("idx_target_template_component_type", "component_type"),
        Index("idx_target_template_tool_component_active", "target_tool_id", "component_type", "is_active"),
    )


class JobExecutionLog(Base):
    """Job execution log model for detailed tracking."""

    __tablename__ = "job_execution_logs"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    migration_job_id: Mapped[UUID] = mapped_column(
        PostgresUUID(as_uuid=True), ForeignKey("migration_jobs.id"), nullable=False
    )
    stage: Mapped[ExecutionStage] = mapped_column(SQLEnum(ExecutionStage), nullable=False)
    status: Mapped[ExecutionStatus] = mapped_column(SQLEnum(ExecutionStatus), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB, name="metadata", nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )

    # Relationships
    migration_job: Mapped["MigrationJob"] = relationship("MigrationJob", back_populates="execution_logs")

    # Indexes
    __table_args__ = (
        Index("idx_job_exec_logs_job_id", "migration_job_id"),
        Index("idx_job_exec_logs_job_stage", "migration_job_id", "stage"),
    )


class Event(Base):
    """Event tracking model."""

    __tablename__ = "events"

    id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[EntityType] = mapped_column(SQLEnum(EntityType), nullable=False)
    entity_id: Mapped[UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[EventStatus] = mapped_column(
        SQLEnum(EventStatus), nullable=False, default=EventStatus.PENDING
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.datetime.utcnow, nullable=False
    )
    processed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
