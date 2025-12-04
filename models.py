"""
SQLAlchemy models used by ETL_Migrator.

These mirror the tables used by ETL_Weaver so both projects can
operate on the same database:
  - mapping_dictionary
  - talend_templates

IR storage is not finalized yet. When ready, you can add an IRJob
model here that stores IR JSON per job.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, UUID, ForeignKey
from sqlalchemy.sql import func

from db import Base


class AuditLog(Base):
    """Abstract base with common audit fields."""

    __abstract__ = True

    uploaded_by = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(String, nullable=False)


class MappingDictionary(AuditLog):
    """
    Component mapping dictionary (shared with ETL_Weaver).

    NOTE: Today ETL_Migrator mainly uses IR-specific mapping tables
    (e.g. ir_property_mappings). This model is provided so we can
    reuse ETL_Weaver-style mappings later without schema changes.
    """

    __tablename__ = "mapping_dictionary"

    id = Column(UUID, primary_key=True, index=True)
    tenant_id = Column(String, nullable=False)
    app_id = Column(String, nullable=False)
    datastage_type = Column(String, nullable=False)
    talend_component = Column(String, nullable=False)
    default_properties = Column(JSON, nullable=True)
    confidence = Column(String, nullable=True)

    # Learning fields
    usage_count = Column(Integer, default=0)
    last_used = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(String, default="active")
    learning_score = Column(Integer, default=0)


class TalendTemplate(AuditLog):
    """
    Talend component templates (shared with ETL_Weaver).
    """

    __tablename__ = "talend_templates"

    id = Column(UUID, primary_key=True, index=True)
    tenant_id = Column(String, nullable=False)
    app_id = Column(String, nullable=False)
    component_name = Column(String, nullable=False)
    template_xml = Column(Text, nullable=False)
    property_definitions = Column(JSON, nullable=True)
    description = Column(Text, nullable=True)
    category = Column(String, nullable=True)
    additional_context = Column(Text, nullable=True)


# Placeholder for future IR storage in DB.
# When you are ready to persist IR to Postgres, you can uncomment and
# migrate this model. TranslationService is already structured to
# optionally fetch IR by ID instead of from a file.
#
# class IRJob(AuditLog):
#     __tablename__ = "ir_jobs"
#     id = Column(UUID, primary_key=True, index=True)
#     tenant_id = Column(String, nullable=False)
#     app_id = Column(String, nullable=False)
#     ir_json = Column(JSON, nullable=False)
#     source_asg_id = Column(UUID, ForeignKey("asgs.id"), nullable=True)


