# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Database Views Module for Material Flow Table Database.

This module defines all database views used for performance optimization.
Views provide denormalized data access for common queries, reducing
complex JOIN operations and improving query performance.

KEY VIEWS:
    1. v_active_parts_full:
        - Complete part information with all related data
        - Denormalized view for active parts (is_active = true)
        - Used for part details display and search
        
    2. v_part_history:
        - Complete part history with change source tracking
        - Shows all versions with breakpoint and classification info
        - Used for audit and rollback operations

    3. v_breakpoint_summary:
        - Breakpoint statistics and summary information
        - Aggregated data for reporting

    4. v_part_change_summary:
        - Summary of changes per part with counts and latest status

    5. v_breakpoint_details:
        - Detailed breakpoint information with counts
        - Used for breakpoint search and filtering

    6. v_parts_by_breakpoint:
        - Parts affected by each breakpoint
        - Used for detailed breakpoint change analysis

    7. v_breakpoint_changes_summary:
        - Aggregated monthly statistics
        - Used for dashboards and analytics

VERSIONING STRATEGY:
    - Views are created during database initialization
    - Views are re-created on migration if structure changes
    - Views do not store data (they are virtual tables)
    - Materialized views can be added later if needed

DEPENDENCIES:
    - PostgreSQL 12+ (supports CREATE OR REPLACE VIEW)
    - SQLAlchemy 1.4.54+ for ORM mapping

USAGE:
    from database.views import (
        ActivePartsFullView,
        PartHistoryView,
        BreakpointDetailsView,
        create_views
    )
    
    # In API endpoint:
    part = session.query(ActivePartsFullView).filter(
        ActivePartsFullView.part_number == 'ABC-123'
    ).first()

VERSION: 1.0.0
Compatibility: Python 3.14.4+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-18
License: MIT
Status: Production
"""

# Standard library imports
from typing import Optional

# Third-party imports
from sqlalchemy import (
    Column, String, Integer, Numeric, SmallInteger, Boolean,
    DateTime, Text, text
)
from sqlalchemy.exc import SQLAlchemyError

# Local imports
from config import get_logger
from database.database import Base

# Logger setup
logger = get_logger(__name__)


# ============================================================================
# SQL VIEW DEFINITIONS
# ============================================================================

# View 1: Active Parts Full (all data denormalized)
VIEW_ACTIVE_PARTS_FULL_SQL = """
CREATE OR REPLACE VIEW v_active_parts_full AS
SELECT 
    -- Part data
    p.part_id,
    p.part_number,
    p.part_name,
    p.part_weight_kg,
    p.version_number,
    p.original_part_id,
    p.created_at,
    p.deactivated_by_breakpoint_id AS part_deactivated_by_breakpoint_id,
    p.is_fully_deactivated,
    
    -- Supplier data
    s.supplier_id AS supplier_id,
    s.supplier_name,
    s.localization,
    s.city,
    s.street,
    s.building,
    
    -- Box data
    b.box_id,
    b.box_type,
    b.box_number,
    b.box_length_mm,
    b.box_width_mm,
    b.box_height_mm,
    b.box_vol_m3,
    b.box_area_m2,
    b.box_stacking,
    b.box_weight_kg,
    ptb.part_per_box,
    
    -- Pallet data
    pl.pallet_id,
    pl.pallet_type,
    pl.pallet_number,
    pl.pallet_length_mm,
    pl.pallet_width_mm,
    pl.pallet_height_mm,
    pl.pallet_vol_m3,
    pl.pallet_area_m2,
    pl.pallet_stacking,
    pl.pallet_weight_kg,
    btp.box_per_pallet,
    
    -- Line data
    l.line_id,
    l.line_code,
    l.line_name,
    w.workshop_code,
    w.workshop_name,
    w.workshop_id AS workshop_id,
    
    -- Model & Configuration data
    ptm.model_id,
    m.model_code,
    m.model_name,
    ptm.is_active,
    ptm.deactivated_at,
    ptm.part_per_vehicle,
    ptm.deactivated_by_breakpoint_id AS model_deactivated_by_breakpoint_id,
    c.configuration_id,
    c.configuration,
    c.transmission,
    
    -- Breakpoint classification (latest)
    bd.breakpoint_id AS latest_breakpoint_id,
    bd.breakpoint_number AS latest_breakpoint_number,
    bd.breakpoint_date AS latest_breakpoint_date,
    bd.change_domain AS latest_change_domain,
    bd.change_nature AS latest_change_nature,
    bd.breakpoint_status AS latest_breakpoint_status

FROM part_data p

-- Supplier (1:1)
LEFT JOIN supplier_data s ON p.supplier_id = s.supplier_id

-- Box (1:1 via part_to_box)
LEFT JOIN part_to_box ptb ON p.part_id = ptb.part_id
LEFT JOIN box_data b ON ptb.box_id = b.box_id

-- Pallet (1:1 via box_to_pallet)
LEFT JOIN box_to_pallet btp ON p.part_id = btp.part_id AND btp.box_id = ptb.box_id
LEFT JOIN pallet_data pl ON btp.pallet_id = pl.pallet_id

-- Line (1:1 via part_to_line)
LEFT JOIN part_to_line ptl ON p.part_id = ptl.part_id
LEFT JOIN line_data l ON ptl.line_id = l.line_id
LEFT JOIN workshop_data w ON l.workshop_id = w.workshop_id

-- Model & Configuration (1:1 via part_to_model)
LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
LEFT JOIN model_data m ON ptm.model_id = m.model_id
LEFT JOIN configuration_data c ON ptm.configuration_id = c.configuration_id

-- Latest breakpoint classification (for active version)
LEFT JOIN part_to_breakpoint ptbkp ON p.part_id = ptbkp.new_part_id
LEFT JOIN breakpoint_data bd ON ptbkp.breakpoint_id = bd.breakpoint_id

WHERE ptm.is_active = true;
"""


# View 2: Part History with change tracking
VIEW_PART_HISTORY_SQL = """
CREATE OR REPLACE VIEW v_part_history AS
SELECT 
    p.part_id,
    p.part_number,
    p.version_number,
    p.part_name,
    p.part_weight_kg,
    p.created_at,
    p.original_part_id,
    p.deactivated_by_breakpoint_id AS part_deactivated_by_breakpoint_id,
    
    s.supplier_id,
    s.supplier_name,
    s.localization,
    
    ptm.model_id,
    m.model_code,
    m.model_name,
    ptm.is_active,
    ptm.deactivated_at,
    ptm.part_per_vehicle,
    c.configuration,
    c.transmission,
    
    ptb.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.description,
    bd.solution,
    bd.change_domain,
    bd.change_nature,
    bd.batch_plan,
    bd.batch_fact,
    
    -- Source tracking
    CASE 
        WHEN bd.breakpoint_number LIKE 'MAN-%' THEN 'manual'
        WHEN bd.breakpoint_number LIKE 'BP-%' THEN 'automatic'
        ELSE 'unknown'
    END AS change_source,
    
    -- Type of change (from part_to_breakpoint)
    CASE
        WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NULL THEN 'ADD'
        WHEN ptb.new_part_id IS NULL AND ptb.old_part_id IS NOT NULL THEN 'DELETE'
        WHEN ptb.new_part_id = ptb.old_part_id THEN 'UPDATE'
        WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NOT NULL THEN 'REPLACE'
        ELSE 'UNKNOWN'
    END AS change_action_type,
    
    -- Part identifiers in transition
    ptb.new_part_id AS transition_new_part_id,
    ptb.old_part_id AS transition_old_part_id

FROM part_data p

LEFT JOIN supplier_data s ON p.supplier_id = s.supplier_id

LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
LEFT JOIN model_data m ON ptm.model_id = m.model_id
LEFT JOIN configuration_data c ON ptm.configuration_id = c.configuration_id

LEFT JOIN part_to_breakpoint ptb ON p.part_id = ptb.new_part_id OR p.part_id = ptb.old_part_id
LEFT JOIN breakpoint_data bd ON ptb.breakpoint_id = bd.breakpoint_id

ORDER BY p.part_number, p.version_number DESC;
"""


# View 3: Breakpoint Summary
VIEW_BREAKPOINT_SUMMARY_SQL = """
CREATE OR REPLACE VIEW v_breakpoint_summary AS
SELECT 
    bd.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.change_domain,
    bd.change_nature,
    bd.description,
    bd.input_date,
    
    -- Count of parts affected
    COUNT(DISTINCT ptb.new_part_id) AS new_parts_count,
    COUNT(DISTINCT ptb.old_part_id) AS old_parts_count,
    COUNT(DISTINCT ptb.model_id) AS models_affected_count,
    
    -- Aggregate of models affected
    STRING_AGG(DISTINCT m.model_code, ', ' ORDER BY m.model_code) AS models_affected,
    
    -- Count by action type
    COUNT(CASE WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NULL THEN 1 END) AS add_count,
    COUNT(CASE WHEN ptb.new_part_id IS NULL AND ptb.old_part_id IS NOT NULL THEN 1 END) AS delete_count,
    COUNT(CASE WHEN ptb.new_part_id = ptb.old_part_id THEN 1 END) AS update_count,
    COUNT(CASE WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NOT NULL AND ptb.new_part_id != ptb.old_part_id THEN 1 END) AS replace_count,
    
    -- Source
    CASE 
        WHEN bd.breakpoint_number LIKE 'MAN-%' THEN 'manual'
        ELSE 'automatic'
    END AS source

FROM breakpoint_data bd
LEFT JOIN part_to_breakpoint ptb ON bd.breakpoint_id = ptb.breakpoint_id
LEFT JOIN model_data m ON ptb.model_id = m.model_id

GROUP BY 
    bd.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.change_domain,
    bd.change_nature,
    bd.description,
    bd.input_date

ORDER BY bd.breakpoint_date DESC;
"""


# View 4: Part Change Summary (per part)
VIEW_PART_CHANGE_SUMMARY_SQL = """
CREATE OR REPLACE VIEW v_part_change_summary AS
SELECT 
    p.part_id,
    p.part_number,
    p.version_number,
    p.part_name,
    p.created_at AS version_created_at,
    
    -- Latest status
    ptm.is_active AS current_is_active,
    ptm.deactivated_at AS current_deactivated_at,
    ptm.model_id,
    m.model_code,
    
    -- Change counts
    COUNT(DISTINCT ptb.breakpoint_id) AS total_changes,
    COUNT(DISTINCT CASE WHEN bd.breakpoint_number LIKE 'MAN-%' THEN ptb.breakpoint_id END) AS manual_changes,
    COUNT(DISTINCT CASE WHEN bd.breakpoint_number LIKE 'BP-%' THEN ptb.breakpoint_id END) AS automatic_changes,
    
    -- Latest change
    MAX(bd.breakpoint_date) AS latest_change_date,
    MAX(bd.breakpoint_number) AS latest_breakpoint_number,
    
    -- Domain summary
    STRING_AGG(DISTINCT bd.change_domain, ', ' ORDER BY bd.change_domain) AS domains_affected,
    
    -- Total versions
    COUNT(DISTINCT p2.part_id) AS total_versions

FROM part_data p

LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
LEFT JOIN model_data m ON ptm.model_id = m.model_id

LEFT JOIN part_to_breakpoint ptb ON p.part_id = ptb.new_part_id OR p.part_id = ptb.old_part_id
LEFT JOIN breakpoint_data bd ON ptb.breakpoint_id = bd.breakpoint_id

LEFT JOIN part_data p2 ON p2.original_part_id = COALESCE(p.original_part_id, p.part_id)

GROUP BY 
    p.part_id,
    p.part_number,
    p.version_number,
    p.part_name,
    p.created_at,
    ptm.is_active,
    ptm.deactivated_at,
    ptm.model_id,
    m.model_code

ORDER BY p.part_number, p.version_number DESC;
"""


# View 5: Breakpoint Details (for filtering and search)
VIEW_BREAKPOINT_DETAILS_SQL = """
CREATE OR REPLACE VIEW v_breakpoint_details AS
SELECT 
    bd.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.change_domain,
    bd.change_nature,
    bd.description,
    bd.solution,
    bd.batch_plan,
    bd.batch_fact,
    bd.input_date,
    
    CASE 
        WHEN bd.breakpoint_number LIKE 'MAN-%' THEN 'manual'
        WHEN bd.breakpoint_number LIKE 'BP-%' THEN 'automatic'
        ELSE 'unknown'
    END AS source,
    
    COUNT(DISTINCT ptb.new_part_id) FILTER (WHERE ptb.new_part_id IS NOT NULL) AS new_parts_count,
    COUNT(DISTINCT ptb.old_part_id) FILTER (WHERE ptb.old_part_id IS NOT NULL) AS old_parts_count,
    COUNT(DISTINCT ptb.model_id) AS models_affected_count,
    
    COUNT(CASE WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NULL THEN 1 END) AS add_count,
    COUNT(CASE WHEN ptb.new_part_id IS NULL AND ptb.old_part_id IS NOT NULL THEN 1 END) AS delete_count,
    COUNT(CASE WHEN ptb.new_part_id = ptb.old_part_id THEN 1 END) AS update_count,
    COUNT(CASE WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NOT NULL AND ptb.new_part_id != ptb.old_part_id THEN 1 END) AS replace_count,
    
    STRING_AGG(DISTINCT m.model_code, ', ' ORDER BY m.model_code) AS models_affected

FROM breakpoint_data bd
LEFT JOIN part_to_breakpoint ptb ON bd.breakpoint_id = ptb.breakpoint_id
LEFT JOIN model_data m ON ptb.model_id = m.model_id

GROUP BY 
    bd.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.change_domain,
    bd.change_nature,
    bd.description,
    bd.solution,
    bd.batch_plan,
    bd.batch_fact,
    bd.input_date

ORDER BY bd.breakpoint_date DESC;
"""


# View 6: Parts by Breakpoint
VIEW_PARTS_BY_BREAKPOINT_SQL = """
CREATE OR REPLACE VIEW v_parts_by_breakpoint AS
SELECT 
    bd.breakpoint_id,
    bd.breakpoint_number,
    bd.breakpoint_date,
    bd.breakpoint_status,
    bd.change_domain,
    bd.change_nature,
    
    old_p.part_id AS old_part_id,
    old_p.part_number AS old_part_number,
    old_p.part_name AS old_part_name,
    old_p.version_number AS old_version_number,
    old_p.part_weight_kg AS old_part_weight_kg,
    old_s.supplier_name AS old_supplier_name,
    
    new_p.part_id AS new_part_id,
    new_p.part_number AS new_part_number,
    new_p.part_name AS new_part_name,
    new_p.version_number AS new_version_number,
    new_p.part_weight_kg AS new_part_weight_kg,
    new_s.supplier_name AS new_supplier_name,
    
    ptb.model_id,
    m.model_code,
    m.model_name,
    
    CASE
        WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NULL THEN 'ADD'
        WHEN ptb.new_part_id IS NULL AND ptb.old_part_id IS NOT NULL THEN 'DELETE'
        WHEN ptb.new_part_id = ptb.old_part_id THEN 'UPDATE'
        WHEN ptb.new_part_id IS NOT NULL AND ptb.old_part_id IS NOT NULL THEN 'REPLACE'
        ELSE 'UNKNOWN'
    END AS action_type

FROM part_to_breakpoint ptb

LEFT JOIN breakpoint_data bd ON ptb.breakpoint_id = bd.breakpoint_id
LEFT JOIN model_data m ON ptb.model_id = m.model_id

LEFT JOIN part_data old_p ON ptb.old_part_id = old_p.part_id
LEFT JOIN supplier_data old_s ON old_p.supplier_id = old_s.supplier_id

LEFT JOIN part_data new_p ON ptb.new_part_id = new_p.part_id
LEFT JOIN supplier_data new_s ON new_p.supplier_id = new_s.supplier_id

ORDER BY bd.breakpoint_date DESC, bd.breakpoint_number;
"""


# View 7: Breakpoint Changes Summary (for dashboard)
VIEW_BREAKPOINT_CHANGES_SUMMARY_SQL = """
CREATE OR REPLACE VIEW v_breakpoint_changes_summary AS
SELECT 
    DATE_TRUNC('month', bd.breakpoint_date) AS month,
    bd.change_domain,
    bd.change_nature,
    CASE 
        WHEN bd.breakpoint_number LIKE 'MAN-%' THEN 'manual'
        ELSE 'automatic'
    END AS source,
    COUNT(*) AS breakpoint_count,
    COUNT(DISTINCT ptb.part_id) AS parts_affected_count,
    COUNT(DISTINCT ptb.model_id) AS models_affected_count
    
FROM breakpoint_data bd
LEFT JOIN part_to_breakpoint ptb ON bd.breakpoint_id = ptb.breakpoint_id

GROUP BY 
    DATE_TRUNC('month', bd.breakpoint_date),
    bd.change_domain,
    bd.change_nature,
    source

ORDER BY month DESC, breakpoint_count DESC;
"""


# ============================================================================
# SQLALCHEMY ORM MAPPINGS FOR VIEWS
# ============================================================================

class ActivePartsFullView(Base):
    """
    ORM mapping for v_active_parts_full view.
    
    Provides denormalized access to all part data with related entities.
    Use this view for read operations where performance is critical.
    """
    __tablename__ = 'v_active_parts_full'
    __table_args__ = {'info': {'is_view': True}}

    # Part data
    part_id = Column(String(40), primary_key=True)
    part_number = Column(String(50))
    part_name = Column(String(100))
    part_weight_kg = Column(Numeric(5, 2))
    version_number = Column(Integer)
    original_part_id = Column(String(40))
    created_at = Column(DateTime)
    part_deactivated_by_breakpoint_id = Column(String(40))
    is_fully_deactivated = Column(Boolean)

    # Supplier data
    supplier_id = Column(String(40))
    supplier_name = Column(String(200))
    localization = Column(String(20))
    city = Column(String(50))
    street = Column(String(100))
    building = Column(String(10))

    # Box data
    box_id = Column(String(40))
    box_type = Column(String(20))
    box_number = Column(String(50))
    box_length_mm = Column(SmallInteger)
    box_width_mm = Column(SmallInteger)
    box_height_mm = Column(SmallInteger)
    box_vol_m3 = Column(Numeric(8, 4))
    box_area_m2 = Column(Numeric(8, 4))
    box_stacking = Column(SmallInteger)
    box_weight_kg = Column(Numeric(5, 2))
    part_per_box = Column(Integer)

    # Pallet data
    pallet_id = Column(String(40))
    pallet_type = Column(String(20))
    pallet_number = Column(String(50))
    pallet_length_mm = Column(SmallInteger)
    pallet_width_mm = Column(SmallInteger)
    pallet_height_mm = Column(SmallInteger)
    pallet_vol_m3 = Column(Numeric(8, 4))
    pallet_area_m2 = Column(Numeric(8, 4))
    pallet_stacking = Column(SmallInteger)
    pallet_weight_kg = Column(Numeric(5, 2))
    box_per_pallet = Column(SmallInteger)

    # Line data
    line_id = Column(String(40))
    line_code = Column(String(10))
    line_name = Column(String(50))
    workshop_code = Column(String(20))
    workshop_name = Column(String(50))
    workshop_id = Column(String(40))

    # Model & Configuration data
    model_id = Column(String(40))
    model_code = Column(String(20))
    model_name = Column(String(50))
    is_active = Column(Boolean)
    deactivated_at = Column(DateTime)
    part_per_vehicle = Column(SmallInteger)
    model_deactivated_by_breakpoint_id = Column(String(40))
    configuration_id = Column(String(40))
    configuration = Column(String(20))
    transmission = Column(String(100))

    # Latest breakpoint classification
    latest_breakpoint_id = Column(String(40))
    latest_breakpoint_number = Column(String(10))
    latest_breakpoint_date = Column(DateTime)
    latest_change_domain = Column(String(20))
    latest_change_nature = Column(String(20))
    latest_breakpoint_status = Column(String(20))


class PartHistoryView(Base):
    """
    ORM mapping for v_part_history view.
    
    Provides complete part history with change source tracking.
    Use this view for audit and version history queries.
    """
    __tablename__ = 'v_part_history'
    __table_args__ = {'info': {'is_view': True}}

    part_id = Column(String(40), primary_key=True)
    part_number = Column(String(50))
    version_number = Column(Integer)
    part_name = Column(String(100))
    part_weight_kg = Column(Numeric(5, 2))
    created_at = Column(DateTime)
    original_part_id = Column(String(40))
    part_deactivated_by_breakpoint_id = Column(String(40))

    supplier_id = Column(String(40))
    supplier_name = Column(String(200))
    localization = Column(String(20))

    model_id = Column(String(40))
    model_code = Column(String(20))
    model_name = Column(String(50))
    is_active = Column(Boolean)
    deactivated_at = Column(DateTime)
    part_per_vehicle = Column(SmallInteger)
    configuration = Column(String(20))
    transmission = Column(String(100))

    breakpoint_id = Column(String(40))
    breakpoint_number = Column(String(10))
    breakpoint_date = Column(DateTime)
    breakpoint_status = Column(String(20))
    description = Column(Text)
    solution = Column(Text)
    change_domain = Column(String(20))
    change_nature = Column(String(20))
    batch_plan = Column(String(10))
    batch_fact = Column(String(10))

    change_source = Column(String(20))
    change_action_type = Column(String(20))
    transition_new_part_id = Column(String(40))
    transition_old_part_id = Column(String(40))


class BreakpointSummaryView(Base):
    """
    ORM mapping for v_breakpoint_summary view.
    
    Provides aggregated breakpoint statistics.
    Use this view for reporting and dashboard queries.
    """
    __tablename__ = 'v_breakpoint_summary'
    __table_args__ = {'info': {'is_view': True}}

    breakpoint_id = Column(String(40), primary_key=True)
    breakpoint_number = Column(String(10))
    breakpoint_date = Column(DateTime)
    breakpoint_status = Column(String(20))
    change_domain = Column(String(20))
    change_nature = Column(String(20))
    description = Column(Text)
    input_date = Column(DateTime)

    new_parts_count = Column(Integer)
    old_parts_count = Column(Integer)
    models_affected_count = Column(Integer)
    models_affected = Column(Text)

    add_count = Column(Integer)
    delete_count = Column(Integer)
    update_count = Column(Integer)
    replace_count = Column(Integer)

    source = Column(String(20))


class PartChangeSummaryView(Base):
    """
    ORM mapping for v_part_change_summary view.
    
    Provides change summary per part.
    Use this view for part analytics and change tracking.
    """
    __tablename__ = 'v_part_change_summary'
    __table_args__ = {'info': {'is_view': True}}

    part_id = Column(String(40), primary_key=True)
    part_number = Column(String(50))
    version_number = Column(Integer)
    part_name = Column(String(100))
    version_created_at = Column(DateTime)

    current_is_active = Column(Boolean)
    current_deactivated_at = Column(DateTime)
    model_id = Column(String(40))
    model_code = Column(String(20))

    total_changes = Column(Integer)
    manual_changes = Column(Integer)
    automatic_changes = Column(Integer)

    latest_change_date = Column(DateTime)
    latest_breakpoint_number = Column(String(10))
    domains_affected = Column(Text)
    total_versions = Column(Integer)


class BreakpointDetailsView(Base):
    """
    ORM mapping for v_breakpoint_details view.
    
    Provides detailed breakpoint information with counts.
    Use this view for breakpoint search and filtering.
    """
    __tablename__ = 'v_breakpoint_details'
    __table_args__ = {'info': {'is_view': True}}

    breakpoint_id = Column(String(40), primary_key=True)
    breakpoint_number = Column(String(10))
    breakpoint_date = Column(DateTime)
    breakpoint_status = Column(String(20))
    change_domain = Column(String(20))
    change_nature = Column(String(20))
    description = Column(Text)
    solution = Column(Text)
    batch_plan = Column(String(10))
    batch_fact = Column(String(10))
    input_date = Column(DateTime)

    source = Column(String(20))

    new_parts_count = Column(Integer)
    old_parts_count = Column(Integer)
    models_affected_count = Column(Integer)

    add_count = Column(Integer)
    delete_count = Column(Integer)
    update_count = Column(Integer)
    replace_count = Column(Integer)

    models_affected = Column(Text)


class PartsByBreakpointView(Base):
    """
    ORM mapping for v_parts_by_breakpoint view.
    
    Provides parts affected by each breakpoint.
    Use this view for detailed breakpoint change analysis.
    """
    __tablename__ = 'v_parts_by_breakpoint'
    __table_args__ = {'info': {'is_view': True}}

    breakpoint_id = Column(String(40), primary_key=True)
    breakpoint_number = Column(String(10))
    breakpoint_date = Column(DateTime)
    breakpoint_status = Column(String(20))
    change_domain = Column(String(20))
    change_nature = Column(String(20))

    old_part_id = Column(String(40))
    old_part_number = Column(String(50))
    old_part_name = Column(String(100))
    old_version_number = Column(Integer)
    old_part_weight_kg = Column(Numeric(5, 2))
    old_supplier_name = Column(String(200))

    new_part_id = Column(String(40))
    new_part_number = Column(String(50))
    new_part_name = Column(String(100))
    new_version_number = Column(Integer)
    new_part_weight_kg = Column(Numeric(5, 2))
    new_supplier_name = Column(String(200))

    model_id = Column(String(40))
    model_code = Column(String(20))
    model_name = Column(String(50))

    action_type = Column(String(20))


class BreakpointChangesSummaryView(Base):
    """
    ORM mapping for v_breakpoint_changes_summary view.
    
    Provides aggregated monthly statistics.
    Use this view for dashboards and analytics.
    """
    __tablename__ = 'v_breakpoint_changes_summary'
    __table_args__ = {'info': {'is_view': True}}

    month = Column(DateTime)
    change_domain = Column(String(20))
    change_nature = Column(String(20))
    source = Column(String(20))
    breakpoint_count = Column(Integer)
    parts_affected_count = Column(Integer)
    models_affected_count = Column(Integer)


# ============================================================================
# VIEW DEFINITIONS DICT (for batch operations)
# ============================================================================

VIEW_DEFINITIONS = {
    'v_active_parts_full': VIEW_ACTIVE_PARTS_FULL_SQL,
    'v_part_history': VIEW_PART_HISTORY_SQL,
    'v_breakpoint_summary': VIEW_BREAKPOINT_SUMMARY_SQL,
    'v_part_change_summary': VIEW_PART_CHANGE_SUMMARY_SQL,
    'v_breakpoint_details': VIEW_BREAKPOINT_DETAILS_SQL,
    'v_parts_by_breakpoint': VIEW_PARTS_BY_BREAKPOINT_SQL,
    'v_breakpoint_changes_summary': VIEW_BREAKPOINT_CHANGES_SUMMARY_SQL,
}

# Views that require materialization (for future use)
MATERIALIZED_VIEWS = {
    # 'v_active_parts_mat': VIEW_ACTIVE_PARTS_FULL_SQL,
}


# ============================================================================
# PUBLIC FUNCTIONS WITH ERROR HANDLING
# ============================================================================

def create_views(engine, drop_existing: bool = False) -> None:
    """
    Create all database views.
    
    Args:
        engine: SQLAlchemy engine
        drop_existing: If True, drop existing views before creating
        
    Raises:
        SQLAlchemyError: On database errors
        ValueError: If engine is None
        Exception: On unexpected errors
    """
    # Validate engine
    if engine is None:
        logger.error("Cannot create views: engine is None")
        raise ValueError("Database engine cannot be None")

    logger.info("Starting database views creation (drop_existing=%s)", drop_existing)

    try:
        with engine.connect() as connection:
            # Drop existing views if requested
            if drop_existing:
                for view_name in reversed(list(VIEW_DEFINITIONS.keys())):
                    try:
                        connection.execute(text(f"DROP VIEW IF EXISTS {view_name} CASCADE;"))
                        logger.info("Dropped existing view: %s", view_name)
                    except SQLAlchemyError as e:
                        logger.warning(
                            "SQLAlchemy error dropping view %s: %s",
                            view_name, str(e)
                        )
                        # Continue with other views
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            "Data error dropping view %s: %s",
                            view_name, str(e)
                        )
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error dropping view %s: %s",
                            view_name, unexpected_error
                        )

            # Create views
            for view_name, view_sql in VIEW_DEFINITIONS.items():
                try:
                    connection.execute(text(view_sql))
                    logger.info("Created view: %s", view_name)
                except SQLAlchemyError as e:
                    logger.error(
                        "SQLAlchemy error creating view %s: %s",
                        view_name, str(e)
                    )
                    raise
                except (ValueError, TypeError, AttributeError) as e:
                    logger.error(
                        "Data error creating view %s: %s",
                        view_name, str(e)
                    )
                    raise RuntimeError(f"Invalid view definition for {view_name}: {e}") from e
                except Exception as unexpected_error:
                    logger.error(
                        "Unexpected error creating view %s: %s",
                        view_name, unexpected_error,
                        exc_info=True
                    )
                    raise RuntimeError(
                        f"Unexpected error creating view {view_name}: {unexpected_error}"
                    ) from unexpected_error

            connection.commit()
            logger.info("All %d database views created successfully", len(VIEW_DEFINITIONS))

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error creating views: %s", str(e))
        raise
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Data error creating views: %s", str(e))
        raise RuntimeError(f"Invalid configuration for views: {e}") from e
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error creating views: %s",
            unexpected_error,
            exc_info=True
        )
        raise RuntimeError(f"Unexpected error creating views: {unexpected_error}") from unexpected_error


def drop_views(engine) -> None:
    """
    Drop all database views.
    
    Args:
        engine: SQLAlchemy engine
        
    Raises:
        SQLAlchemyError: On database errors
        ValueError: If engine is None
        Exception: On unexpected errors
    """
    # Validate engine
    if engine is None:
        logger.error("Cannot drop views: engine is None")
        raise ValueError("Database engine cannot be None")

    logger.info("Starting database views drop")

    try:
        with engine.connect() as connection:
            dropped_count = 0
            for view_name in reversed(list(VIEW_DEFINITIONS.keys())):
                try:
                    connection.execute(text(f"DROP VIEW IF EXISTS {view_name} CASCADE;"))
                    logger.info("Dropped view: %s", view_name)
                    dropped_count += 1
                except SQLAlchemyError as e:
                    logger.warning(
                        "SQLAlchemy error dropping view %s: %s",
                        view_name, str(e)
                    )
                    # Continue with other views
                except (ValueError, TypeError, AttributeError) as e:
                    logger.warning(
                        "Data error dropping view %s: %s",
                        view_name, str(e)
                    )
                except Exception as unexpected_error:
                    logger.warning(
                        "Unexpected error dropping view %s: %s",
                        view_name, unexpected_error
                    )

            connection.commit()
            logger.info("Dropped %d views successfully", dropped_count)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error dropping views: %s", str(e))
        raise
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Data error dropping views: %s", str(e))
        raise RuntimeError(f"Invalid configuration for views: {e}") from e
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error dropping views: %s",
            unexpected_error,
            exc_info=True
        )
        raise RuntimeError(f"Unexpected error dropping views: {unexpected_error}") from unexpected_error


def view_exists(engine, view_name: str) -> bool:
    """
    Check if a view exists in the database.
    
    Args:
        engine: SQLAlchemy engine
        view_name: Name of the view to check
        
    Returns:
        bool: True if view exists, False otherwise
        
    Raises:
        ValueError: If engine is None or view_name is empty
    """
    # Validate parameters
    if engine is None:
        logger.error("Cannot check view existence: engine is None")
        raise ValueError("Database engine cannot be None")

    if not view_name or not view_name.strip():
        logger.error("Cannot check view existence: view_name is empty")
        raise ValueError("view_name cannot be empty")

    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.views 
                        WHERE table_name = :view_name
                    )
                """),
                {'view_name': view_name.strip()}
            ).scalar()

            exists = bool(result)
            logger.debug("View %s exists: %s", view_name, exists)
            return exists

    except SQLAlchemyError as e:
        logger.error(
            "SQLAlchemy error checking view %s: %s",
            view_name, str(e)
        )
        return False
    except (ValueError, TypeError, AttributeError) as e:
        logger.error(
            "Data error checking view %s: %s",
            view_name, str(e)
        )
        return False
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error checking view %s: %s",
            view_name, unexpected_error,
            exc_info=True
        )
        return False


def get_view_definition(engine, view_name: str) -> Optional[str]:
    """
    Get the definition of a view.
    
    Args:
        engine: SQLAlchemy engine
        view_name: Name of the view
        
    Returns:
        str: View definition or None if view doesn't exist
        
    Raises:
        ValueError: If engine is None or view_name is empty
    """
    # Validate parameters
    if engine is None:
        logger.error("Cannot get view definition: engine is None")
        raise ValueError("Database engine cannot be None")

    if not view_name or not view_name.strip():
        logger.error("Cannot get view definition: view_name is empty")
        raise ValueError("view_name cannot be empty")

    try:
        with engine.connect() as connection:
            # First check if view exists
            if not view_exists(engine, view_name):
                logger.debug("View %s does not exist", view_name)
                return None

            result = connection.execute(
                text("""
                    SELECT pg_get_viewdef(:view_name::regclass, true)
                """),
                {'view_name': view_name.strip()}
            ).scalar()

            if result:
                logger.debug("Retrieved definition for view %s (%d chars)",
                           view_name, len(str(result)))
            else:
                logger.debug("View %s exists but definition is empty", view_name)

            return result

    except SQLAlchemyError as e:
        logger.error(
            "SQLAlchemy error getting view definition for %s: %s",
            view_name, str(e)
        )
        return None
    except (ValueError, TypeError, AttributeError) as e:
        logger.error(
            "Data error getting view definition for %s: %s",
            view_name, str(e)
        )
        return None
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error getting view definition for %s: %s",
            view_name, unexpected_error,
            exc_info=True
        )
        return None


# ============================================================================
# PUBLIC INTERFACE
# ============================================================================

__all__ = [
    # ORM classes
    'ActivePartsFullView',
    'PartHistoryView',
    'BreakpointSummaryView',
    'PartChangeSummaryView',
    'BreakpointDetailsView',
    'PartsByBreakpointView',
    'BreakpointChangesSummaryView',

    # Functions
    'create_views',
    'drop_views',
    'view_exists',
    'get_view_definition',

    # Constants
    'VIEW_DEFINITIONS',
    'MATERIALIZED_VIEWS',
]
