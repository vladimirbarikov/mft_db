# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Database Models Module for Material Flow Table Database with SQLAlchemy ORM.

DATABASE ENUM TYPES, MODELS AND TABLES:
1. ENUM TYPES:
    - LOCALIZATION_ENUM:
        - yes/no - Local or foreign supplier. 
        - no data - if no information about Localization status.
    - PACKAGING_TYPE_ENUM:
        - returnable/non-returnable - Packaging type. 
        - no data - if no information about Packaging type.
    - MODEL_CODES_ENUM:
        - a01, a08, b02, b04, b06, b16 - Platform codes.
        - no data - if no information about Model code.
    - MODEL_NAMES_ENUM:
        - jolion, h3, f7, f7x, dargo, h7 - Model names.
        - no data - if no information about Model name.
    - WORKSHOP_CODES_ENUM:
        - as, comp, paint, weld, stamp, en - Workshop codes.
        - no data - if no information about Workshop code.
    - WORKSHOP_NAMES_ENUM:
        - assembly, component, painting, welding, stamping, engine.
        - no data - if no information about Workshop name.
    - CONFIGURATION_ENUM:
        - comfort, elite, tech-plus, premium - Vehicle assembly levels.
        - no data - if no information about Configuration level.
    - BREAKPOINT_ACTION_ENUM:
        - replace, delete, add, update - Types of technical changes.
        - no data - if no information about Breakpoint action.

2. CORE ENTITY TABLES:
    - supplier_data      - Information about component suppliers
    - part_data          - Data about automotive components (parts)
    - box_data           - Packaging box specifications
    - pallet_data        - Pallet (platform) specifications
    - model_data         - Vehicle models
    - configuration_data - Vehicle configuration types (comfort, elite, etc.)
    - workshop_data      - Production workshops
    - line_data          - Production lines
    - breakpoint_data    - Technical changes (breakpoints)

3. JUNCTION TABLES:
    - part_to_box        - Relationship between parts and packaging boxes
    - box_to_pallet      - Relationship between parts, boxes and pallets
    - part_to_model      - Relationship between parts, vehicle models and configurations
    - part_to_line       - Relationship between parts and production lines
    - part_to_breakpoint - Part change history with before/after values and action type

STORED INFORMATION:

1. SUPPLIERS (supplier_data):
   - Basic data: identifier, name, location
   - Address: city, street, building
   - Localization: yes/no (local/foreign supplier)

2. PARTS (part_data):
   - Identifiers: PART_ID, PART_NUMBER
   - Description: name, weight (kg) with CheckConstraint (>= 0)
   - Supplier relationship (ForeignKey with RESTRICT on delete)
   - Soft delete support: is_active flag, deactivated_at timestamp
   - deactivated_by_breakpoint_id: References breakpoint that caused deactivation (SET NULL on delete)

3. PACKAGING (box_data, pallet_data):
   - Packaging type: returnable/non-returnable/no data
   - Dimensions: length, width, height (mm) with CheckConstraint (> 0)
   - Calculated parameters: volume (m³), area (m²)
   - Weight and maximum stacking capability with CheckConstraint (>= 0)
   - Automatic packaging number generation: 'R/N/no_d L-W-H' format
   - Real-time calculation via database triggers (Computed fields)
   - UniqueConstraint on (type, length, width, height) to prevent duplicates

4. PRODUCTION (workshop_data, line_data):
   - Workshops: code (as, comp, paint, etc.) and name
   - Lines: code, name, workshop affiliation (ForeignKey with RESTRICT on delete)

5. VEHICLE MODELS (model_data):
   - Model codes: a01, a08, b02, b04, b06, b16, no data
   - Model names: jolion, h3, f7, f7x, dargo, h7, no data

6. TECHNICAL CHANGES (breakpoint_data):
   - Breakpoint number (unique, mandatory) and date
   - Entry date into the system (auto-generated)
   - Batch information
   - Description of change

7. CHANGE HISTORY (part_to_breakpoint):
   - Links parts to breakpoints with model specificity
   - Action type: replace, delete, add, update, no data
   - Before-change values (snapshot from Excel):
        * part_number_before_change
        * supplier_name_before_change
        * localization_before_change (uses LOCALIZATION_ENUM)
        * line_name_before_change
   - After-change references to current master data:
        * supplier_id (SET NULL on delete, NULL for DELETE actions)
        * line_id (SET NULL on delete, NULL for DELETE actions)
   - Tracks model-specific part changes
   - Composite primary key: (part_id, breakpoint_id, model_id)
   - For DELETE action: supplier_id and line_id are NULL (no new supplier/line)

IMPLEMENTATION FEATURES:
    - UUID format: 32 hexadecimal characters + 4 hyphens = 36 characters total
    - Automatic ID generation with prefixes: SUP_, PRT_, BOX_, PLT_, MDL_, CFG_, WSP_, LNE_, BPT_
    - All ID fields use format: PREFIX_ + UUID = 40 characters total
    - Business rule validation through CheckConstraint:
        * Positive values for dimensions, weights, quantities
        * Non-negative values for stacking heights
    - Enum type support for categorized data (including breakpoint actions with 'no data')
    - Unique constraints for dimension combinations (box/pallet)
    - Complete bidirectional relationship mapping with back_populates
    - Real-time calculation of packaging volume/area via SQLAlchemy Computed fields
    - Automatic packaging number generation via SQLAlchemy Computed expressions
    - Composite foreign keys with RESTRICT/CASCADE/SET NULL rules:
        * RESTRICT: Prevents deletion of referenced records with dependencies
        * CASCADE: Automatically deletes child records when parent deleted
        * SET NULL: Sets foreign key to NULL when referenced record deleted
    - Comprehensive indexing strategy including GIN for text search
    - Optimized lazy loading strategies:
        * selectin: Used for collections to avoid N+1 queries
        * joined: Used for single relationships when always needed
        * select: Default lazy loading for less frequently accessed relationships

RELATIONSHIP STRUCTURE:
    - Supplier (1) ↔ (N) Part (N) ↔ (N) Box (N) ↔ (N) Pallet
    - Part (N) ↔ (N) Model (with Configuration)
    - Part (N) ↔ (N) Line (N) ↔ (1) Workshop
    - Part (N) ↔ (N) Breakpoint (change history) with Action type
    - Configuration (1) ↔ (N) PartToModel (N) ↔ (1) Model
    - Model (1) ↔ (N) PartToBreakpoint (N) ↔ (1) Part (model-specific changes)
    - Supplier (1) ↔ (N) PartToBreakpoint (supplier change history)
    - Line (1) ↔ (N) PartToBreakpoint (line change history)

CHANGE TRACKING (PartToBreakpoint):
    - Composite PK: (part_id, breakpoint_id, model_id)
    - Action field: replace/delete/add/update/no data
    - Before values: part_number, supplier_name, localization, line_name (snapshots)
    - After references: supplier_id, line_id (current master data)
    - Enables complete audit trail of part evolution per model
    - Business rules:
        * DELETE action: supplier_id and line_id must be NULL
        * REPLACE/UPDATE: supplier_id and line_id reference new values
        * ADD action: before-change fields typically NULL
        * Part soft deletion: is_active=False with deactivation breakpoint reference

DATABASE CONSTRAINTS SUMMARY:
    - Check Constraints:
        * part_weight_kg >= 0
        * box_weight_kg >= 0, box_length_mm > 0, box_width_mm > 0, box_height_mm > 0, box_stacking >= 0
        * pallet_weight_kg >= 0, pallet_length_mm > 0, pallet_width_mm > 0, pallet_height_mm > 0, pallet_stacking >= 0
        * part_per_box > 0, box_per_pallet > 0, part_per_vehicle > 0
    - Unique Constraints:
        * supplier_name
        * part_number
        * (box_type, box_length_mm, box_width_mm, box_height_mm) on box_data
        * (pallet_type, pallet_length_mm, pallet_width_mm, pallet_height_mm) on pallet_data
        * model_code, model_name on model_data
        * configuration on configuration_data
        * workshop_code, workshop_name on workshop_data
        * line_code on line_data
        * breakpoint_number on breakpoint_data

Version: 1.0.0
Compatibility: Python 3.12.3, SQLAlchemy 1.4.54, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-01-16
Last Modified: 2026-03-20
License: MIT
Status: Production
"""
# Third-party imports
from sqlalchemy import (
    CheckConstraint, Column, Computed, DateTime, Enum as SqlEnum,
    ForeignKey, func, Index, text, UniqueConstraint
)
from sqlalchemy.types import (
    Boolean, Integer, Numeric, String, SmallInteger, Text
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Base class
Base = declarative_base()

# ========== ENUM TYPES ==========
LOCALIZATION_ENUM = SqlEnum(
    'yes', 'no', 'no data',
    name='localization'
)

PACKAGING_TYPE_ENUM = SqlEnum(
    'returnable', 'non-returnable', 'no data',
    name='packaging_type'
)

MODEL_CODES_ENUM = SqlEnum(
    'a01', 'a08', 'b02', 'b04', 'b06', 'b16', 'no data',
    name='model_codes'
)

MODEL_NAMES_ENUM = SqlEnum(
    'jolion', 'h3', 'f7', 'f7x', 'dargo', 'h7', 'no data',
    name='model_names'
)

WORKSHOP_CODES_ENUM = SqlEnum(
    'as', 'comp', 'paint', 'weld', 'stamp', 'en', 'no data',
    name='workshop_codes'
)

WORKSHOP_NAMES_ENUM = SqlEnum(
    'assembly', 'component', 'painting', 'welding', 'stamping', 'engine', 'no data',
    name='workshop_names'
)

CONFIGURATION_ENUM = SqlEnum(
    'comfort', 'elite', 'tech-plus', 'premium', 'no data',
    name='configuration_types'
)

BREAKPOINT_ACTION_ENUM = SqlEnum(
    'replace', 'delete', 'add', 'update', 'no data',
    name='breakpoint_action'
)

# ========== CORE ENTITY TABLES ==========
class SupplierData(Base):
    '''
    Model defines a table for storing supplier's information.
    '''
    __tablename__ = 'supplier_data'
    __table_args__ = (
        Index('idx_supplier_name', 'supplier_name'),
        Index('idx_supplier_city', 'city'),
        Index('idx_supplier_localization', 'localization'),
        {
            'comment': """
            PURPOSE: Automotive component suppliers master data
            ---
            COLUMN DESCRIPTION:
            - supplier_id: Unique system identifier (SUP_ + 36-character UUID)
            - supplier_name: Legal entity name for contracts
            - location/city/street/building: Complete address
            - localization: 'yes'=local, 'no'=foreign (affects customs)
            ---
            RELATIONSHIPS:
            - One supplier produces many parts (1:N with PartData)
            ---
            BUSINESS RULES:
            - City is mandatory for logistics planning
            - Localization affects duty calculations
            - Supplier ID is immutable
            """
        },
    )
    supplier_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'SUP_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    supplier_name = Column(
        String(200),
        unique=True,
        nullable=False
    )
    location = Column(
        String(50),
        nullable=True
    )
    city = Column(
        String(50),
        nullable=True
    )
    street = Column(
        String(100),
        nullable=True
    )
    building = Column(
        String(10),
        nullable=True
    )
    localization = Column(
        LOCALIZATION_ENUM,
        nullable=False
    )
    # Relationships
    parts = relationship('PartData', back_populates='supplier', lazy='selectin')
    breakpoint_changes = relationship(
        'PartToBreakpoint',
        foreign_keys='PartToBreakpoint.supplier_id',
        back_populates='supplier',
        lazy='select'
    )

class PartData(Base):
    '''
    Model defines a table for storing component's information.
    '''
    __tablename__ = 'part_data'
    __table_args__ = (
        Index('idx_part_number', 'part_number'),
        Index('idx_part_name', 'part_name'),
        Index('idx_part_weight', 'part_weight_kg'),
        Index('idx_part_supplier_id', 'supplier_id'),
        Index('idx_part_active', 'is_active'),
        Index('idx_part_deactivated', 'deactivated_at'),
        {
            'comment': """
            PURPOSE: Automotive component master data
            ---
            COLUMN DESCRIPTION:
            - part_id: Unique system identifier (PRT_ + 36-character UUID)
            - part_number: Business number (99999AAA999 format)
            - part_name: Technical description
            - part_weight_kg: Weight in kilograms (precision 0.01)
            - supplier_id: References supplier_data
            - is_active: Whether part is currently in production (soft delete flag)
            - deactivated_at: When part was deactivated (if is_active=False)
            - deactivated_by_breakpoint_id: Which breakpoint caused deactivation
            ---
            RELATIONSHIPS:
            - Many-to-Many with: ModelData, LineData, BoxData, BreakpointData
            - Many-to-One with: SupplierData
            ---
            BUSINESS RULES:
            - Part number follows corporate standard
            - Weight critical for logistics costing
            - Each part has exactly one supplier
            - Parts are never physically deleted, only deactivated (soft delete)
            - Historical data in part_to_breakpoint remains valid
            """
        },
    )
    part_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'PRT_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    part_number = Column(
        String(50),
        unique=True,
        nullable=False
    )
    part_name = Column(
        String(100),
        nullable=True
    )
    part_weight_kg = Column(
        Numeric(5, 2),
        CheckConstraint('part_weight_kg >= 0'),
        nullable=True
    )
    supplier_id = Column(
        String(40),
        ForeignKey('supplier_data.supplier_id', ondelete='RESTRICT'),
        nullable=False,
        comment="The supplier cannot be deleted if there are part-numbers!"
    )
    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text('true'),
        comment="Whether part is currently in production. False = soft deleted"
    )
    deactivated_at = Column(
        DateTime,
        nullable=True,
        comment="When part was deactivated (set when is_active becomes False)"
    )
    deactivated_by_breakpoint_id = Column(
        String(40),
        ForeignKey('breakpoint_data.breakpoint_id', ondelete='SET NULL'),
        nullable=True,
        comment="Which breakpoint caused this part to be deactivated"
    )
    # Relationships
    supplier = relationship(
        'SupplierData',
        back_populates='parts',
        lazy='joined'
    )
    boxes = relationship(
        'PartToBox',
        back_populates='part',
        lazy='selectin'
    )
    models = relationship(
        'PartToModel',
        back_populates='part',
        lazy='selectin'
    )
    lines = relationship(
        'PartToLine',
        back_populates='part',
        lazy='select'
    )
    box_pallet_combinations = relationship(
        'BoxToPallet',
        back_populates='part',
        lazy='selectin'
    )
    breakpoints = relationship(
        'PartToBreakpoint',
        back_populates='part',
        lazy='select'
    )
    deactivation_breakpoint = relationship(
        'BreakpointData',
        foreign_keys=[deactivated_by_breakpoint_id],
        lazy='select'
    )


class BoxData(Base):
    '''
    Model defines a table for storing box's information.
    '''
    __tablename__ = 'box_data'
    __table_args__ = (
        Index('idx_box_number', 'box_number'),
        Index('idx_box_type', 'box_type'),
        Index('idx_box_dimensions',
              'box_length_mm', 'box_width_mm', 'box_height_mm'),
        UniqueConstraint('box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm',
                         name='unique_box_dimensions'),
        {
            'comment': """
            PURPOSE: Packaging specifications for logistics
            ---
            COLUMN DESCRIPTION:
            - box_id: Unique system identifier (BOX_ + 36-character UUID)
            - box_number: Auto-generated from type and dimensions (R/N L-W-H)
            - box_type: 'returnable' or 'non-returnable'
            - Dimensions: Length/Width/Height in mm
            - box_vol_m3/box_area_m2: Calculated from dimensions
            - box_stacking: Max safe box stacking height
            ---
            RELATIONSHIPS:
            - Many-to-Many with: PartData (via part_to_box)
            - Many-to-Many with: PalletData (via box_to_pallet)
            ---
            BUSINESS RULES:
            - Standard sizes:
                - 400x300x200mm - Small electronic components
                - 600x400x300mm - Middle nodes (sensors, control units)
                - 800x600x400mm - Large parts (headlights, seats)
                - 1200x800x600mm - Very large components (doors, hoods)
            - Dimensions in 5mm increments
            - Volume/area auto-calculated on insert/update
            - Box number auto-generated from type and dimensions
            """
        },
    )
    box_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'BOX_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    box_type = Column(
        PACKAGING_TYPE_ENUM,
        nullable=False
    )
    box_weight_kg = Column(
        Numeric(5, 2),
        CheckConstraint('box_weight_kg >= 0'),
        nullable=True
    )
    box_length_mm = Column(
        SmallInteger,
        CheckConstraint('box_length_mm > 0'),
        nullable=True
    )
    box_width_mm = Column(
        SmallInteger,
        CheckConstraint('box_width_mm > 0'),
        nullable=True
    )
    box_height_mm = Column(
        SmallInteger,
        CheckConstraint('box_height_mm > 0'),
        nullable=True
    )
    box_number = Column(String(50), Computed(
        """
        CASE
            WHEN box_type IS NOT NULL
                AND box_length_mm IS NOT NULL
                AND box_width_mm IS NOT NULL
                AND box_height_mm IS NOT NULL
            THEN 
                CASE 
                    WHEN box_type = 'returnable' THEN 'R ' || 
                        box_length_mm::text || '-' ||
                        box_width_mm::text || '-' ||
                        box_height_mm::text
                    WHEN box_type = 'non-returnable' THEN 'N ' ||
                        box_length_mm::text || '-' ||
                        box_width_mm::text || '-' ||
                        box_height_mm::text
                    WHEN box_type = 'no data' THEN 'no_d ' ||
                        box_length_mm::text || '-' ||
                        box_width_mm::text || '-' ||
                        box_height_mm::text
                    ELSE NULL
                END
            ELSE NULL
        END
        """
        ), nullable=True
    )
    box_vol_m3 = Column(Numeric(8, 4), Computed(
        """
        CASE
            WHEN box_length_mm IS NOT NULL
                 AND box_width_mm IS NOT NULL
                 AND box_height_mm IS NOT NULL
            THEN ROUND(
                (box_length_mm::numeric / 1000.0) *
                (box_width_mm::numeric / 1000.0) *
                (box_height_mm::numeric / 1000.0), 4
            )
            ELSE NULL
        END
        """
        ), nullable=True
    )
    box_area_m2 = Column(Numeric(8, 4), Computed(
        """
        CASE
            WHEN box_length_mm IS NOT NULL
                 AND box_width_mm IS NOT NULL
            THEN ROUND(
                (box_length_mm::numeric / 1000.0) *
                (box_width_mm::numeric / 1000.0), 4
            )
            ELSE NULL
        END
        """
        ), nullable=True
    )
    box_stacking = Column(
        SmallInteger,
        CheckConstraint('box_stacking >= 0'),
        nullable=True
    )
    # Relationships
    parts = relationship(
        'PartToBox',
        back_populates='box',
        lazy='select'
    )
    pallets = relationship(
        'BoxToPallet',
        back_populates='box',
        lazy='select'
    )


class PalletData(Base):
    '''
    Model defines a table for storing pallet's information.
    '''
    __tablename__ = 'pallet_data'
    __table_args__ = (
        Index('idx_pallet_number', 'pallet_number'),
        Index('idx_pallet_type', 'pallet_type'),
        Index('idx_pallet_dimensions',
              'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm'),
        UniqueConstraint('pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm',
                         name='unique_pallet_dimensions'),
        {
            'comment': """
            PURPOSE: Pallet specifications for logistics
            ---
            COLUMN DESCRIPTION:
            - pallet_id: Unique system identifier (PLT_ + 36-character UUID)
            - pallet_number: Auto-generated from type and dimensions (R/N L-W-H)
            - pallet_type: 'returnable' or 'non-returnable'
            - Dimensions: Length/Width/Height in mm
            - pallet_vol_m3/pallet_area_m2: Calculated from dimensions
            - pallet_stacking: Max safe pallet stacking height
            ---
            RELATIONSHIPS:
            - Many-to-Many with: BoxData (via box_to_pallet)
            ---
            BUSINESS RULES:
            - Standard sizes:
                - 1200x800mm - Euro
                - 1200x1000mm - Industrial
            - Dimensions in 10mm increments
            - Volume/area auto-calculated on insert/update
            - Pallet number auto-generated from type and dimensions
            """
        },
    )
    pallet_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'PLT_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    pallet_type = Column(
        PACKAGING_TYPE_ENUM,
        nullable=False
    )
    pallet_weight_kg = Column(
        Numeric(5, 2),
        CheckConstraint('pallet_weight_kg >= 0'),
        nullable=True
    )
    pallet_length_mm = Column(
        SmallInteger,
        CheckConstraint('pallet_length_mm > 0'),
        nullable=True
    )
    pallet_width_mm = Column(
        SmallInteger,
        CheckConstraint('pallet_width_mm > 0'),
        nullable=True
    )
    pallet_height_mm = Column(
        SmallInteger,
        CheckConstraint('pallet_height_mm > 0'),
        nullable=True
    )
    pallet_number = Column(String(50), Computed(
        """
        CASE
            WHEN pallet_type IS NOT NULL
                AND pallet_length_mm IS NOT NULL
                AND pallet_width_mm IS NOT NULL
                AND pallet_height_mm IS NOT NULL
            THEN 
                CASE 
                    WHEN pallet_type = 'returnable' THEN 'R ' || 
                        pallet_length_mm::text || '-' ||
                        pallet_width_mm::text || '-' ||
                        pallet_height_mm::text
                    WHEN pallet_type = 'non-returnable' THEN 'N ' ||
                        pallet_length_mm::text || '-' ||
                        pallet_width_mm::text || '-' ||
                        pallet_height_mm::text
                    WHEN pallet_type = 'no data' THEN 'no_d ' ||
                        pallet_length_mm::text || '-' ||
                        pallet_width_mm::text || '-' ||
                        pallet_height_mm::text
                    ELSE NULL
                END
            ELSE NULL
        END
        """
        ), nullable=True
    )
    pallet_vol_m3 = Column(Numeric(8, 4), Computed(
        """
        CASE
            WHEN pallet_length_mm IS NOT NULL
                 AND pallet_width_mm IS NOT NULL
                 AND pallet_height_mm IS NOT NULL
            THEN ROUND(
                (pallet_length_mm::numeric / 1000.0) *
                (pallet_width_mm::numeric / 1000.0) *
                (pallet_height_mm::numeric / 1000.0), 4
            )
            ELSE NULL
        END
        """
        ), nullable=True
    )
    pallet_area_m2 = Column(Numeric(8, 4), Computed(
        """
        CASE
            WHEN pallet_length_mm IS NOT NULL
                 AND pallet_width_mm IS NOT NULL
            THEN ROUND(
                (pallet_length_mm::numeric / 1000.0) *
                (pallet_width_mm::numeric / 1000.0), 4
            )
            ELSE NULL
        END
        """
        ), nullable=True
    )
    pallet_stacking = Column(
        SmallInteger,
        CheckConstraint('pallet_stacking >= 0'),
        nullable=True
    )
    # Relationships
    boxes = relationship(
        'BoxToPallet',
        back_populates='pallet',
        lazy='select'
    )


class ModelData(Base):
    '''
    Model defines a table for storing model's information.
    '''
    __tablename__ = 'model_data'
    __table_args__ = (
        Index('idx_model_code', 'model_code'),
        Index('idx_model_name', 'model_name'),
        {
            'comment': """
            PURPOSE: Vehicle model master data
            ---
            COLUMN DESCRIPTION:
            - model_id: Unique system identifier (MDL_ + 36-character UUID)
            - model_code: Platform codes (A01, B02, etc.)
            - model_name: Marketing names (Jolion, H3, etc.)
            ---
            RELATIONSHIPS:
            - Many-to-Many with: PartData (via part_to_model)
            ---
            BUSINESS RULES:
            - Model codes follow platform-generation-variant pattern
            - Used for BOM (Bill of Materials) configuration
            """
        },
    )
    model_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'MDL_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    model_code = Column(
        MODEL_CODES_ENUM,
        unique=True,
        nullable=False
    )
    model_name = Column(
        MODEL_NAMES_ENUM,
        unique=True,
        nullable=False
    )
    # Relationships
    parts = relationship(
        'PartToModel',
        back_populates='model',
        lazy='select'
    )
    breakpoint_changes = relationship(
        'PartToBreakpoint',
        foreign_keys='PartToBreakpoint.model_id',
        back_populates='model',
        lazy='select'
    )


class ConfigurationData(Base):
    '''
    Model defines a table for storing configuration types.
    '''
    __tablename__ = 'configuration_data'
    __table_args__ = (
        Index('idx_configuration', 'configuration'),
        {
            'comment': """
            PURPOSE: Configuration types master data
            ---
            COLUMN DESCRIPTION:
            - configuration_id: Unique system identifier (CFG_ + 36-character UUID)
            - configuration: Configuration name (comfort, elite, tech_plus, premium)
            - description: Optional description of the configuration
            ---
            BUSINESS RULES:
            - Configuration names are unique
            - Used across all vehicle models
            - Affects part consumption per vehicle
            """
        },
    )
    configuration_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'CFG_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    configuration = Column(
        CONFIGURATION_ENUM,
        unique=True,
        nullable=False
    )
    description = Column(
        String(100),
        nullable=True
    )
    # Relationships
    part_models = relationship(
        'PartToModel',
        back_populates='configuration'
    )


class WorkshopData(Base):
    '''
    Model defines a table for storing workshop's information.
    '''
    __tablename__ = 'workshop_data'
    __table_args__ = (
        Index('idx_workshop_code', 'workshop_code'),
        Index('idx_workshop_name', 'workshop_name'),
        {
            'comment': """
            PURPOSE: Production workshop organization
            ---
            COLUMN DESCRIPTION:
            - workshop_id: Unique system identifier (WSP_ + 36-character UUID)
            - workshop_code: Process codes (AS, COMP, etc.)
            - workshop_name: Full names (Assembly, Component, etc.)
            ---
            RELATIONSHIPS:
            - One workshop contains many lines (1:N with LineData)
            ---
            BUSINESS RULES:
            - Workshops represent manufacturing process steps
            - Codes follow production flow sequence
            """
        },
    )
    workshop_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'WSP_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    workshop_code = Column(
        WORKSHOP_CODES_ENUM,
        unique=True,
        nullable=False
    )
    workshop_name = Column(
        WORKSHOP_NAMES_ENUM,
        unique=True,
        nullable=False
    )
    # Relationships
    lines = relationship('LineData', back_populates='workshop', lazy='joined')


class LineData(Base):
    '''
    Model defines a table for storing line's information.
    '''
    __tablename__ = 'line_data'
    __table_args__ = (
        Index('idx_line_code', 'line_code'),
        Index('idx_line_name', 'line_name'),
        Index('idx_line_workshop_id', 'workshop_id'),
        {
            'comment': """
            PURPOSE: Production line specifications
            ---
            COLUMN DESCRIPTION:
            - line_id: Unique system identifier (LNE_ + 36-character UUID)
            - line_code: Line identifier within workshop
            - line_name: Descriptive name
            - workshop_id: References workshop_data
            ---
            RELATIONSHIPS:
            - Many-to-One with: WorkshopData
            - Many-to-Many with: PartData (via part_to_line)
            ---
            BUSINESS RULES:
            - Each line belongs to exactly one workshop
            - Parts are assigned to specific installation lines
            """
        },
    )
    line_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'LNE_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    line_code = Column(
        String(10),
        unique=True,
        nullable=False
    )
    line_name = Column(
        String(50),
        nullable=True
    )
    workshop_id = Column(
        String(40),
        ForeignKey('workshop_data.workshop_id', ondelete='RESTRICT'),
        nullable=False,
        comment="The production workshop cannot be deleted if there are lines!"
    )
    # Relationships
    workshop = relationship(
        'WorkshopData',
        back_populates='lines',
        lazy='joined'
    )
    parts = relationship(
        'PartToLine',
        back_populates='line',
        lazy='select'
    )
    breakpoint_changes = relationship(
        'PartToBreakpoint',
        foreign_keys='PartToBreakpoint.line_id',
        back_populates='line',
        lazy='select'
    )


class BreakpointData(Base):
    '''
    Model defines a table for storing technical change's information.
    '''
    __tablename__ = 'breakpoint_data'
    __table_args__ = (
        Index('idx_breakpoint_number', 'breakpoint_number'),
        Index('idx_breakpoint_date', 'breakpoint_date'),
        Index('idx_input_date', 'input_date'),
        Index('idx_breakpoint_batch', 'batch'),
        Index('idx_breakpoint_composite_date_number',
              'breakpoint_date', 'breakpoint_number'),
        {
            'comment': """
            PURPOSE: Technical change management (breakpoints)
            ---
            COLUMN DESCRIPTION:
            - breakpoint_id: Unique system identifier (BPT_ + 36-character UUID)
            - input_date: When record was created
            - breakpoint_number: Engineering change identifier
            - breakpoint_date: When change takes effect
            - batch: Number of batch the technical change occurred
            - description: Cause and solution of the technical change
            ---
            RELATIONSHIPS:
            - Many-to-Many with: PartData (via part_to_breakpoint)
            ---
            BUSINESS RULES:
            - Tracks part changes before/after engineering changes
            - Used for version control and traceability
            """
        },
    )
    breakpoint_id = Column(
        String(40),
        primary_key=True,
        server_default=text("'BPT_' || gen_random_uuid()::text"),
        unique=True,
        nullable=False
    )
    input_date = Column(
        DateTime(),
        server_default=func.now(),
        nullable=False
    )
    breakpoint_number = Column(
        String(10),
        unique=True,
        nullable=False
    )
    breakpoint_date = Column(
        DateTime(),
        nullable=False
    )
    batch = Column(
        String(10),
        nullable=True
    )
    description = Column(
        Text,
        nullable=True
    )
    # Relationships
    parts = relationship(
        'PartToBreakpoint',
        back_populates='breakpoint',
        lazy='select'
    )


# ========== JUNCTION TABLES ==========

class PartToBox(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and BoxData.
    '''
    __tablename__ = 'part_to_box'
    __table_args__ = (
        Index('idx_ptb_part_id', 'part_id'),
        Index('idx_ptb_box_id', 'box_id'),
        Index('idx_ptb_composite', 'part_id', 'box_id'),
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Parts ↔ Boxes
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data
            - box_id: References box_data
            - part_per_box: Quantity of parts per box
            ---
            BUSINESS RULES:
            - Defines packaging configuration for parts
            - Used for warehouse capacity planning
            """
        },
    )
    part_id = Column(
        String(40),
        ForeignKey('part_data.part_id', ondelete='CASCADE'),
        primary_key=True
    )
    box_id = Column(
        String(40),
        ForeignKey('box_data.box_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The box cannot be removed if it is used by parts!"
    )
    part_per_box = Column(
        Integer,
        CheckConstraint('part_per_box > 0'),
        nullable=True
    )
    # Relationships
    part = relationship(
        'PartData',
        back_populates='boxes'
    )
    box = relationship(
        'BoxData',
        back_populates='parts'
    )


class BoxToPallet(Base):
    '''
    Junction table used to organize many-to-many relationships
    between main entities: PartData, BoxData and PalletData.
    '''
    __tablename__ = 'box_to_pallet'
    __table_args__ = (
        Index('idx_btp_box_id', 'box_id'),
        Index('idx_btp_pallet_id', 'pallet_id'),
        Index('idx_btp_part_id', 'part_id'),
        Index('idx_btp_composite', 'box_id', 'pallet_id', 'part_id'),
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Boxes ↔ Pallets with Part association
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data (which part uses this box-pallet combination)
            - box_id: References box_data
            - pallet_id: References pallet_data
            - box_per_pallet: Quantity of boxes per pallet
            ---
            BUSINESS RULES:
            - Defines pallet loading configuration
            - Optimizes transportation and warehouse space utilization
            """
        },
    )
    part_id = Column(
        String(40),
        ForeignKey('part_data.part_id', ondelete='CASCADE'),
        primary_key=True
    )
    box_id = Column(
        String(40),
        ForeignKey('box_data.box_id', ondelete='CASCADE'),
        primary_key=True
    )
    pallet_id = Column(
        String(40),
        ForeignKey('pallet_data.pallet_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The pallet cannot be removed if it is used by boxes!"
    )
    box_per_pallet = Column(
        SmallInteger,
        CheckConstraint('box_per_pallet > 0'),
        nullable=True
    )
    # Relationships
    part = relationship(
        'PartData',
        back_populates='box_pallet_combinations'
    )
    box = relationship(
        'BoxData',
        back_populates='pallets'
    )
    pallet = relationship(
        'PalletData',
        back_populates='boxes'
    )


class PartToModel(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and ModelData.
    '''
    __tablename__ = 'part_to_model'
    __table_args__ = (
        Index('idx_ptm_part_id', 'part_id'),
        Index('idx_ptm_model_id', 'model_id'),
        Index('idx_ptm_configuration_id', 'configuration_id'),
        Index('idx_ptm_composite', 'part_id', 'model_id'),
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Parts ↔ Vehicle Models with configuration
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data
            - model_id: References model_data
            - configuration_id: References configuration_data
            - part_per_vehicle: Quantity used per vehicle for this configuration
            ---
            BUSINESS RULES:
            - Defines which parts go into which vehicle models with specific configuration
            - Different configurations may use different quantities of the same part
            - Used for BOM configuration and costing
            """
        },
    )
    part_id = Column(
        String(40),
        ForeignKey('part_data.part_id', ondelete='CASCADE'),
        primary_key=True
    )
    model_id = Column(
        String(40),
        ForeignKey('model_data.model_id', ondelete='RESTRICT'),
        primary_key=True
    )
    configuration_id = Column(
        String(40),
        ForeignKey('configuration_data.configuration_id', ondelete='RESTRICT'),
        primary_key=True
    )
    part_per_vehicle = Column(
        SmallInteger,
        CheckConstraint('part_per_vehicle > 0'),
        nullable=True
    )
    # Relationships
    part = relationship(
        'PartData',
        back_populates='models'
    )
    model = relationship(
        'ModelData',
        back_populates='parts'
    )
    configuration = relationship(
        'ConfigurationData',
        back_populates='part_models'
    )


class PartToLine(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and LineData.
    '''
    __tablename__ = 'part_to_line'
    __table_args__ = (
        Index('idx_ptl_part_id', 'part_id'),
        Index('idx_ptl_line_id', 'line_id'),
        Index('idx_ptl_composite', 'part_id', 'line_id'),
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Parts ↔ Production Lines
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data
            - line_id: References line_data
            ---
            BUSINESS RULES:
            - Defines which parts are installed on which lines
            - Used for production scheduling and line balancing
            """
        },
    )
    part_id = Column(
        String(40),
        ForeignKey('part_data.part_id', ondelete='CASCADE'),
        primary_key=True
    )
    line_id = Column(
        String(40),
        ForeignKey('line_data.line_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The line cannot be deleted if parts are installed on it!"
    )
    # Relationships
    part = relationship(
        'PartData',
        back_populates='lines'
    )
    line = relationship(
        'LineData',
        back_populates='parts'
    )


class PartToBreakpoint(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and BreakpointData.
    '''
    __tablename__ = 'part_to_breakpoint'
    __table_args__ = (
        Index('idx_ptbkp_part_id', 'part_id'),
        Index('idx_ptbkp_breakpoint_id', 'breakpoint_id'),
        Index('idx_ptbkp_supplier_id', 'supplier_id'),
        Index('idx_ptbkp_line_id', 'line_id'),
        Index('idx_ptbkp_model_id', 'model_id'),
        Index('idx_ptbkp_action', 'action'),
        Index('idx_ptbkp_composite', 'part_id', 'breakpoint_id'),
        {
            'comment': """
            PURPOSE: Part change history across breakpoints
            ---
            COLUMN DESCRIPTION:
                - part_id: References part_data (the part being changed)
                - breakpoint_id: References breakpoint_data (the change event)
                - model_id: References model_data (which model this change applies to)
                - action: Type of change (replace, delete, add, update)
                - supplier_id: References supplier_data (new/current supplier) - NULL for DELETE
                - line_id: References line_data (new/current line) - NULL for DELETE
                - *_before_change: Values before engineering change (snapshots)
            ---
            BUSINESS RULES:
                - Tracks part evolution over time per model
                - The same part may have different changes for different models
                - BEFORE values are snapshots from Excel at time of change
                - AFTER values are references to current master data
                - For DELETE action: supplier_id and line_id are NULL (no new supplier/line)
                - For DELETE action: before-change fields contain the old values
                - ACTION determines how to process the change:
                    * replace: Old part replaced by new part number
                    * delete: Part removed from production (soft delete)
                    * add: New part introduced
                    * update: Part attributes changed without part number change
            """
        },
    )
    part_id = Column(
        String(40),
        ForeignKey('part_data.part_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The part-number cannot be deleted as it is included in the revision history!"
    )
    breakpoint_id = Column(
        String(40),
        ForeignKey('breakpoint_data.breakpoint_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The breakpoint cannot be deleted as it is included in the revision history!"
    )
    model_id = Column(
        String(40),
        ForeignKey('model_data.model_id', ondelete='RESTRICT'),
        primary_key=True,
        comment="The model cannot be deleted as it is included in the revision history!"
    )
    supplier_id = Column(
        String(40),
        ForeignKey('supplier_data.supplier_id', ondelete='SET NULL'),
        nullable=True,
        comment="New/current supplier. NULL for DELETE actions (part removed from production)."
    )
    line_id = Column(
        String(40),
        ForeignKey('line_data.line_id', ondelete='SET NULL'),
        nullable=True,
        comment="New/current line. NULL for DELETE actions (part removed from production)."
    )
    action = Column(
        BREAKPOINT_ACTION_ENUM,
        nullable=False
    )
    # Before-change snapshots
    part_number_before_change = Column(
        String(50),
        nullable=True,
        comment="Part number before change (snapshot). Always populated for DELETE."
    )
    supplier_name_before_change = Column(
        String(200),
        nullable=True,
        comment="Supplier name before change (snapshot). Populated for DELETE/REPLACE/UPDATE."
    )
    localization_before_change = Column(
        LOCALIZATION_ENUM,
        nullable=True,
        comment="Localization status before change (snapshot). NULL for ADD/NO DATA actions."
    )
    line_name_before_change = Column(
        String(50),
        nullable=True,
        comment="Line name before change (snapshot). Populated for DELETE/REPLACE/UPDATE."
    )
    # Relationships
    part = relationship(
        'PartData',
        back_populates='breakpoints'
    )
    breakpoint = relationship(
        'BreakpointData',
        back_populates='parts'
    )
    model = relationship(
        'ModelData',
        foreign_keys=[model_id],
        back_populates='breakpoint_changes'
    )
    supplier = relationship(
        'SupplierData',
        foreign_keys=[supplier_id],
        back_populates='breakpoint_changes'
    )
    line = relationship(
        'LineData',
        foreign_keys=[line_id],
        back_populates='breakpoint_changes'
    )
