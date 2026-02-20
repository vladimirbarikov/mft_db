"""
Database Models Module for Material Flow Table Database with SQLAlchemy ORM.

DATABASE MODELS AND TABLES:

1. CORE ENTITY TABLES:
   - supplier_data      - Information about component suppliers
   - part_data          - Data about automotive components (parts)
   - box_data           - Packaging box specifications
   - pallet_data        - Pallet (platform) specifications
   - model_data         - Vehicle models
   - workshop_data      - Production workshops
   - line_data          - Production lines
   - breakpoint_data    - Technical changes (breakpoints)

2. JUNCTION TABLES (MANY-TO-MANY RELATIONSHIPS):
   - part_to_box        - Relationship between parts and packaging boxes
   - box_to_pallet      - Relationship between boxes and pallets
   - part_to_model      - Relationship between parts and vehicle models
   - part_to_line       - Relationship between parts and production lines
   - part_to_breakpoint - Part change history (before/after breakpoint)

STORED INFORMATION:

1. SUPPLIERS (supplier_data):
   - Basic data: identifier, name, location
   - Address: city, street, building
   - Localization: yes/no (local/foreign supplier)

2. PARTS (part_data):
   - Identifiers: PART_ID, PART_NUMBER
   - Description: name, weight (kg)
   - Supplier relationship (ForeignKey)

3. PACKAGING (box_data, pallet_data):
   - Packaging type: returnable/non-returnable
   - Dimensions: length, width, height (mm)
   - Calculated parameters: volume (m³), area (m²)
   - Weight and maximum stacking capability
   - Automatic packaging number generation
   - Real-time calculation via database triggers

4. PRODUCTION (workshop_data, line_data):
   - Workshops: code (AS, COMP, PAINT, etc.) and name
   - Lines: code, name, workshop affiliation

5. VEHICLE MODELS (model_data):
   - Model codes: A01, A08, B02, etc.
   - Model names: Jolion, H3, F7, etc.

6. TECHNICAL CHANGES (breakpoint_data):
   - Breakpoint number and date
   - Entry date into the system
   - Part change history

IMPLEMENTATION FEATURES:
- UUID format: 32 hexadecimal characters + 4 hyphens = 36 characters total
- Automatic ID generation with prefixes (SUP_, PRT_, BOX_, etc.)
- All ID fields use format: 
    PREFIX_ + UUID = 40 characters total (e.g., SUP_f47ac10b-58cc-4372-a567-0e02b2c3d479)
- Business rule validation through CheckConstraint
- Context-aware default values for packaging numbers
- Enum type support for categorized data
- Complete relationship mapping with back references (back_populates)
- Real-time calculation of packaging volume/area via SQLAlchemy event handlers
- Automatic calculation and number generation via SQLAlchemy event handlers

RELATIONSHIP STRUCTURE:
   Supplier (1) ↔ (N) Part (N) ↔ (N) Box (N) ↔ (N) Pallet
   Part (N) ↔ (N) Model
   Part (N) ↔ (N) Line (N) ↔ (1) Workshop
   Part (N) ↔ (N) Breakpoint (change history)

Version: 1.1.0
Compatibility: Python 3.12.3, SQLAlchemy 1.4.54, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-01-16
Last Modified: 2026-02-11
License: MIT
Status: Production
"""
import uuid
from sqlalchemy import (
    CheckConstraint, Column, Computed, DateTime,
    Enum as SqlEnum, ForeignKey, func, Index
)
from sqlalchemy.types import (
    Integer, Numeric, String, SmallInteger
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Base class
Base = declarative_base()

# ========== ENUM TYPES ==========
LOCALIZATION_ENUM = SqlEnum(
    'yes', 'no', name='localization'
)

PACKAGING_TYPE_ENUM = SqlEnum(
    'returnable', 'non-returnable', name='packaging_type'
)

MODEL_CODES_ENUM = SqlEnum(
    'A01', 'A08', 'B02', 'B04', 'B06', 'B16', name='model_codes'
)

MODEL_NAMES_ENUM = SqlEnum(
    'Jolion', 'H3', 'F7', 'F7x', 'Dargo', 'H7', name='model_names'
)

WORKSHOP_CODES_ENUM = SqlEnum(
    'AS', 'COMP', 'PAINT', 'WELD', 'STAMP', 'EN', name='workshop_codes'
)

WORKSHOP_NAMES_ENUM = SqlEnum(
    'Assembly', 'Component', 'Painting', 'Welding', 'Stamping', 'Engine', name='workshop_names'
)


# ========== CORE ENTITY TABLES ==========
class SupplierData(Base):
    '''
    Model defines a table for storing supplier's information.
    '''
    __tablename__ = 'supplier_data'
    __table_args__ = (
        Index('idx_supplier_name', 'supplier_name'),            # Search by supplier name
        Index('idx_supplier_city', 'city'),                     # Filter by city
        Index('idx_supplier_localization', 'localization'),     # Local/foreign filter
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
        default=lambda: f"SUP_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    supplier_name = Column(String(200), unique=True, nullable=False)
    location = Column(String(50))
    city = Column(String(50))
    street = Column(String(100))
    building = Column(String(10))
    localization = Column(LOCALIZATION_ENUM)
    parts = relationship('PartData', back_populates='supplier', lazy='selectin')


class PartData(Base):
    '''
    Model defines a table for storing component's information.
    '''
    __tablename__ = 'part_data'
    __table_args__ = (
        Index('idx_part_number', 'part_number'),            # Search by part number
        Index('idx_part_name', 'part_name'),                # Search by part name
        Index('idx_part_weight', 'part_weight_kg'),         # Weight range
        Index('idx_part_supplier_id', 'supplier_id'),       # Foreign Key
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
            ---
            RELATIONSHIPS:
            - Many-to-Many with: ModelData, LineData, BoxData, BreakpointData
            - Many-to-One with: SupplierData
            ---
            BUSINESS RULES:
            - Part number follows corporate standard
            - Weight critical for logistics costing
            - Each part has exactly one supplier
            """
        },
    )
    part_id = Column(
        String(40),
        primary_key=True,
        default=lambda: f"PRT_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    part_number = Column(String(50), unique=True, nullable=False)
    part_name = Column(String(100))
    part_weight_kg = Column(
        Numeric(5, 2),
        CheckConstraint('part_weight_kg >= 0'))
    supplier_id = Column(
        String(40),
        ForeignKey('supplier_data.supplier_id', ondelete='RESTRICT'),
        nullable=False,
        comment="The supplier cannot be deleted if there are part-numbers!"
    )
    supplier = relationship('SupplierData', back_populates='parts', lazy='joined')
    boxes = relationship('PartToBox', back_populates='part', lazy='selectin')
    models = relationship('PartToModel', back_populates='part', lazy='selectin')
    lines = relationship('PartToLine', back_populates='part', lazy='select')
    breakpoints = relationship('PartToBreakpoint', back_populates='part', lazy='select')


class BoxData(Base):
    '''
    Model defines a table for storing box's information.
    '''
    __tablename__ = 'box_data'
    __table_args__ = (
        Index('idx_box_number', 'box_number'),          # Search by box number
        Index('idx_box_type', 'box_type'),              # Returnable/non-returnable filter
        Index('idx_box_dimensions',                     # Composite index for searching by box dimensions
              'box_length_mm', 'box_width_mm', 'box_height_mm'),                  
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
        default=lambda: f"BOX_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    box_type = Column(PACKAGING_TYPE_ENUM)
    box_weight_kg = Column(Numeric(5, 2), CheckConstraint('box_weight_kg >= 0'))
    box_length_mm = Column(SmallInteger)
    box_width_mm = Column(SmallInteger)
    box_height_mm = Column(SmallInteger)
    box_number = Column(String(50), Computed(
        """
        CASE
            WHEN box_type IS NOT NULL
                 AND box_length_mm IS NOT NULL
                 AND box_width_mm IS NOT NULL
                 AND box_height_mm IS NOT NULL
            THEN (CASE WHEN box_type = 'returnable' THEN 'R' ELSE 'N' END) || ' ' ||
                 box_length_mm::text || '-' ||
                 box_width_mm::text || '-' ||
                 box_height_mm::text
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
    box_stacking = Column(SmallInteger)
    parts = relationship('PartToBox', back_populates='box', lazy='select')
    pallets = relationship('BoxToPallet', back_populates='box', lazy='select')


class PalletData(Base):
    '''
    Model defines a table for storing pallet's information.
    '''
    __tablename__ = 'pallet_data'
    __table_args__ = (
        Index('idx_pallet_number', 'pallet_number'),            # Search by pallet number
        Index('idx_pallet_type', 'pallet_type'),                # Returnable/non-returnable filter
        Index('idx_pallet_dimensions',                          # Composite index for searching by pallet dimensions
              'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm'),
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
        default=lambda: f"PLT_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    pallet_type = Column(PACKAGING_TYPE_ENUM)
    pallet_weight_kg = Column(Numeric(5, 2), CheckConstraint('pallet_weight_kg >= 0'))
    pallet_length_mm = Column(SmallInteger)
    pallet_width_mm = Column(SmallInteger)
    pallet_height_mm = Column(SmallInteger)
    pallet_number = Column(String(50), Computed(
        """
        CASE
            WHEN pallet_type IS NOT NULL
                 AND pallet_length_mm IS NOT NULL
                 AND pallet_width_mm IS NOT NULL
                 AND pallet_height_mm IS NOT NULL
            THEN (CASE WHEN pallet_type = 'returnable' THEN 'R' ELSE 'N' END) || ' ' ||
                 pallet_length_mm::text || '-' ||
                 pallet_width_mm::text || '-' ||
                 pallet_height_mm::text
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
    pallet_stacking = Column(SmallInteger)
    boxes = relationship('BoxToPallet', back_populates='pallet', lazy='select')


class ModelData(Base):
    '''
    Model defines a table for storing model's information.
    '''
    __tablename__ = 'model_data'
    __table_args__ = (
        Index('idx_model_code', 'model_code'),          # Search by model code
        Index('idx_model_name', 'model_name'),          # Search by model name
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
        default=lambda: f"MDL_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    model_code = Column(MODEL_CODES_ENUM, unique=True, nullable=False)
    model_name = Column(MODEL_NAMES_ENUM)
    parts = relationship('PartToModel', back_populates='model', lazy='select')


class WorkshopData(Base):
    '''
    Model defines a table for storing workshop's information.
    '''
    __tablename__ = 'workshop_data'
    __table_args__ = (
        Index('idx_workshop_code', 'workshop_code'),            # Search by workshop code
        Index('idx_workshop_name', 'workshop_name'),            # Search by workshop name
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
        default=lambda: f"WSP_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    workshop_code = Column(WORKSHOP_CODES_ENUM, unique=True, nullable=False)
    workshop_name = Column(WORKSHOP_NAMES_ENUM)
    lines = relationship('LineData', back_populates='workshop', lazy='joined')


class LineData(Base):
    '''
    Model defines a table for storing line's information.
    '''
    __tablename__ = 'line_data'
    __table_args__ = (
        Index('idx_line_code', 'line_code'),            # Search by line code
        Index('idx_line_name', 'line_name'),            # Search by line name
        Index('idx_line_workshop_id', 'workshop_id'),   # Foreign Key
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
        default=lambda: f"LNE_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    line_code = Column(String(10), unique=True, nullable=False)
    line_name = Column(String(50))
    workshop_id = Column(
        String(40),
        ForeignKey('workshop_data.workshop_id', ondelete='RESTRICT'),
        nullable=False,
        comment="The production workshop cannot be deleted if there are lines!"
        )
    workshop = relationship('WorkshopData', back_populates='lines', lazy='joined')
    parts = relationship('PartToLine', back_populates='line', lazy='select')


class BreakpointData(Base):
    '''
    Model defines a table for storing technical change's information.
    '''
    __tablename__ = 'breakpoint_data'
    __table_args__ = (
        Index('idx_breakpoint_number', 'breakpoint_number'),    # Search by breakpoint number
        Index('idx_breakpoint_date', 'breakpoint_date'),        # Date range
        Index('idx_input_date', 'input_date'),                  # Sorting by input date
        {
            'comment': """
            PURPOSE: Technical change management (breakpoints)
            ---
            COLUMN DESCRIPTION:
            - breakpoint_id: Unique system identifier (BPT_ + 36-character UUID)
            - input_date: When record was created
            - breakpoint_number: Engineering change identifier
            - breakpoint_date: When change takes effect
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
        default=lambda: f"BPT_{uuid.uuid4()}",
        unique=True,
        nullable=False
    )
    input_date = Column(DateTime(), server_default=func.now())
    breakpoint_number = Column(String(10), unique=True, nullable=False)
    breakpoint_date = Column(DateTime())
    parts = relationship('PartToBreakpoint', back_populates='breakpoint', lazy='select')


# ========== JUNCTION TABLES ==========

class PartToBox(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and BoxData.
    '''
    __tablename__ = 'part_to_box'
    __table_args__ = (
        Index('idx_ptb_part_id', 'part_id'),                # Foreign Key
        Index('idx_ptb_box_id', 'box_id'),                  # Foreign Key
        Index('idx_ptb_composite', 'part_id', 'box_id'),    # Composite for JOIN
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
    part_per_box = Column(Integer)
    part = relationship('PartData', back_populates='boxes')
    box = relationship('BoxData', back_populates='parts')


class BoxToPallet(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: BoxData and PalletData.
    '''
    __tablename__ = 'box_to_pallet'
    __table_args__ = (
        Index('idx_btp_box_id', 'box_id'),                  # Foreign Key
        Index('idx_btp_pallet_id', 'pallet_id'),            # Foreign Key
        Index('idx_btp_composite', 'box_id', 'pallet_id'),  # Composite for JOIN
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Boxes ↔ Pallets
            ---
            COLUMN DESCRIPTION:
            - box_id: References box_data
            - pallet_id: References pallet_data
            - box_per_pallet: Quantity of boxes per pallet
            ---
            BUSINESS RULES:
            - Defines pallet loading configuration
            - Optimizes transportation space utilization
            """
        },
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
    box_per_pallet = Column(SmallInteger)
    box = relationship('BoxData', back_populates='pallets')
    pallet = relationship('PalletData', back_populates='boxes')


class PartToModel(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and ModelData.
    '''
    __tablename__ = 'part_to_model'
    __table_args__ = (
        Index('idx_ptm_part_id', 'part_id'),                # Foreign Key
        Index('idx_ptm_model_id', 'model_id'),              # Foreign Key
        Index('idx_ptm_composite', 'part_id', 'model_id'),  # Composite for JOIN
        {
            'comment': """
            PURPOSE: Many-to-many relationship: Parts ↔ Vehicle Models
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data
            - model_id: References model_data
            - configuration: Variant code (Comfort, Elite, Premium, Tech Plus)
            - part_per_vehicle: Quantity used per vehicle
            ---
            BUSINESS RULES:
            - Defines which parts go into which vehicle models
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
        primary_key=True,
        comment="The model cannot be deleted if it uses parts!"
    )
    configuration = Column(String(20))
    part_per_vehicle = Column(SmallInteger)
    part = relationship('PartData', back_populates='models')
    model = relationship('ModelData', back_populates='parts')


class PartToLine(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and LineData.
    '''
    __tablename__ = 'part_to_line'
    __table_args__ = (
        Index('idx_ptl_part_id', 'part_id'),                # Foreign Key
        Index('idx_ptl_line_id', 'line_id'),                # Foreign Key
        Index('idx_ptl_composite', 'part_id', 'line_id'),   # Composite for JOIN
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
    part = relationship('PartData', back_populates='lines')
    line = relationship('LineData', back_populates='parts')


class PartToBreakpoint(Base):
    '''
    Junction table used to organize many-to-many relationships
    between two main entities: PartData and BreakpointData.
    '''
    __tablename__ = 'part_to_breakpoint'
    __table_args__ = (
        Index('idx_ptbkp_part_id', 'part_id'),                      # Foreign Key
        Index('idx_ptbkp_breakpoint_id', 'breakpoint_id'),          # Foreign Key
        Index('idx_ptbkp_composite', 'part_id', 'breakpoint_id'),   # Composite for JOIN
        {
            'comment': """
            PURPOSE: Part change history across breakpoints
            ---
            COLUMN DESCRIPTION:
            - part_id: References part_data
            - breakpoint_id: References breakpoint_data
            - *_before_change: Values before engineering change
            ---
            BUSINESS RULES:
            - Tracks part evolution over time
            - Enables traceability and version control
            - Critical for quality and recall management
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
    part_number_before_change = Column(String(50))
    supplier_name_before_change = Column(String(200))
    localization_before_change = Column(LOCALIZATION_ENUM)
    line_name_before_change = Column(String(50))
    part = relationship('PartData', back_populates='breakpoints')
    breakpoint = relationship('BreakpointData', back_populates='parts')
