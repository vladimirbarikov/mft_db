"""
Database Models Module - SQLAlchemy ORM for Managing Manufacturing Data
of an Automotive Manufacturing Enterprise.

Compatibility: SQLAlchemy 1.4.54

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

- Automatic ID generation with prefixes (SUP_, PRT_, BOX_, etc.)
- Business rule validation through CheckConstraint
- Context-aware default values for packaging numbers
- Enum type support for categorized data
- Complete relationship mapping with back references (back_populates)

RELATIONSHIP STRUCTURE:
   Supplier (1) ↔ (N) Part (N) ↔ (N) Box (N) ↔ (N) Pallet
   Part (N) ↔ (N) Model
   Part (N) ↔ (N) Line (N) ↔ (1) Workshop
   Part (N) ↔ (N) Breakpoint (change history)

Version: 1.0.0
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""

import random
import string
from sqlalchemy import Column, ForeignKey, CheckConstraint, DateTime, func
from sqlalchemy.types import String, SmallInteger, Numeric, Integer, Enum as SqlEnum
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Base class
Base = declarative_base()

def generate_random_id(prefix):
    '''
    Func generates a random ID with an arbitrary set of letters and numbers.
    :param prefix: Prefix for identifying the record type ('SUP_', 'PRT_', 'MDL_', etc.)
    :return: Unique ID
    '''
    random_part = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return f"{prefix}{random_part}"

def generate_box_number(context):
    '''
    Func generates BOX_NUMBER based on packaging type and dimensions.
    Format: 'A 1340-560-440' or 'B 1340-560-440'
    '''
    params = context.get_current_parameters()
    box_type = params.get('BOX_TYPE')
    length = params.get('BOX_LENGTH_MM')
    width = params.get('BOX_WIDTH_MM')
    height = params.get('BOX_HEIGHT_MM')

    prefix = 'A' if box_type == 'non-returnable' else 'B'
    return f"{prefix} {length}-{width}-{height}"

def generate_pallet_number(context):
    '''
    Func generates PALLET_NUMBER based on packaging type and dimensions.
    Format: 'A 1340-1560-1440' or 'B 1340-1560-1440'
    '''
    params = context.get_current_parameters()
    pallet_type = params.get('PALLET_TYPE')
    length = params.get('PALLET_LENGTH_MM')
    width = params.get('PALLET_WIDTH_MM')
    height = params.get('PALLET_HEIGHT_MM')

    prefix = 'A' if pallet_type == 'non-returnable' else 'B'
    return f"{prefix} {length}-{width}-{height}"

# Enum types
localization_enum = SqlEnum('yes', 'no', name='localization')
packaging_type_enum = SqlEnum('returnable', 'non-returnable', name='packaging_type')
model_codes_enum = SqlEnum('A01', 'A08', 'B02', 'B04', 'B06', 'B16', name='model_codes')
model_names_enum = SqlEnum('Jolion', 'H3', 'F7', 'F7x', 'Dargo', 'H7', name='model_names')
workshop_codes_enum = SqlEnum('AS', 'COMP', 'PAINT', 'WELD', 'STAMP', 'EN', name='workshop_codes')
workshop_names_enum = SqlEnum('Assembly', 'Component', 'Painting', 'Welding', 'Stamping', 'Engine', name='workshop_names')

class SupplierData(Base):
    '''
    This is a model that defines a table for storing information
    about suppliers.
    '''
    __tablename__ = 'supplier_data'
    SUPPLIER_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("SUP_"))
    SUPPLIER_NAME = Column(String(200))
    LOCATION = Column(String(50))
    CITY = Column(String(50))
    STREET = Column(String(100))
    BUILDING = Column(String(10))
    LOCALIZATION = Column(localization_enum)
    parts = relationship("PartData", back_populates="supplier")

class PartData(Base):
    '''
    This is a model that defines a table for storing information
    about auto components.
    '''
    __tablename__ = 'part_data'
    PART_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("PRT_"))
    PART_NUMBER = Column(String(50))
    PART_NAME = Column(String(100))
    PART_WEIGHT_KG = Column(Numeric(5, 3), CheckConstraint('PART_WEIGHT_KG >= 0'))
    SUPPLIER_ID = Column(String(12), ForeignKey('supplier_data.SUPPLIER_ID'))
    supplier = relationship("SupplierData", back_populates="parts")
    boxes = relationship("PartToBox", back_populates="part")
    models = relationship("PartToModel", back_populates="part")
    lines = relationship("PartToLine", back_populates="part")
    breakpoints = relationship("PartToBreakpoint", back_populates="part")
    

class BoxData(Base):
    '''
    This is a model that defines a table for storing information
    about packaging boxes.
    '''
    __tablename__ = 'box_data'
    BOX_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("BOX_"))
    BOX_NUMBER = Column(String(50), default=generate_box_number)
    BOX_TYPE = Column(packaging_type_enum)
    BOX_WEIGHT_KG = Column(Numeric(5, 3), CheckConstraint('BOX_WEIGHT_KG >= 0'))
    BOX_LENGTH_MM = Column(SmallInteger)
    BOX_WIDTH_MM = Column(SmallInteger)
    BOX_HEIGHT_MM = Column(SmallInteger)
    BOX_VOL_M3 = Column(Numeric(5, 3), CheckConstraint('BOX_VOL_M3 >= 0'))
    BOX_AREA_M2 = Column(Numeric(5, 3), CheckConstraint('BOX_AREA_M2 >= 0'))
    BOX_STACKING = Column(SmallInteger)
    parts = relationship("PartToBox", back_populates="box")
    pallets = relationship("BoxToPallet", back_populates="box")

class PalletData(Base):
    '''
    This is a model that defines a table for storing information 
    about packaging pallets.
    '''
    __tablename__ = 'pallet_data'
    PALLET_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("PLT_"))
    PALLET_NUMBER = Column(String(50), default=generate_pallet_number)
    PALLET_TYPE = Column(packaging_type_enum)
    PALLET_WEIGHT_KG = Column(Numeric(5, 3), CheckConstraint('PALLET_WEIGHT_KG >= 0'))
    PALLET_LENGTH_MM = Column(SmallInteger)
    PALLET_WIDTH_MM = Column(SmallInteger)
    PALLET_HEIGHT_MM = Column(SmallInteger)
    PALLET_VOL_M3 = Column(Numeric(5, 3), CheckConstraint('PALLET_VOL_M3 >= 0'))
    PALLET_AREA_M2 = Column(Numeric(5, 3), CheckConstraint('PALLET_AREA_M2 >= 0'))
    PALLET_STACKING = Column(SmallInteger)
    boxes = relationship("BoxToPallet", back_populates="pallet")

class PartToBox(Base):
    '''
    This is a junction table used to organize many-to-many relationships
    between two main entities: PartData and BoxData.
    '''
    __tablename__ = 'part_to_box'
    PART_ID = Column(String(12), ForeignKey('part_data.PART_ID'), primary_key=True)
    BOX_ID = Column(String(12), ForeignKey('box_data.BOX_ID'), primary_key=True)
    PART_PER_BOX = Column(Integer)
    part = relationship("PartData", back_populates="boxes")
    box = relationship("BoxData", back_populates="parts")

class BoxToPallet(Base):
    '''
    This is a junction table used to organize many-to-many relationships
    between two main entities: BoxData and PalletData.
    '''
    __tablename__ = 'box_to_pallet'
    BOX_ID = Column(String(12), ForeignKey('box_data.BOX_ID'), primary_key=True)
    PALLET_ID = Column(String(12), ForeignKey('pallet_data.PALLET_ID'), primary_key=True)
    BOX_PER_PALLET = Column(SmallInteger)
    box = relationship("BoxData", back_populates="pallets")
    pallet = relationship("PalletData", back_populates="boxes")

class ModelData(Base):
    '''
    This is a model that defines a table for storing information 
    about car models.
    '''
    __tablename__ = 'model_data'
    MODEL_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("MDL_"))
    MODEL_CODE = Column(model_codes_enum)
    MODEL_NAME = Column(model_names_enum)
    parts = relationship("PartToModel", back_populates="model")

class PartToModel(Base):
    '''
    This is a junction table used to organize many-to-many relationships
    between two main entities: PartData and ModelData.
    '''
    __tablename__ = 'part_to_model'
    PART_ID = Column(String(12), ForeignKey('part_data.PART_ID'), primary_key=True)
    MODEL_ID = Column(String(12), ForeignKey('model_data.MODEL_ID'), primary_key=True)
    CONFIGURATION = Column(String(20))
    PART_PER_VEHICLE = Column(SmallInteger)
    part = relationship("PartData", back_populates="models")
    model = relationship("ModelData", back_populates="parts")

class WorkshopData(Base):
    '''
    This is a model that defines a table for storing information 
    about production workshops.
    '''
    __tablename__ = 'workshop_data'
    WORKSHOP_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("WSP_"))
    WORKSHOP_CODE = Column(workshop_codes_enum)
    WORKSHOP_NAME = Column(workshop_names_enum)
    lines = relationship("LineData", back_populates="workshop")

class LineData(Base):
    '''
    This is a model that defines a table for storing information 
    about production lines.
    '''
    __tablename__ = 'line_data'
    LINE_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("LNE_"))
    LINE_CODE = Column(String(10))
    LINE_NAME = Column(String(50))
    WORKSHOP_ID = Column(String(10), ForeignKey('workshop_data.WORKSHOP_ID'))
    workshop = relationship("WorkshopData", back_populates="lines")
    parts = relationship("PartToLine", back_populates="line")

class PartToLine(Base):
    '''
    This is a junction table used to organize many-to-many relationships
    between two main entities: PartData and LineData.
    '''
    __tablename__ = 'part_to_line'
    PART_ID = Column(String(12), ForeignKey('part_data.PART_ID'), primary_key=True)
    LINE_ID = Column(String(12), ForeignKey('line_data.LINE_ID'), primary_key=True)
    part = relationship("PartData", back_populates="lines")
    line = relationship("LineData", back_populates="parts")

class BreakpointData(Base):
    '''
    This is a model that defines a table for storing information 
    about technical changes of auto components: breakpoints.
    '''
    __tablename__ = 'breakpoint_data'
    BREAKPOINT_ID = Column(String(12), primary_key=True, default=lambda: generate_random_id("BPT_"))
    INPUT_DATE = Column(DateTime(), server_default=func.now())
    BREAKPOINT_NUMBER = Column(String(10), nullable=False)
    BREAKPOINT_DATE = Column(DateTime())
    parts = relationship("PartToBreakpoint", back_populates="breakpoint")

class PartToBreakpoint(Base):
    '''
    This is a junction table used to organize many-to-many relationships
    between two main entities: PartData and BreakpointData.
    '''
    __tablename__ = 'part_to_breakpoint'
    PART_ID = Column(String(12), ForeignKey('part_data.PART_ID'), primary_key=True)
    BREAKPOINT_ID = Column(String(12), ForeignKey('breakpoint_data.BREAKPOINT_ID'), primary_key=True)
    PART_NUMBER_BEFORE_CHANGE = Column(String(50))
    SUPPLIER_NAME_BEFORE_CHANGE = Column(String(200))
    LOCALIZATION_BEFORE_CHANGE = Column(localization_enum)
    LINE_NAME_BEFORE_CHANGE = Column(String(50))
    part = relationship("PartData", back_populates="breakpoints")
    breakpoint = relationship("BreakpointData", back_populates="parts")
