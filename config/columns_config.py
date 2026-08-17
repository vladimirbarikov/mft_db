# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Column configuration for Material Flow Table Database.

This module defines all required column names for extracting and transforming
manufacturing data from Excel files. These constants are used throughout the
ETL pipeline to ensure consistent column naming across tasks.

CORE ENTITY TABLES COLUMNS (UPPERCASE for Excel extraction):

    SUPPLIER_COLS: Columns for supplier entity table
        - SUPPLIER_NAME, LOCATION, CITY, STREET, BUILDING, LOCALIZATION

    PART_COLS: Columns for part entity table
        - PART_NUMBER, PART_NAME, PART_WEIGHT_KG, SUPPLIER_NAME

    BOX_COLS: Columns for box entity table (with dimensions)
        - BOX_TYPE, BOX_WEIGHT_KG, BOX_LENGTH_MM, BOX_WIDTH_MM, 
          BOX_HEIGHT_MM, BOX_STACKING

    PALLET_COLS: Columns for pallet entity table (with dimensions)
        - PALLET_TYPE, PALLET_WEIGHT_KG, PALLET_LENGTH_MM, PALLET_WIDTH_MM,
          PALLET_HEIGHT_MM, PALLET_STACKING

    MODEL_COLS: Columns for model entity table
        - MODEL_CODE, MODEL_NAME

    CONFIGURATION_COLS: Columns for configuration entity table
        - CONFIGURATION, TRANSMISSION

    WORKSHOP_COLS: Columns for workshop entity table
        - WORKSHOP_CODE, WORKSHOP_NAME

    LINE_COLS: Columns for production line entity table
        - LINE_CODE, LINE_NAME, WORKSHOP_CODE

    BREAKPOINT_COLS: Columns for breakpoint entity table (technical changes)
        - BP_NO, STATUS, BATCH_PLAN, BATCH_FACT, CHANGE_DATE, BOM_PRODUCT,
          DESCRIPTION, SOLUTION

    PART_BEFORE_COLS (old parts):
        - PART_NO_BEFORE, PART_NAME_BEFORE, CONFIGURATION, WORKSHOP_BEFORE,
          WORKCENTER_NO_BEFORE, WORKCENTER_NAME_BEFORE, QUANTITY_PER_VEHICLE_BEFORE,
          QUANTITY_PER_BOX_BEFORE, BOX_BEFORE, PALLET_BEFORE, DISPOSAL,
          SUPPLIER_NAME_BEFORE, LOCALIZATION_BEFORE

    PART_AFTER_COLS (new parts):
        - PART_NO_AFTER, PART_NAME_AFTER, CONFIGURATION, WORKSHOP_AFTER,
          WORKCENTER_NO_AFTER, WORKCENTER_NAME_AFTER, QUANTITY_PER_VEHICLE_AFTER,
          QUANTITY_PER_BOX_AFTER, BOX_AFTER, PALLET_AFTER, INTERCHANGEABLE,
          SUPPLIER_NAME_AFTER, LOCALIZATION_AFTER

JUNCTION TABLES COLUMNS (UPPERCASE for Excel extraction):

    PART_TO_BOX_COMPOSITE_COLS: Part-to-box junction (composite key)
        - PART_NUMBER, BOX_TYPE, BOX_LENGTH_MM, BOX_WIDTH_MM, 
          BOX_HEIGHT_MM, PART_PER_BOX

    BOX_TO_PALLET_COMPOSITE_COLS: Box-to-pallet junction (composite key)
        - PART_NUMBER, BOX_TYPE, BOX_LENGTH_MM, BOX_WIDTH_MM, BOX_HEIGHT_MM,
          PALLET_TYPE, PALLET_LENGTH_MM, PALLET_WIDTH_MM, PALLET_HEIGHT_MM,
          BOX_PER_PALLET

    PART_TO_MODEL_COLS: Part-to-model junction
        - PART_NUMBER, MODEL_CODE, CONFIGURATION, PART_PER_VEHICLE

    PART_TO_LINE_COLS: Part-to-line junction
        - PART_NUMBER, LINE_CODE

    PART_TO_BREAKPOINT_COLS: Part-to-breakpoint junction (change history)
        - PART_NO_BEFORE, PART_NO_AFTER, BP_NO, BOM_PRODUCT, SUPPLIER_NAME_BEFORE,
          SUPPLIER_NAME_AFTER, WORKCENTER_NO_BEFORE, WORKCENTER_NO_AFTER, 
          WORKCENTER_NAME_BEFORE, WORKCENTER_NAME_AFTER, WORKSHOP_BEFORE, WORKSHOP_AFTER,
          LOCALIZATION_BEFORE, LOCALIZATION_AFTER

MFT PIPELINE CONFIGURATION (for mft_dag.py, mft_mapper.py, mft_loader.py)

    MFT_COMPOSITE_COLUMNS: Composite key types requiring special handling
        - 'box_composite', 'pallet_composite'

    MFT_JUNCTION_REQUIRED: Required columns for MFT pipeline junction tables
        - part_to_box_composite: part_number, box_type, box_length_mm, 
          box_width_mm, box_height_mm
        - box_to_pallet_composite: part_number, box_type, box_length_mm,
          box_width_mm, box_height_mm, pallet_type, pallet_length_mm,
          pallet_width_mm, pallet_height_mm
        - part_to_model: part_number, model_code, configuration
        - part_to_line: part_number, line_code

    MFT_JUNCTION_OPTIONAL: Optional columns for MFT pipeline junction tables
        - part_to_box_composite: part_per_box
        - box_to_pallet_composite: box_per_pallet
        - part_to_model: part_per_vehicle
        - part_to_line: (none)

    MFT_TABLE_REQUIREMENTS: Required columns for MFT pipeline core tables
        - supplier_data, part_data, box_data, pallet_data, model_data,
          workshop_data, line_data, configuration_data

BP PIPELINE CONFIGURATION (for bp_dag.py, bp_mapper.py, bp_loader.py)

    BP_JUNCTION_REQUIRED: Required columns for breakpoint junction table
        - part_to_breakpoint: part_no_before, part_no_after, bp_no, bom_product

    BP_JUNCTION_OPTIONAL: Optional columns for breakpoint junction table
        - part_to_breakpoint: supplier_name_before, supplier_name_after,
          workcenter_no_before, workcenter_no_after, workcenter_name_before,
          workcenter_name_after, workshop_before, workshop_after,
          localization_before, localization_after, box_before, box_after,
          pallet_before, pallet_after

    BP_TABLE_REQUIREMENTS: Required columns for BP pipeline core tables
        - breakpoint_data, part_data_before, part_data_after

    BP_ACTION_TYPES: Action types for breakpoint changes
        - ADD: new_part_id NOT NULL, old_part_id NULL
        - DELETE: new_part_id NULL, old_part_id NOT NULL
        - UPDATE: new_part_id = old_part_id (same part)
        - REPLACE: Two records (DELETE old + ADD new) in same breakpoint

    BP_REQUIRED_FIELDS_BY_ACTION: Required fields for each action type
        - ADD: part_no_after, part_name_after, configuration, workshop_after,
          workcenter_no_after, workcenter_name_after, supplier_name_after,
          localization_after, box_after, pallet_after
        - DELETE: part_no_before, part_name_before, configuration, workshop_before,
          workcenter_no_before, workcenter_name_before, supplier_name_before,
          localization_before, box_before, pallet_before
        - UPDATE: part_no_before, part_no_after, part_name_before, part_name_after,
          configuration, workshop_before, workshop_after, workcenter_no_before,
          workcenter_no_after, workcenter_name_before, workcenter_name_after,
          supplier_name_before, supplier_name_after, localization_before,
          localization_after, box_before, box_after, pallet_before, pallet_after
        - REPLACE: part_no_before, part_no_after, part_name_before, part_name_after,
          configuration, workshop_before, workshop_after, workcenter_no_before,
          workcenter_no_after, workcenter_name_before, workcenter_name_after,
          supplier_name_before, supplier_name_after, localization_before,
          localization_after, box_before, box_after, pallet_before, pallet_after

DATABASE MODEL MAPPING (MFT PIPELINE):

    Core Entity Tables:
        supplier_data        ←→ SUPPLIER_COLS
        part_data            ←→ PART_COLS
        box_data             ←→ BOX_COLS
        pallet_data          ←→ PALLET_COLS
        model_data           ←→ MODEL_COLS
        configuration_data   ←→ CONFIGURATION_COLS
        workshop_data        ←→ WORKSHOP_COLS
        line_data            ←→ LINE_COLS
    
    Junction Tables:
        part_to_box          ←→ Used only in MFT pipeline
        box_to_pallet        ←→ Used only in MFT pipeline
        part_to_model        ←→ Used only in MFT pipeline
        part_to_line         ←→ Used only in MFT pipeline


DATABASE MODEL MAPPING (BREAKPOINT PIPELINE):

    Core Entity Tables:
        breakpoint_data      ←→ BREAKPOINT_COLS
        parts_before_data    ←→ PART_BEFORE_COLS
        parts_after_data     ←→ PART_AFTER_COLS

    Junction Tables:
        part_to_breakpoint   ←→ Used only in BP pipeline

ARCHITECTURE NOTES:

    MFT Pipeline (ETL Pattern):
        - Static data loading (suppliers, parts, boxes, pallets)
        - Core tables are pre-loaded, junction tables only link existing records
        - No creation of new records in core tables
        - Mapper performs only ID lookups, no business logic
        - Loader handles INSERT with ON CONFLICT DO NOTHING

    BP Pipeline (ETL Pattern):
        - Dynamic data loading (breakpoints, parts before/after, junctions)
        - Core tables may be created or updated during pipeline execution
        - Supports four action types: ADD, DELETE, UPDATE, REPLACE
        - Mapper determines action type based on presence of before/after data
        - Mapper performs ID lookups and creates new records when needed
        - Loader handles INSERT, UPDATE, and soft DELETE operations
        - Versioning: Each part change creates new version in part_data
        - Soft deactivation: Parts are deactivated via is_active=False in PartToModel
        - Junction table part_to_breakpoint links old and new part versions
        - Box and pallet records are looked up or created as needed
        - All ID lookups use business keys (part_number, supplier_name, etc.)
        - ENUM validation handled by enum_validator.py module
        - Cascade: Breakpoint deactivation triggers part deactivation via FK relationships

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-03-21
License: MIT
Status: Production
"""
# ========== MFT PIPELINE CONFIGURATION ==========
# For use in mft_dag.py, mft_mapper.py, and mft_loader.py

# ========== CORE ENTITY TABLES COLUMNS (UPPERCASE for Excel extraction) ==========
# Supplier entity columns
SUPPLIER_COLS = [
    'SUPPLIER_NAME',
    'LOCATION',
    'CITY',
    'STREET',
    'BUILDING',
    'LOCALIZATION',
]

# Part entity columns
PART_COLS = [
    'PART_NUMBER',
    'PART_NAME',
    'PART_WEIGHT_KG',
    'SUPPLIER_NAME'
]

# Box entity columns (with dimensional composite key)
BOX_COLS = [
    'BOX_TYPE',
    'BOX_WEIGHT_KG',
    'BOX_LENGTH_MM',
    'BOX_WIDTH_MM',
    'BOX_HEIGHT_MM',
    'BOX_STACKING',
]

# Pallet entity columns (with dimensional composite key)
PALLET_COLS = [
    'PALLET_TYPE',
    'PALLET_WEIGHT_KG',
    'PALLET_LENGTH_MM',
    'PALLET_WIDTH_MM',
    'PALLET_HEIGHT_MM',
    'PALLET_STACKING',
]

# Model entity columns
MODEL_COLS = [
    'MODEL_CODE',
    'MODEL_NAME',
]

# Configuration entity columns
CONFIGURATION_COLS = [
    'CONFIGURATION',
    'TRANSMISSION'
]

# Workshop entity columns
WORKSHOP_COLS = [
    'WORKSHOP_CODE',
    'WORKSHOP_NAME',
]

# Production line entity columns
LINE_COLS = [
    'LINE_CODE',
    'LINE_NAME',
    'WORKSHOP_CODE',
]

# ========== JUNCTION TABLES COLUMNS (UPPERCASE for Excel extraction) ==========
# Part-to-Box junction columns (composite key includes box dimensions)
PART_TO_BOX_COMPOSITE_COLS = [
    'PART_NUMBER',
    'BOX_TYPE',
    'BOX_LENGTH_MM',
    'BOX_WIDTH_MM',
    'BOX_HEIGHT_MM',
    'PART_PER_BOX',
]

# Box-to-Pallet junction columns (composite key includes both box and pallet dimensions)
BOX_TO_PALLET_COMPOSITE_COLS = [
    'PART_NUMBER',
    'BOX_TYPE',
    'BOX_LENGTH_MM',
    'BOX_WIDTH_MM',
    'BOX_HEIGHT_MM',
    'PALLET_TYPE',
    'PALLET_LENGTH_MM',
    'PALLET_WIDTH_MM',
    'PALLET_HEIGHT_MM',
    'BOX_PER_PALLET',
]

# Part-to-Model junction columns
PART_TO_MODEL_COLS = [
    'PART_NUMBER',
    'MODEL_CODE',
    'CONFIGURATION',
    'PART_PER_VEHICLE',
]

# Part-to-Line junction columns
PART_TO_LINE_COLS = [
    'PART_NUMBER',
    'LINE_CODE',
]

# Special composite key columns that need custom handling
MFT_COMPOSITE_COLUMNS = {'box_composite', 'pallet_composite'}

# Required columns for each junction table type (in lowercase for mapper)
MFT_JUNCTION_REQUIRED = {
    'part_to_box_composite': [
        'part_number',
        'box_type',
        'box_length_mm',
        'box_width_mm',
        'box_height_mm'
    ],
    'box_to_pallet_composite': [
        'part_number',
        'box_type',
        'box_length_mm',
        'box_width_mm',
        'box_height_mm',
        'pallet_type',
        'pallet_length_mm',
        'pallet_width_mm',
        'pallet_height_mm'
    ],
    'part_to_model': [
        'part_number',
        'model_code',
        'configuration'
    ],
    'part_to_line': [
        'part_number',
        'line_code'
    ]
}

# Optional columns for each junction table type (in lowercase for mapper)
MFT_JUNCTION_OPTIONAL = {
    'part_to_box_composite': ['part_per_box'],
    'box_to_pallet_composite': ['box_per_pallet'],
    'part_to_model': ['part_per_vehicle'],
    'part_to_line': []
}

# Expected columns for each core entity table type (in lowercase for loader)
MFT_TABLE_REQUIREMENTS = {
            'supplier_data': [
                'supplier_name',
                'location',
                'city',
                'street',
                'building',
                'localization'
            ],
            'part_data': [
                'part_number',
                'part_name',
                'part_weight_kg',
                'supplier_id'
            ],
            'box_data': [
                'box_type',
                'box_weight_kg',
                'box_length_mm',
                'box_width_mm',
                'box_height_mm',
                'box_stacking'
            ],
            'pallet_data': [
                'pallet_type',
                'pallet_weight_kg',
                'pallet_length_mm',
                'pallet_width_mm',
                'pallet_height_mm',
                'pallet_stacking'
            ],
            'model_data': [
                'model_code',
                'model_name'
            ],
            'workshop_data': [
                'workshop_code',
                'workshop_name'
            ],
            'line_data': [
                'line_code',
                'line_name',
                'workshop_id'
            ],
            'configuration_data': [
                'configuration',
                'transmission'
            ]
        }

# ========== BP PIPELINE CONFIGURATION ==========
# For use in bp_dag.py, bp_mapper.py and bp_loader.py

# ========== CORE ENTITY TABLES COLUMNS (UPPERCASE for Excel extraction) ==========
# Breakpoint entity columns
BREAKPOINT_COLS = [
    'BP_NO',
    'STATUS',
    'BATCH_PLAN',
    'BATCH_FACT',
    'CHANGE_DATE',
    'BOM_PRODUCT',
    'DESCRIPTION',
    'SOLUTION',
]

# Parts before (old parts) entity columns
PART_BEFORE_COLS = [
    'PART_NO_BEFORE',
    'PART_NAME_BEFORE',
    'BOM_PRODUCT',
    'CONFIGURATION',
    'WORKSHOP_BEFORE',
    'WORKCENTER_NO_BEFORE',
    'WORKCENTER_NAME_BEFORE',
    'QUANTITY_PER_VEHICLE_BEFORE',
    'QUANTITY_PER_BOX_BEFORE',
    'BOX_BEFORE',
    'PALLET_BEFORE',
    'DISPOSAL',
    'SUPPLIER_NAME_BEFORE',
    'LOCALIZATION_BEFORE',
]

# Parts after (new parts) entity columns
PART_AFTER_COLS = [
    'PART_NO_AFTER',
    'PART_NAME_AFTER',
    'BOM_PRODUCT',
    'CONFIGURATION',
    'WORKSHOP_AFTER',
    'WORKCENTER_NO_AFTER',
    'WORKCENTER_NAME_AFTER',
    'QUANTITY_PER_VEHICLE_AFTER',
    'QUANTITY_PER_BOX_AFTER',
    'BOX_AFTER',
    'PALLET_AFTER',
    'INTERCHANGEABLE',
    'SUPPLIER_NAME_AFTER',
    'LOCALIZATION_AFTER',
]

# ========== JUNCTION TABLES COLUMNS (UPPERCASE for Excel extraction) ==========
# Part-to-Breakpoints junction columns
PART_TO_BREAKPOINT_COLS = [
    'PART_NO_BEFORE',          # old_part_id lookup
    'PART_NO_AFTER',           # new_part_id lookup
    'BP_NO',                   # breakpoint_id lookup
    'BOM_PRODUCT',             # model_id lookup
]

# ========== BP JUNCTION CONFIGURATION ==========
# Required columns for breakpoint junction table (in lowercase for mapper)
BP_JUNCTION_REQUIRED = {
    'part_to_breakpoint': [
        'part_no_before',
        'part_no_after',
        'bp_no',
        'bom_product'
    ]
}

# ========== BP TABLE REQUIREMENTS ==========
# Expected columns for each core entity table type (in lowercase for loader)
BP_TABLE_REQUIREMENTS = {
    'breakpoint_data': [
        'breakpoint_number',
        'breakpoint_status',
        'batch_plan',
        'batch_fact',
        'breakpoint_date',
        'description',
        'solution'
    ],
    'part_data_before': [
        'part_number',
        'part_name',
        'bom_product',
        'configuration',
        'workshop_before',
        'workcenter_no_before',
        'workcenter_name_before',
        'quantity_per_vehicle_before',
        'quantity_per_box_before',
        'box_before',
        'pallet_before',
        'disposal',
        'supplier_name_before',
        'localization_before'
    ],
    'part_data_after': [
        'part_number',
        'part_name',
        'bom_product',
        'configuration',
        'workshop_after',
        'workcenter_no_after',
        'workcenter_name_after',
        'quantity_per_vehicle_after',
        'quantity_per_box_after',
        'box_after',
        'pallet_after',
        'interchangeable',
        'supplier_name_after',
        'localization_after'
    ]
}

# ========== BP ACTION TYPES ==========
# Action types for breakpoint changes
BP_ACTION_TYPES = {
    'ADD': 'add',           # new_part_id NOT NULL, old_part_id NULL
    'DELETE': 'delete',     # new_part_id NULL, old_part_id NOT NULL
    'UPDATE': 'update',     # new_part_id = old_part_id (same part)
    'REPLACE': 'replace'    # Two records (DELETE old + ADD new) in same breakpoint
}

# ========== BP REQUIRED FIELDS BY ACTION ==========
# Required fields for each action type
BP_REQUIRED_FIELDS_BY_ACTION = {
    'ADD': [
        'part_no_after',
        'part_name_after',
        'bom_product',
        'configuration',
        'workshop_after',
        'workcenter_no_after',
        'workcenter_name_after',
        'supplier_name_after',
        'localization_after',
        'box_after',
        'pallet_after'
    ],
    'DELETE': [
        'part_no_before',
        'part_name_before',
        'bom_product',
        'configuration',
        'workshop_before',
        'workcenter_no_before',
        'workcenter_name_before',
        'supplier_name_before',
        'localization_before',
        'box_before',
        'pallet_before'
    ],
    'UPDATE': [
        'part_no_before',
        'part_no_after',
        'part_name_before',
        'part_name_after',
        'bom_product',
        'configuration',
        'workshop_before',
        'workshop_after',
        'workcenter_no_before',
        'workcenter_no_after',
        'workcenter_name_before',
        'workcenter_name_after',
        'supplier_name_before',
        'supplier_name_after',
        'localization_before',
        'localization_after',
        'box_before',
        'box_after',
        'pallet_before',
        'pallet_after'
    ],
    'REPLACE': [
        'part_no_before',
        'part_no_after',
        'part_name_before',
        'part_name_after',
        'bom_product',
        'configuration',
        'workshop_before',
        'workshop_after',
        'workcenter_no_before',
        'workcenter_no_after',
        'workcenter_name_before',
        'workcenter_name_after',
        'supplier_name_before',
        'supplier_name_after',
        'localization_before',
        'localization_after',
        'box_before',
        'box_after',
        'pallet_before',
        'pallet_after'
    ]
}
