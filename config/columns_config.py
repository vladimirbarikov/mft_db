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
        - CONFIGURATION, DESCRIPTION

    WORKSHOP_COLS: Columns for workshop entity table
        - WORKSHOP_CODE, WORKSHOP_NAME

    LINE_COLS: Columns for production line entity table
        - LINE_CODE, LINE_NAME, WORKSHOP_CODE

    BREAKPOINT_COLS: Columns for breakpoint entity table (technical changes)
        - BREAKPOINT_NUMBER, BREAKPOINT_DATE, DESCRIPTION, BATCH

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
        - PART_NUMBER, BREAKPOINT_NUMBER, MODEL_CODE, ACTION,
          SUPPLIER_NAME, LINE_CODE, PART_NUMBER_BEFORE_CHANGE,
          SUPPLIER_NAME_BEFORE_CHANGE, LOCALIZATION_BEFORE_CHANGE,
          LINE_NAME_BEFORE_CHANGE

MFT PIPELINE CONFIGURATION (for mft_dag.py, mft_mapper.py, mft_loader.py)
    MFT_JUNCTION_REQUIRED: Required columns for MFT pipeline junction tables
    MFT_JUNCTION_OPTIONAL: Optional columns for MFT pipeline junction tables
    MFT_TABLE_REQUIREMENTS: Required columns for MFT pipeline core tables
    MFT_EXCEL_COLUMNS: Excel column mappings for MFT pipeline

BP PIPELINE CONFIGURATION (for bp_dag.py, bp_mapper.py, bp_loader.py)
    BP_JUNCTION_REQUIRED: Required columns for BP pipeline junction tables
                        (includes MODEL_CODE and ACTION as mandatory fields)
    BP_JUNCTION_OPTIONAL: Optional columns for BP pipeline junction tables
    BP_TABLE_REQUIREMENTS: Required columns for BP pipeline core tables
    BP_EXCEL_COLUMNS: Excel column mappings for BP pipeline
    BP_LOOKUP_TABLES: Tables needed for ID lookups in BP pipeline
                        (supplier, line, part, breakpoint, model)

BP PIPELINE CONFIGURATION (for bp_dag.py, bp_mapper.py, bp_loader.py)
    BP_JUNCTION_REQUIRED: Required columns for BP pipeline junction tables
                        (includes MODEL_CODE and ACTION as mandatory fields)
    BP_JUNCTION_OPTIONAL: Optional columns for BP pipeline junction tables
    BP_TABLE_REQUIREMENTS: Required columns for BP pipeline core tables
    BP_EXCEL_COLUMNS: Excel column mappings for BP pipeline
    BP_LOOKUP_TABLES: Tables needed for ID lookups in BP pipeline
                        (supplier, line, part, breakpoint, model)

DATABASE MODEL MAPPING:

    Core Entity Tables:
        supplier_data        ←→ SUPPLIER_COLS
        part_data            ←→ PART_COLS
        box_data             ←→ BOX_COLS
        pallet_data          ←→ PALLET_COLS
        model_data           ←→ MODEL_COLS
        configuration_data   ←→ CONFIGURATION_COLS
        workshop_data        ←→ WORKSHOP_COLS
        line_data            ←→ LINE_COLS
        breakpoint_data      ←→ BREAKPOINT_COLS (used in BP pipeline)
    
    Junction Tables:
        part_to_box          ←→ Used only in MFT pipeline
        box_to_pallet        ←→ Used only in MFT pipeline
        part_to_model        ←→ Used only in MFT pipeline
        part_to_line         ←→ Used only in MFT pipeline
        part_to_breakpoint   ←→ Used only in BP pipeline
                              (includes MODEL_CODE and ACTION for change tracking)

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-03-12
License: MIT
Status: Production
"""

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
    'DESCRIPTION'
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

# Breakpoint entity columns (technical changes)
BREAKPOINT_COLS = [
    'BREAKPOINT_NUMBER',
    'BREAKPOINT_DATE',
    'DESCRIPTION',
    'BATCH',
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

# Part-to-Breakpoints junction columns
PART_TO_BREAKPOINT_COLS = [
    'PART_NUMBER',
    'BREAKPOINT_NUMBER',
    'MODEL_CODE',
    'ACTION',
    'SUPPLIER_NAME',
    'LINE_CODE',
    'LINE_NAME_BEFORE_CHANGE',
    'PART_NUMBER_BEFORE_CHANGE',
    'SUPPLIER_NAME_BEFORE_CHANGE',
    'LOCALIZATION_BEFORE_CHANGE',
]

# ========== MFT PIPELINE CONFIGURATION ==========
# For use in mft_dag.py, mft_mapper.py, and mft_loader.py

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
                'description'
            ]
        }

# Excel column mappings for MFT pipeline
MFT_EXCEL_COLUMNS = {
    'supplier': SUPPLIER_COLS,
    'part': PART_COLS,
    'box': BOX_COLS,
    'pallet': PALLET_COLS,
    'model': MODEL_COLS,
    'configuration': CONFIGURATION_COLS,
    'workshop': WORKSHOP_COLS,
    'line': LINE_COLS,
    'part_to_box': PART_TO_BOX_COMPOSITE_COLS,
    'box_to_pallet': BOX_TO_PALLET_COMPOSITE_COLS,
    'part_to_model': PART_TO_MODEL_COLS,
    'part_to_line': PART_TO_LINE_COLS,
    # Note: 'part_to_breakpoint' is used only in BP_EXCEL_COLUMNS
}

# ========== BP PIPELINE CONFIGURATION ==========
# For use in bp_dag.py, bp_mapper.py, and bp_loader.py

# Required columns for breakpoint junction table (in lowercase for mapper)
BP_JUNCTION_REQUIRED = {
    'part_to_breakpoint': [
        'part_number',
        'breakpoint_number',
        'model_code'
    ]
}

# Optional columns for breakpoint junction table (in lowercase for mapper)
BP_JUNCTION_OPTIONAL = {
    'part_to_breakpoint': [
        'action',
        'supplier_name',
        'line_code',
        'part_number_before_change',
        'supplier_name_before_change',
        'localization_before_change',
        'line_name_before_change'
    ]
}

# Expected columns for breakpoint core table
BP_TABLE_REQUIREMENTS = {
    'breakpoint_data': [
        'breakpoint_number',
        'breakpoint_date',
        'description',
        'batch'
    ]
}

# Excel column mappings for BP pipeline
BP_EXCEL_COLUMNS = {
    'breakpoint': BREAKPOINT_COLS,
    'part_to_breakpoint': PART_TO_BREAKPOINT_COLS,
}

# Tables needed for ID lookups in BP pipeline
BP_LOOKUP_TABLES = {
    'supplier': SUPPLIER_COLS,
    'line': LINE_COLS,
    'part': PART_COLS,
    'breakpoint': ['breakpoint_number'],
    'model': ['model_code']
}
