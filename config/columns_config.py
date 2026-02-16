# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Column configuration for Material Flow Table Database.

This module defines all required column names for extracting and transforming
manufacturing data from Excel files. These constants are used throughout the
ETL pipeline to ensure consistent column naming across tasks.

Constants:
    SUPPLIER_COLS: Columns for supplier entity table
    PART_COLS: Columns for part entity table
    BOX_COLS: Columns for box entity table (with dimensions)
    PALLET_COLS: Columns for pallet entity table (with dimensions)
    MODEL_COLS: Columns for model entity table
    WORKSHOP_COLS: Columns for workshop entity table
    LINE_COLS: Columns for production line entity table
    
    PART_TO_BOX_COMPOSITE_COLS: Columns for part-to-box junction (composite key)
    BOX_TO_PALLET_COMPOSITE_COLS: Columns for box-to-pallet junction (composite key)
    PART_TO_MODEL_COLS: Columns for part-to-model junction
    PART_TO_LINE_COLS: Columns for part-to-line junction

    COLUMN_TO_MODEL: Mapping of column names to SQLAlchemy models for mapper
    COMPOSITE_COLUMNS: Special composite key columns
    JUNCTION_REQUIRED_COLUMNS: Required columns for each junction type (lowercase)
    JUNCTION_OPTIONAL_COLUMNS: Optional columns for each junction type (lowercase)

Usage:
    from config.columns_config import SUPPLIER_COLS, PART_COLS, ...
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

# ========== MAPPER CONFIGURATION (LOWERCASE for database operations) ==========

# Special composite key columns that need custom handling
COMPOSITE_COLUMNS = {'box_composite', 'pallet_composite'}

# Expected columns for each junction table type (in lowercase for mapper)
JUNCTION_REQUIRED_COLUMNS = {
    'part_to_box_composite': [
        'part_number',
        'box_type',
        'box_length_mm',
        'box_width_mm',
        'box_height_mm'
    ],
    'box_to_pallet_composite': [
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
        'model_code'
    ],
    'part_to_line': [
        'part_number',
        'line_code'
    ]
}

# Optional columns for each junction table type (in lowercase for mapper)
JUNCTION_OPTIONAL_COLUMNS = {
    'part_to_box_composite': ['part_per_box'],
    'box_to_pallet_composite': ['box_per_pallet'],
    'part_to_model': ['configuration', 'part_per_vehicle'],
    'part_to_line': []
}

# ========== LOADER CONFIGURATION (LOWERCASE for database operations) ==========
# Expected columns for each core entity table type (in lowercase for loader)
TABLE_REQUIREMENTS = {
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
        }
