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
        - PART_NUMBER, BREAKPOINT_NUMBER, SUPPLIER_NAME, LINE_CODE,
          PART_NUMBER_BEFORE_CHANGE, SUPPLIER_NAME_BEFORE_CHANGE,
          LOCALIZATION_BEFORE_CHANGE, LINE_NAME_BEFORE_CHANGE

MAPPER CONFIGURATION (LOWERCASE for database operations):

    COMPOSITE_COLUMNS: Special composite key columns requiring custom handling
        - box_composite, pallet_composite

    JUNCTION_REQUIRED_COLUMNS: Required columns for each junction type
        - part_to_box_composite: part_number, box_type, box_length_mm,
          box_width_mm, box_height_mm
        - box_to_pallet_composite: part_number, box_type, box_length_mm,
          box_width_mm, box_height_mm, pallet_type, pallet_length_mm,
          pallet_width_mm, pallet_height_mm
        - part_to_model: part_number, model_code, configuration
        - part_to_line: part_number, line_code
        - part_to_breakpoint: part_number, breakpoint_number

    JUNCTION_OPTIONAL_COLUMNS: Optional columns for each junction type
        - part_to_box_composite: part_per_box
        - box_to_pallet_composite: box_per_pallet
        - part_to_model: part_per_vehicle
        - part_to_line: []
        - part_to_breakpoint: supplier_name, line_code, 
          part_number_before_change, supplier_name_before_change,
          localization_before_change, line_name_before_change


LOADER CONFIGURATION (LOWERCASE for database operations):

    TABLE_REQUIREMENTS: Expected columns for each core entity table
        - supplier_data: supplier_name, location, city, street, 
          building, localization
        - part_data: part_number, part_name, part_weight_kg, supplier_id
        - box_data: box_type, box_weight_kg, box_length_mm, box_width_mm,
          box_height_mm, box_stacking
        - pallet_data: pallet_type, pallet_weight_kg, pallet_length_mm,
          pallet_width_mm, pallet_height_mm, pallet_stacking
        - model_data: model_code, model_name
        - workshop_data: workshop_code, workshop_name
        - line_data: line_code, line_name, workshop_id
        - configuration_data: configuration, description
        - breakpoint_data: breakpoint_number, breakpoint_date

USAGE EXAMPLES:

    # Import columns for Excel extraction
    from config.columns_config import (
        SUPPLIER_COLS, PART_COLS, BOX_COLS, PALLET_COLS,
        PART_TO_BOX_COMPOSITE_COLS, PART_TO_MODEL_COLS,
        PART_TO_BREAKPOINT_COLS
    )
    
    # Use in ETL pipeline
    df = pd.read_excel('data.xlsx', usecols=PART_TO_BREAKPOINT_COLS)
    
    # Validate junction data
    from config.columns_config import (
        JUNCTION_REQUIRED_COLUMNS,
        JUNCTION_OPTIONAL_COLUMNS
    )
    
    def validate_junction_data(junction_type, data):
        required = JUNCTION_REQUIRED_COLUMNS[junction_type]
        optional = JUNCTION_OPTIONAL_COLUMNS[junction_type]
        # Validation logic...

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
        breakpoint_data      ←→ BREAKPOINT_COLS
    
    Junction Tables:
        part_to_box          ←→ PART_TO_BOX_COMPOSITE_COLS
        box_to_pallet        ←→ BOX_TO_PALLET_COMPOSITE_COLS
        part_to_model        ←→ PART_TO_MODEL_COLS
        part_to_line         ←→ PART_TO_LINE_COLS
        part_to_breakpoint   ←→ PART_TO_BREAKPOINT_COLS

Note: All constants follow naming convention:
    - UPPERCASE_WITH_UNDERSCORES for Excel column names
    - lowercase_with_underscores for database operations
    - *_COLS for lists of column names
    - *_REQUIRED_COLUMNS for mandatory fields
    - *_OPTIONAL_COLUMNS for optional fields

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-03-11
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
    'SUPPLIER_NAME',
    'LINE_CODE',
    'PART_NUMBER_BEFORE_CHANGE',
    'SUPPLIER_NAME_BEFORE_CHANGE',
    'LOCALIZATION_BEFORE_CHANGE',
    'LINE_NAME_BEFORE_CHANGE',
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
    ],
    'part_to_breakpoint': [
        'part_number',
        'breakpoint_number'
    ],
}

# Optional columns for each junction table type (in lowercase for mapper)
JUNCTION_OPTIONAL_COLUMNS = {
    'part_to_box_composite': ['part_per_box'],
    'box_to_pallet_composite': ['box_per_pallet'],
    'part_to_model': ['part_per_vehicle'],
    'part_to_line': [],
    'part_to_breakpoint': [
        'supplier_name',
        'line_code',
        'part_number_before_change',
        'supplier_name_before_change',
        'localization_before_change',
        'line_name_before_change'
    ],
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
            'configuration_data': [
                'configuration',
                'description'
            ],
            'breakpoint_data': [
                'breakpoint_number',
                'breakpoint_date'
            ],
        }
