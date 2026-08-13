# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Universal ENUM Validation Module for Material Flow Table Database.

This module provides comprehensive ENUM validation functionality for the ETL pipeline,
serving as a critical data quality layer that ensures all ENUM values conform to
database constraints before loading. It is designed to be used in both MFT and BP
DAGs during the transformation phase.

Key Features:
    - Database-synchronized validation (ENUM values imported directly from database.py)
    - Simple function interface matching existing transformation functions
    - Automatic handling of NULL values, case variations, and whitespace
    - Comprehensive logging with invalid value sampling
    - Support for all ENUM types in the database schema
    - Support for BP pipeline columns (before/after variants, breakpoint status)

Architecture:
    The module follows a simple functional design pattern:
    1. ENUM Import: Direct imports from database.py ensure schema synchronization
    2. Column Mapping: Dictionary maps column names to corresponding ENUM types
    3. Validation Logic: Core validation with cleaning, masking, and replacement
    4. Logging Layer: Detailed DEBUG/INFO/WARNING logging for monitoring

Configuration Source:
    This module imports ENUM types directly from database.py:
        - LOCALIZATION_ENUM: Supplier localization status (yes/no/no data)
        - PACKAGING_TYPE_ENUM: Returnable/non-returnable packaging
        - MODEL_CODES_ENUM: Vehicle platform codes (a01, a08, b02, etc.)
        - MODEL_NAMES_ENUM: Vehicle marketing names (jolion, h3, f7, etc.)
        - WORKSHOP_CODES_ENUM: Production workshop codes (as, comp, paint, etc.)
        - WORKSHOP_NAMES_ENUM: Full workshop names (assembly, component, etc.)
        - CONFIGURATION_ENUM: Vehicle trim levels (comfort, elite, tech-plus, premium)
        - BREAKPOINT_STATUS_ENUM: Engineering change status types (approved, published, closed)

Column Mapping Support:
    The module supports both MFT and BP pipeline columns:
    
    MFT Pipeline:
        - LOCALIZATION, localization
        - BOX_TYPE, box_type, PALLET_TYPE, pallet_type
        - MODEL_CODE, model_code, MODEL_NAME, model_name
        - WORKSHOP_CODE, workshop_code, WORKSHOP_NAME, workshop_name
        - CONFIGURATION, configuration
    
    BP Pipeline:
        - localization_before, LOCALIZATION_BEFORE
        - localization_after, LOCALIZATION_AFTER
        - box_before, BOX_BEFORE, box_after, BOX_AFTER
        - pallet_before, PALLET_BEFORE, pallet_after, PALLET_AFTER
        - workshop_before, WORKSHOP_BEFORE, workshop_after, WORKSHOP_AFTER
        - bom_product, BOM_PRODUCT
        - breakpoint_status, STATUS, status

Dependencies:
    - Polars for DataFrame operations
    - SQLAlchemy for ENUM access (via database.py)
    - PostgreSQL 12+ as the target database

Usage Example:
    from dags.tasks.enum_validator import enum_validate

    # MFT Pipeline usage
    df = pl.DataFrame({"LOCALIZATION": ["yes", "no", "invalid", "YES", None]})
    validated_df = enum_validate(df, "LOCALIZATION")
    # Result: ["yes", "no", "no data", "yes", "no data"]

    # BP Pipeline usage
    df = pl.DataFrame({
        "breakpoint_status": ["approved", "published", "invalid", "closed"],
        "workshop_before": ["as", "comp", "invalid", "paint"]
    })
    validated_df = enum_validate(df, "breakpoint_status")
    validated_df = enum_validate(df, "workshop_before")
    # Invalid values replaced with 'no data'

Integration Notes:
    - Must be used AFTER convert_to_string() and basic_clean_text()
    - Can be used in both MFT and BP pipeline transformation phases
    - No database connection required (uses imported ENUM definitions only)
    - Column names are case-sensitive, both UPPERCASE and lowercase supported
    - BP pipeline columns with _before/_after suffixes are fully supported

Performance Considerations:
    - Vectorized operations via Polars (no row-by-row loops)
    - Single pass per column with minimal temporary columns
    - Memory efficient with in-place updates where possible
    - Scales linearly to millions of rows

Error Handling:
    - Missing columns: Logs warning and returns DataFrame unchanged
    - No ENUM mapping: Logs debug and skips validation
    - Empty allowed values: Logs warning and skips validation
    - NULL values: Gracefully converted to empty strings
    - Invalid values: Replaced with 'no data', logged with samples

Version: 1.0.0
Compatibility: Python 3.14.4+, Polars 1.36.1
Maintainer: PLD Engineering Center
Created: 2026-03-18
Last Modified: 2026-08-13
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
from typing import List

# Third-party imports
import polars as pl

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from database.database import (
    LOCALIZATION_ENUM,
    PACKAGING_TYPE_ENUM,
    MODEL_CODES_ENUM,
    MODEL_NAMES_ENUM,
    WORKSHOP_CODES_ENUM,
    WORKSHOP_NAMES_ENUM,
    CONFIGURATION_ENUM,
    BREAKPOINT_STATUS_ENUM
)

# Logger setup
logger = get_logger(__name__)


# Dictionary mapping column names to ENUM objects
# This allows direct lookup by column name as used in DAGs
_COLUMN_TO_ENUM = {
    # LOCALIZATION_ENUM mappings
    'LOCALIZATION': LOCALIZATION_ENUM,
    'localization': LOCALIZATION_ENUM,
    'localization_before': LOCALIZATION_ENUM,
    'LOCALIZATION_BEFORE': LOCALIZATION_ENUM,
    'localization_after': LOCALIZATION_ENUM,
    'LOCALIZATION_AFTER': LOCALIZATION_ENUM,

    # PACKAGING_TYPE_ENUM mappings
    'BOX_TYPE': PACKAGING_TYPE_ENUM,
    'box_type': PACKAGING_TYPE_ENUM,
    'PALLET_TYPE': PACKAGING_TYPE_ENUM,
    'pallet_type': PACKAGING_TYPE_ENUM,
    'box_before': PACKAGING_TYPE_ENUM,
    'BOX_BEFORE': PACKAGING_TYPE_ENUM,
    'box_after': PACKAGING_TYPE_ENUM,
    'BOX_AFTER': PACKAGING_TYPE_ENUM,
    'pallet_before': PACKAGING_TYPE_ENUM,
    'PALLET_BEFORE': PACKAGING_TYPE_ENUM,
    'pallet_after': PACKAGING_TYPE_ENUM,
    'PALLET_AFTER': PACKAGING_TYPE_ENUM,

    # MODEL_CODES_ENUM mappings
    'MODEL_CODE': MODEL_CODES_ENUM,
    'model_code': MODEL_CODES_ENUM,
    'bom_product': MODEL_CODES_ENUM,
    'BOM_PRODUCT': MODEL_CODES_ENUM,

    # MODEL_NAMES_ENUM mappings
    'MODEL_NAME': MODEL_NAMES_ENUM,
    'model_name': MODEL_NAMES_ENUM,

    # WORKSHOP_CODES_ENUM mappings
    'WORKSHOP_CODE': WORKSHOP_CODES_ENUM,
    'workshop_code': WORKSHOP_CODES_ENUM,
    'workshop_before': WORKSHOP_CODES_ENUM,
    'WORKSHOP_BEFORE': WORKSHOP_CODES_ENUM,
    'workshop_after': WORKSHOP_CODES_ENUM,
    'WORKSHOP_AFTER': WORKSHOP_CODES_ENUM,

    # WORKSHOP_NAMES_ENUM mappings
    'WORKSHOP_NAME': WORKSHOP_NAMES_ENUM,
    'workshop_name': WORKSHOP_NAMES_ENUM,

    # CONFIGURATION_ENUM mappings
    'CONFIGURATION': CONFIGURATION_ENUM,
    'configuration': CONFIGURATION_ENUM,

    # BREAKPOINT_STATUS_ENUM mappings
    'breakpoint_status': BREAKPOINT_STATUS_ENUM,
    'STATUS': BREAKPOINT_STATUS_ENUM,
    'status': BREAKPOINT_STATUS_ENUM,
}


def _get_enum_values(enum_obj) -> List[str]:
    """
    Extract allowed values from SQLAlchemy Enum object.
    
    Args:
        enum_obj: SQLAlchemy Enum object from database.py
        
    Returns:
        List of allowed string values
    """
    if hasattr(enum_obj, 'enums'):
        # For SQLAlchemy Enum
        return list(enum_obj.enums)
    elif hasattr(enum_obj, 'values'):
        # Alternative access method
        return list(enum_obj.values)
    else:
        logger.error("Could not extract values from enum: %s", enum_obj)
        return []


def enum_validate(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    """
    Validate a column against its corresponding ENUM type.

    This function automatically detects the appropriate ENUM type based on
    the column name and validates all values in the column. Invalid values
    are replaced with 'no data'.

    Args:
        df: Polars DataFrame
        column_name: Name of column to validate (e.g., 'LOCALIZATION', 'BOX_TYPE')

    Returns:
        DataFrame with validated column

    Example:
        >>> import polars as pl
        >>> from dags.tasks.enum_validator import enum_validate
        >>> 
        >>> df = pl.DataFrame({
        ...     "LOCALIZATION": ["yes", "no", "invalid", "YES", None],
        ...     "OTHER_COL": [1, 2, 3, 4, 5]
        ... })
        >>> 
        >>> # Simple usage - just like convert_to_string()
        >>> validated_df = enum_validate(df, "LOCALIZATION")
        >>> 
        >>> print(validated_df)
        shape: (5, 2)
        ┌──────────────┬───────────┐
        │ LOCALIZATION ┆ OTHER_COL │
        │ ---          ┆ ---       │
        │ str          ┆ i64       │
        ╞══════════════╪═══════════╡
        │ yes          ┆ 1         │
        │ no           ┆ 2         │
        │ no data      ┆ 3         │
        │ yes          ┆ 4         │
        │ no data      ┆ 5         │
        └──────────────┴───────────┘
    """
    # Check if column exists
    if column_name not in df.columns:
        logger.warning("Column '%s' not found in DataFrame", column_name)
        return df

    # Get the appropriate ENUM for this column
    enum_obj = _COLUMN_TO_ENUM.get(column_name)

    if enum_obj is None:
        logger.debug(
            "No ENUM mapping found for column '%s', skipping validation",
            column_name
        )
        return df

    # Get allowed values from the ENUM
    allowed_values = _get_enum_values(enum_obj)

    if not allowed_values:
        logger.warning("No allowed values found for column '%s'", column_name)
        return df

    # Create a temporary column with cleaned values
    # Handle null values by converting to string first
    temp_col = f"__temp_{column_name}"
    df = df.with_columns(
        pl.when(pl.col(column_name).is_null())
        .then(pl.lit(""))
        .otherwise(pl.col(column_name))
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_lowercase()
        .alias(temp_col)
    )

    # Find invalid values (empty string or not in allowed values)
    invalid_mask = (
        (pl.col(temp_col) == "") |
        (~pl.col(temp_col).is_in(allowed_values))
    )
    invalid_count = df.filter(invalid_mask).height

    if invalid_count > 0:
        # Get sample of invalid values for logging
        invalid_samples = (
            df.filter(invalid_mask & (pl.col(temp_col) != ""))
            .select(temp_col)
            .unique()
            .head(5)
            .to_series()
            .to_list()
        )

        logger.warning(
            "Found %d invalid value(s) in column '%s'. "
            "Allowed values: %s. Sample: %s",
            invalid_count, column_name, allowed_values, invalid_samples
        )

        # Replace invalid values with 'no data'
        df = df.with_columns(
            pl.when(invalid_mask)
            .then(pl.lit('no data'))
            .otherwise(pl.col(temp_col))
            .alias(column_name)
        )
    else:
        # All values are valid, use cleaned values
        df = df.with_columns(
            pl.col(temp_col).alias(column_name)
        )

    # Remove temporary column
    df = df.drop(temp_col)

    return df
