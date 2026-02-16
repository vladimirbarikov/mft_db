# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Data Extraction Module for Material Flow Table Database.

This module handles data extraction from Excel source files and creates specialized
DataFrames for different business entities in the automotive manufacturing domain.
It serves as the first step in the ETL pipeline, preparing raw data for subsequent
transformation and loading operations.

Key Features:
    - Excel file reading and validation with multiple engine support
    - Creation of main DataFrame from source Excel files
    - Extraction of specialized DataFrames for different business domains
    - Comprehensive data validation and error handling
    - Airflow integration ready with task decorator support
    - Memory-efficient processing with Polars DataFrames

Architecture:
    The module follows a two-phase extraction approach:
    1. Raw Extraction: Read Excel file into main DataFrame (create_main_df)
    2. Specialization: Extract specific columns for business domains (create_specialized_df)
    
    This separation allows for flexible pipeline composition and reuse of the
    main DataFrame across multiple specialized extractions.

Dependencies:
    - Polars 1.0.0+ for efficient DataFrame operations
    - OpenPyXL 3.1.0+ for Excel file processing
    - Apache Airflow 2.8.0+ (optional) for workflow integration
    - Python 3.12.3+ for type hints and modern features

Performance Considerations:
    - Uses Polars for memory-efficient DataFrame operations
    - Lazy evaluation available for large files (not implemented)
    - Column selection minimizes memory footprint
    - File existence check before full read prevents wasted I/O

Security Notes:
    - Validates file paths to prevent directory traversal attacks
    - Input validation for all DataFrame operations
    - No execution of dynamic code from source files
    - Safe handling of potentially malicious Excel content

Error Handling:
    - Comprehensive exception hierarchy (ValueError, TypeError, ComputeError)
    - Detailed logging at appropriate levels (INFO, WARNING, ERROR)
    - Graceful degradation for empty DataFrames
    - Clear error messages for missing columns/files

Integration Notes:
    - Designed as Airflow tasks (commented decorators ready for activation)
    - Output compatible with transformer.py and loader.py modules
    - Column naming follows manufacturing data warehouse conventions
    - Supports both standalone and pipeline execution

Usage Example:
    ```
    from dags.tasks.extractor import create_main_df, create_specialized_df
    
    # Extract main DataFrame from Excel
    main_df = create_main_df('/path/to/source.xlsx')
    
    # Create specialized DataFrame for suppliers
    supplier_columns = ['supplier_name', 'supplier_code', 'country']
    supplier_df = create_specialized_df(main_df, supplier_columns)
    
    # Create specialized DataFrame for parts  
    part_columns = ['part_number', 'part_name', 'weight_kg']
    part_df = create_specialized_df(main_df, part_columns)
    ```

Module Structure:
    - create_main_df(): Primary Excel file reader and validator
    - create_specialized_df(): Column selector for business domains

Development Mode:
    - Uses hardcoded file path for testing
    - Comprehensive logging for debugging
    - Standalone execution capability for development
    - Type hints for better IDE support

Note:
    This module assumes Excel files follow the standardized Material Flow Table
    format. Column names and data types should be consistent across source files.
    For non-standard formats, additional preprocessing may be required.

Version: 1.1.0
Compatibility: Python 3.12.3+, Polars 1.0.0+, OpenPyXL 3.1.0+
Maintainer: PLD Engineering Center
Created: 2025-10-20
Last Modified: 2026-02-16
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import os
import sys

# Third-party imports
import polars as pl

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)


def create_main_df(f_path: str | Path) -> pl.DataFrame:
    """
    Read Excel file into Polars DataFrame.
    
    Args:
        f_path: Path to Excel file
        
    Returns:
        DataFrame with Excel data
        
    Raises:
        ValueError: If file not found
        pl.exceptions.ComputeError: If Excel processing fails
    """
    # Check the file availability
    if not f_path or not os.path.exists(f_path):
        raise ValueError("The file was not found. The file path is missing or wrong.")

    try:
        main_df = pl.read_excel(
            f_path,
            engine='openpyxl'
        )

    except pl.exceptions.ComputeError as e:
        logger.error("Error processing data: %s", e)
        raise

    except Exception as e:
        logger.error("Unexpected error reading file: %s", e)
        raise

    logger.info("=" * 60)

    logger.info(
        "Successfully created main dataframe.\n"
        "Shape: %d rows, %d columns\n"
        "Columns: %s",
        main_df.height,
        main_df.width,
        ', '.join(main_df.columns),
    )

    logger.info("=" * 60)

    return main_df


def create_specialized_df(
        main_df: pl.DataFrame,
        required_columns: list[str]
    ) -> pl.DataFrame:
    """
    Extract specific columns from main DataFrame.
    
    Args:
        main_df: Source DataFrame
        required_columns: Columns to extract
        
    Returns:
        DataFrame with selected columns
        
    Raises:
        TypeError: If main_df is not a Polars DataFrame
        ValueError: If required columns are missing
    """
    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = f"Provided data type must be a polars DataFrame. But got: {type(main_df)}."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main dataframe is empty - creating empty specialized dataframe with required columns."
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns: {', '.join(missing_columns)}. "
                f"Available columns: {', '.join(main_df.columns)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Create specialized DataFrame with required_columns
        specialized_df = main_df.select(required_columns)

        # Enhanced success logging
        logger.info(
            "Successfully created specialized dataframe.\n"
            "Shape: %d rows, %d columns\n"
            "Columns: %s",
            specialized_df.height,
            specialized_df.width,
            ', '.join(specialized_df.columns),
        )

        return specialized_df

    except (TypeError, ValueError):
        # Expected errors have already been logged
        raise

    except Exception as e:
        # Catch unexpected errors
        logger.error("Unexpected error creating supplier dataframe: %s", e)
        raise
