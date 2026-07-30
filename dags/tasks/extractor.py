# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Data Extraction Module for Material Flow Table Database.

This module handles data extraction from Excel source files and creates specialized
DataFrames for different business entities in the automotive manufacturing domain.
It serves as the first step in the ETL pipeline, preparing raw data for subsequent
transformation and loading operations.

Key Features:
    - Streaming Excel file reading from memory (bytes) - no disk I/O
    - Creation of main DataFrame from Excel content
    - Extraction of specialized DataFrames for different business domains
    - Comprehensive data validation and error handling
    - Airflow integration ready with task decorator support
    - Memory-efficient processing with Polars DataFrames

Architecture:
    The module follows a streaming-first approach:
    1. Raw Extraction: Read Excel content from bytes into main DataFrame
    2. Specialization: Extract specific columns for business domains

    This separation allows for flexible pipeline composition and reuse of the
    main DataFrame across multiple specialized extractions.

Streaming Mode:
    All Excel content is processed from memory using io.BytesIO.
    No disk I/O operations are performed, making it ideal for:
    - Upload API streaming endpoints
    - Containerized deployments with ephemeral storage
    - Environments where file system access is restricted

Dependencies:
    - Polars 1.32.3 for efficient DataFrame operations
    - OpenPyXL 3.1.0+ for Excel file processing
    - Apache Airflow 2.8.0+ (optional) for workflow integration
    - Python 3.12.3+ for type hints and modern features

Version: 2.0.0
Compatibility: Python 3.14.4+, Polars 1.32.3, OpenPyXL 3.1.5+
Maintainer: PLD Engineering Center
Created: 2025-10-20
Last Modified: 2026-07-27
License: MIT
Status: Production Ready
"""
# Standard library imports
from pathlib import Path
import sys
import io

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


def create_main_df(file_content: bytes) -> pl.DataFrame:
    """
    Create Polars DataFrame from Excel file content in memory (bytes).

    Reads Excel data directly from memory without saving to disk.
    This function is used in streaming upload mode where files are
    processed entirely in memory.

    Args:
        file_content (bytes): Raw Excel file content as bytes

    Returns:
        pl.DataFrame: DataFrame with Excel data

    Raises:
        ValueError: If file_content is empty or invalid
        pl.exceptions.ComputeError: If Excel processing fails

    Example:
        >>> # From uploaded file
        >>> content = await file.read()
        >>> df = create_main_df(content)

        >>> # From base64 decoded content
        >>> content = base64.b64decode(encoded_data)
        >>> df = create_main_df(content)
    """
    if not file_content:
        raise ValueError("File content is empty or None")

    try:
        main_df = pl.read_excel(
            io.BytesIO(file_content),
            engine='openpyxl'
        )

    except pl.exceptions.ComputeError as e:
        logger.error("Error processing Excel data from memory: %s", e)
        raise

    except Exception as unexpected_error:
        logger.error("Unexpected error reading Excel from memory: %s", unexpected_error)
        raise

    logger.info("=" * 60)
    logger.info(
        "Successfully created main dataframe from memory.\n"
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
        pl.DataFrame: DataFrame with selected columns

    Raises:
        TypeError: If main_df is not a Polars DataFrame
        ValueError: If required columns are missing

    Example:
        >>> main_df = create_main_df(file_content)
        >>> supplier_columns = ['supplier_name', 'supplier_code', 'country']
        >>> supplier_df = create_specialized_df(main_df, supplier_columns)
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

    except Exception as unexpected_error:
        # Catch unexpected errors
        logger.error("Unexpected error creating supplier dataframe: %s", unexpected_error)
        raise
