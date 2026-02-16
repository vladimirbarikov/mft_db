# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Data Transformation Module for Material Flow Table Database.

This module provides comprehensive data transformation and cleaning functionality
for automotive manufacturing data. It handles type conversions, text normalization,
and data quality improvements for multiple DataFrames containing supplier, part,
packaging, and production data.

Key Features:
    - Type conversion functions for Int64, string, and float data types with null safety
    - Automatic Chinese character detection and pinyin conversion using pypinyin
    - Multilingual CamelCase text processing (Cyrillic and Latin alphabets)
    - Advanced text cleaning with punctuation removal and normalization
    - Graceful fallback mechanisms with basic cleaning as backup
    - Comprehensive error handling and detailed logging at multiple levels

Architecture:
    The module follows a layered transformation approach:
    1. Type Conversion Layer: Convert data types while preserving null values
    2. Text Processing Layer: Clean and normalize text with language detection
    3. Fallback Layer: Basic cleaning when advanced transformations fail
    
    Functions are designed to be composable, allowing complex transformation
    pipelines to be built from simple, reusable components.

Dependencies:
    - Polars 1.0.0+ for efficient DataFrame operations and type casting
    - PyPinyin 0.50.0+ for Chinese character to pinyin conversion
    - Python 3.12.3+ for type hints and modern string handling
    - Standard library: re for regex operations, sys for path management

Performance Considerations:
    - Uses Polars' vectorized operations for optimal performance
    - Map_elements used only when necessary (Chinese detection)
    - Minimal string copying through in-place transformations
    - Early column existence checks prevent wasted processing
    - Caching not required due to stateless transformation functions

Security Notes:
    - No execution of dynamic code from input data
    - Input validation for all DataFrame operations
    - Safe handling of special characters and Unicode
    - No external network calls or file system access beyond input
    - Regex patterns are precompiled for safety and performance

Error Handling:
    - Comprehensive exception hierarchy with appropriate logging levels
    - Graceful degradation with fallback to basic cleaning
    - Column existence validation before transformation attempts
    - Null value preservation throughout all transformations
    - Detailed error messages with column context for debugging
    - Unexpected errors are caught and logged separately for debugging

Integration Notes:
    - Designed to work with extractor.py output (Polars DataFrames)
    - Output compatible with loader.py input requirements
    - Column naming conventions match manufacturing data warehouse standards
    - Functions can be chained for complex transformation pipelines
    - Airflow task compatible with proper error propagation

Usage Example:
    ```
    from dags.tasks.transformer import (
        convert_to_int64, 
        convert_to_string,
        convert_to_float,
        advanced_clean_text,
        basic_clean_text
    )
    
    # Transform a supplier DataFrame
    supplier_df = convert_to_string(supplier_df, 'supplier_name')
    supplier_df = advanced_clean_text(supplier_df, 'supplier_name')
    supplier_df = convert_to_float(supplier_df, 'annual_volume')
    
    # Transform a part DataFrame with error handling
    try:
        part_df = convert_to_int64(part_df, 'part_quantity')
        part_df = advanced_clean_text(part_df, 'part_description')
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        # Fallback to basic cleaning
        part_df = basic_clean_text(part_df, 'part_description')
    ```

Module Structure:
    - convert_to_int64(): Integer conversion with null safety
    - convert_to_string(): String conversion with type preservation
    - convert_to_float(): Float conversion with decimal rounding
    - basic_clean_text(): Minimal text normalization
    - advanced_clean_text(): Comprehensive multilingual text cleaning
    - Internal helper functions: Chinese detection and pinyin conversion

Text Processing Pipeline:
    Chinese Text:     汉字 → pinyin → cleaning → "Han Zi"
    CamelCase:        "engineMount" → "engine Mount" → "Engine Mount"
    Mixed Text:       "Part-123 (Special)" → "Part 123 Special"
    Cyrillic+Latin:   "ДвигательEngine" → "Двигатель Engine"

Note:
    This module assumes input DataFrames follow the standardized Material Flow Table
    format. For non-standard data, additional preprocessing may be required before
    applying these transformations. The module is designed to be stateless and
    thread-safe when used with separate DataFrame instances.

Version: 1.0.0
Compatibility: Python 3.12.3+, Polars 1.0.0+, PyPinyin 0.50.0+
Maintainer: PLD Engineering Center
Created: 2025-10-25
Last Modified: 2026-01-22
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import re

# Third-party imports
import polars as pl
from polars.exceptions import ComputeError, ColumnNotFoundError
from pypinyin import lazy_pinyin

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)

def columns_to_lowercase(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert all column names to lowercase for database compatibility.
    
    Args:
        df: Input Polars DataFrame
        
    Returns:
        DataFrame with all column names converted to lowercase
    """
    return df.rename({col: col.lower() for col in df.columns})

def convert_to_int64(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert column to nullable Int64 with fallback string conversion.
    
    Args:
        df: Input Polars DataFrame
        col: Column name to convert
        
    Returns:
        DataFrame with column converted to Int64
        
    Examples:
        >>> df = pl.DataFrame({'quantity': ['123', '456', None]})
        >>> convert_to_int64(df, 'quantity')
    """
    try:
        # Check if column exists
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to Int64, handling nulls appropriately
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Int64, strict=False)
            .alias(col)
        )

    except (ComputeError, ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to Int64: %s", col, e)

        # Fallback: try to convert via string
        try:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False).alias(col)
            )

        except (ComputeError, ColumnNotFoundError, ValueError) as fallback_error:
            logger.error("Fallback conversion failed for column '%s': %s", col, fallback_error)
        except Exception as unexpected_error:
            logger.error("Unexpected error during fallback conversion for column '%s': %s",
                        col, unexpected_error, exc_info=True)

    except Exception as unexpected_error:
        logger.error("Unexpected error converting column '%s' to Int64: %s",
                    col, unexpected_error, exc_info=True)

    return df


def convert_to_string(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert column to string (Utf8) with null preservation.
    
    Args:
        df: Input Polars DataFrame
        col: Column name to convert
        
    Returns:
        DataFrame with column converted to Utf8
        
    Examples:
        >>> df = pl.DataFrame({'id': [1, 2, 3]})
        >>> convert_to_string(df, 'id')
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to string type (Utf8 in polars)
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .map_elements(
                lambda x: None if (x is None or x == "") else x,
                return_dtype=pl.Utf8
            )
            .alias(col)
        )

    except (ComputeError, ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to string: %s", col, e)
    except Exception as unexpected_error:
        logger.error("Unexpected error converting column '%s' to string: %s",
                    col, unexpected_error, exc_info=True)

    return df


def convert_to_float(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert column to Float64 with 2-decimal rounding.
    
    Args:
        df: Input Polars DataFrame
        col: Column name to convert
        
    Returns:
        DataFrame with column converted to Float64 and rounded to 2 decimals
        
    Examples:
        >>> df = pl.DataFrame({'price': ['19.99', '29.50', None]})
        >>> convert_to_float(df, 'price')
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to Float64 and round to 2 decimal places
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Float64, strict=False)
            .round(2)
            .alias(col)
        )

    except (ComputeError, ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to float: %s", col, e)
    except Exception as unexpected_error:
        logger.error("Unexpected error converting column '%s' to float: %s",
                    col, unexpected_error, exc_info=True)

    return df

def basic_clean_text(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Apply minimal text cleaning with space normalization.
    
    Args:
        df: Input Polars DataFrame
        col: Column name containing text to clean
        
    Returns:
        DataFrame with cleaned text (spaces normalized, trimmed)
        
    Examples:
        >>> df = pl.DataFrame({'name': ['  John   Doe  ', None]})
        >>> basic_clean_text(df, 'name')
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        df = df.with_columns(
            pl.col(col)
            # Normalize multiple spaces to single space
            .str.replace_all(r"\s+", " ")
            # Trim leading/trailing spaces
            .str.strip_chars()
            .map_elements(
                lambda x: None if (x is None or x == "") else x,
                return_dtype=pl.Utf8
            )
            .alias(col)
        )

        logger.debug("Applied basic cleaning to column '%s'", col)

    except (ComputeError, ColumnNotFoundError) as e:
        logger.error("Basic cleaning failed for column '%s': %s", col, e)
    except Exception as unexpected_error:
        logger.error("Unexpected error in basic cleaning for column '%s': %s",
                    col, unexpected_error, exc_info=True)

    return df

def advanced_clean_text(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Apply comprehensive multilingual text cleaning with fallback.
    
    Features:
        - Chinese characters → pinyin
        - CamelCase splitting
        - Punctuation removal
        - Space normalization
        - Title case conversion
    
    Args:
        df: Input Polars DataFrame
        col: Column name containing text to clean
        
    Returns:
        DataFrame with comprehensively cleaned text
        
    Examples:
        >>> df = pl.DataFrame({'text': ['engineMount-123', '汽车零部件', None]})
        >>> advanced_clean_text(df, 'text')
    """

    # Define special characters to remove
    special_chars = re.escape(r"-)(][.,;:_/\|+*&^%$#@!~`\"'<>?{}")

    # Define Chinese character range (basic and extended)
    chinese_pattern = re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]')

    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Helper function for Chinese to pinyin conversion
        def chinese_to_pinyin(name: str) -> str:
            """Convert Chinese characters to pinyin"""
            if name is None:
                return None
            try:
                return ''.join(lazy_pinyin(name))
            except Exception as e:
                logger.debug("Pinyin conversion failed for text '%s': %s", name[:50], e)
                return name

        # Helper function to detect if text contains Chinese characters
        def contains_chinese(text: str) -> bool:
            """Check if text contains Chinese characters"""
            if text is None:
                return False
            try:
                return bool(chinese_pattern.search(text))
            except (TypeError, re.error) as e:
                logger.debug("Chinese detection failed: %s", e)
                return False

        # Apply transformations
        df = df.with_columns(
            pl.col(col)
            # Convert Chinese characters to pinyin if present
            .map_elements(
                lambda x: chinese_to_pinyin(x) if x and contains_chinese(x) else x,
                return_dtype=pl.Utf8
            )
            # Handle lowerCamelCase (e.g., "engineMount" → "engine Mount")
            .str.replace_all(r"([a-z])([A-Z])", r"$1 $2")
            # Handle UpperCamelCase for both Cyrillic and Latin
            .str.replace_all(r"([A-ZА-ЯЁ][^A-ZА-ЯЁ]*)", r" $1")
            # Separate numbers from text (including decimals)
            .str.replace_all(r"(\d+(?:\.\d+)?)", r" $1 ")
            # Remove all special characters
            .str.replace_all(f"[{special_chars}\n\t]", " ")
            # Normalize multiple spaces to single space
            .str.replace_all(r"\s+", " ")
            # Trim leading/trailing spaces
            .str.strip_chars()
            # Apply title case
            .str.to_titlecase()
            .alias(col)
        )

        logger.debug("Successfully cleaned text column '%s'", col)

    except (ComputeError, ColumnNotFoundError, re.error) as e:
        logger.warning("Error cleaning text in column '%s': %s", col, e)

        # Apply basic cleaning as fallback
        logger.info("Applying basic cleaning as fallback for column '%s'", col)
        df = basic_clean_text(df, col)

    except ImportError as e:
        logger.error("PyPinyin module not available for column '%s': %s", col, e)
        # Apply basic cleaning as fallback
        df = basic_clean_text(df, col)

    except Exception as unexpected_error:
        logger.error("Unexpected error in advanced cleaning for column '%s': %s",
                    col, unexpected_error, exc_info=True)
        # Apply basic cleaning as fallback
        df = basic_clean_text(df, col)

    return df
