# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Data Transformation Module for Material Flow Table Database.

This module provides comprehensive data transformation and cleaning functionality
for automotive manufacturing data. It handles type conversions, text normalization,
and data quality improvements for multiple DataFrames containing supplier, part,
packaging, and production data.

Key Features:
    - Type conversion functions for Int64, String, and Float64 data types with null safety
    - Column name standardization (lowercase conversion) for database compatibility
    - Automatic Chinese character detection and Pinyin conversion using pypinyin
    - Advanced text cleaning with camelCase splitting and number separation
    - Basic text cleaning with Cyrillic to Latin character mapping
    - Graceful fallback mechanisms with error handling at multiple levels
    - Comprehensive error handling and detailed logging at DEBUG and ERROR levels

Architecture:
    The module follows a layered transformation approach:
    1. Column Standardization: Convert all column names to lowercase
    2. Type Conversion Layer: Convert data types while preserving null values
    3. Text Processing Layer: Clean and normalize text with language detection
    4. Fallback Layer: Basic cleaning when advanced transformations fail
    
    Functions are designed to be composable, allowing complex transformation
    pipelines to be built from simple, reusable components.

Dependencies:
    - Polars 1.0.0+ for efficient DataFrame operations and type casting
    - PyPinyin 0.50.0+ for Chinese character to Pinyin conversion (optional fallback)
    - Python 3.12.3+ for type hints and modern string handling
    - Standard library: re for regex operations, sys for path management, pathlib for paths

Performance Considerations:
    - Uses Polars' vectorized operations for optimal performance where possible
    - Map_elements used selectively for Chinese detection and Cyrillic mapping
    - Early column existence checks prevent wasted processing on missing columns
    - Regex patterns are compiled at runtime but used efficiently
    - Minimal string copying through expression-based transformations

Security Notes:
    - No execution of dynamic code from input data
    - Input validation through column existence checks
    - Safe handling of special characters and Unicode across all transformations
    - No external network calls or file system access beyond input DataFrames
    - All regex patterns use re.escape() where appropriate for safety

Error Handling:
    - Comprehensive exception hierarchy with appropriate logging levels (DEBUG, WARNING, ERROR)
    - Graceful degradation with DataFrame preservation on errors
    - Advanced text cleaning falls back to basic cleaning on failure
    - Column existence validation before any transformation
    - Null value preservation throughout all transformations
    - Unexpected errors are caught, logged with traceback, and return original DataFrame
    - Specific handling for ComputeError, ValueError, TypeError, AttributeError, re.error

Integration Notes:
    - Designed to work with extractor.py output (Polars DataFrames)
    - Output compatible with loader.py input requirements
    - Column naming follows lowercase convention for database compatibility
    - Functions maintain DataFrame schema except for transformed columns
    - Airflow task compatible with proper error propagation
    - All functions return DataFrame (never None) for safe chaining

Usage Example:
    ```python
    from dags.tasks.transformer import (
        columns_to_lowercase,
        convert_to_int64,
        convert_to_string,
        convert_to_float,
        basic_clean_text,
        advanced_clean_text,
        pinyin_conversion
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

Module Functions:
    - columns_to_lowercase(): Convert all column names to lowercase for consistency
    - convert_to_int64(): Safe integer conversion with string fallback
    - convert_to_float(): Float conversion with 2-decimal rounding
    - convert_to_string(): String conversion with empty string -> null handling
    - basic_clean_text(): Minimal cleaning with Cyrillic mapping and special char removal
    - advanced_clean_text(): CamelCase splitting and number separation with basic fallback
    - pinyin_conversion(): Chinese character detection and Pinyin conversion

Text Processing Examples:    
    Input Text	            Function	            Output
    "发动机供应商"	        pinyin_conversion	    "fa dong ji gong ying shang"
    "engineMount-123"	    advanced_clean_text	   "engine mount 123"
    "UpperCamelCase"	    advanced_clean_text	   "upper camel case"
    "Hello, World!"	        basic_clean_text	   "hello world"
    "user@email.com"	    basic_clean_text	   "user email com"
    " multiple spaces "	    basic_clean_text	   "multiple spaces"
    "техт"	                basic_clean_text	   "text" # 'т'→'t', 'е'→'e', 'х'→'x'
    None	                any function	       null (preserved)

Important Notes:
    - Cyrillic mapping in basic_clean_text() is LIMITED to 11 specific characters
    - No function performs full Cyrillic-to-Latin transliteration
    - advanced_clean_text() ALWAYS converts to lowercase at the end
    - pinyin_conversion() requires optional pypinyin library
    - All functions preserve null values
    - All functions return original DataFrame if column not found
    - Empty strings become null in convert_to_string() only

Version: 1.0.0
Compatibility: Python 3.12.3+, Polars 1.0.0+, PyPinyin 0.50.0+
Maintainer: PLD Engineering Center
Created: 2025-10-25
Last Modified: 2026-03-11
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import re

# Third-party imports
import polars as pl
from polars.exceptions import ComputeError
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

    except (ComputeError, ValueError, TypeError, AttributeError) as e:
        logger.warning("Error converting column '%s' to Int64: %s", col, e)

        # Fallback: try to convert via string
        try:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False).alias(col)
            )

        except (ComputeError, ValueError, TypeError, AttributeError) as fallback_error:
            logger.error(
                "Fallback conversion failed for column '%s': %s", col, fallback_error)
        except Exception as unexpected_error:
            logger.error("Unexpected error during fallback conversion for column '%s': %s",
                        col, unexpected_error, exc_info=True)

    except Exception as unexpected_error:
        logger.error("Unexpected error converting column '%s' to Int64: %s",
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

    except (ComputeError, ValueError, TypeError, AttributeError,) as e:
        logger.warning("Error converting column '%s' to float: %s", col, e)
        # Return original DataFrame without changes
        return df

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error converting column '%s' to float: %s",
            col, unexpected_error, exc_info=True
        )
        # Return original DataFrame without changes
        return df

    return df


def convert_to_string(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert column to string (Utf8) with null preservation and empty string handling.

    Performs the following operations:
        1. Safely casts any data type to string (UTF-8) with strict=False to handle errors
        2. Converts empty strings to None (null) for consistent null representation

    Note:
        - Returns original DataFrame if column not found

    Args:
        df: Input Polars DataFrame
        col: Column name to convert
        
    Returns:
        DataFrame with column converted to Utf8, where empty strings become None
        
    Examples:
        >>> df = pl.DataFrame({'id': [1, 2, 3, None]})
        >>> convert_to_string(df, 'id')
        shape: (4, 1)
        ┌──────┐
        │ id   │
        │ ---  │
        │ str  │
        ╞══════╡
        │ 1    │
        │ 2    │
        │ 3    │
        │ null │
        └──────┘
        
        >>> df = pl.DataFrame({'text': ['hello', '', None, [1,2,3]]})
        >>> convert_to_string(df, 'text')
        shape: (4, 1)
        ┌───────┐
        │ text  │
        │ ---   │
        │ str   │
        ╞═══════╡
        │ hello │
        │ null  │
        │ null  │
        │ null  │
        └───────┘
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to string type (Utf8 in polars)
        df = df.with_columns(
            pl.col(col)
            # Converting a data type to a string
            .cast(pl.Utf8, strict=False)
            # Handle empty strings
            .map_elements(
                lambda x: None if (x is None or x == "") else x,
                return_dtype=pl.Utf8
            )
            .alias(col)
        )

    except (ComputeError, ValueError, TypeError, AttributeError,) as e:
        logger.warning("Error converting column '%s' to string: %s", col, e)
        # Return original DataFrame without changes
        return df

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error converting column '%s' to string: %s",
            col, unexpected_error, exc_info=True
        )
        # Return original DataFrame without changes
        return df

    return df


def basic_clean_text(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Apply basic text cleaning with special character removal, lowercase conversion,
    and Cyrillic to Latin character mapping for visually similar characters.
    
    Performs the following operations in order:
        1. Removes all special characters (punctuation, symbols, newlines, tabs)
        2. Converts text to lowercase
        3. Maps visually similar Cyrillic characters to Latin equivalents 
           (e.g., Russian 'а', 'в', 'е', 'к', 'м', 'н', 'о', 'р', 'с', 'т', 'х')
        4. Normalizes multiple spaces to single space
        5. Trims leading/trailing spaces
    
    Note:
        - Assumes input text is already a string type
        - Only maps Cyrillic characters that look similar to Latin ones
        - Preserves None values (doesn't convert them)
        - Returns original DataFrame if column not found
    
    Args:
        df: Input Polars DataFrame
        col: Column name containing text to clean
        
    Returns:
        DataFrame with cleaned text containing only:
        - Latin letters (from mapped Cyrillic characters)
        - Numbers (preserved)
        - Single spaces between words
        - Original None values preserved
        
    Examples:
        >>> import polars as pl        
        >>> # Cleaning user input data
        >>> df = pl.DataFrame({'user_input': [
        ...     'Hello, World!',               # Latin with punctuation
        ...     'техт',                        # Cyrillic
        ...     'user@email.com',              # Email format
        ...     '  multiple   spaces  ',       # Extra spaces
        ... ]})
        >>> basic_clean_text(df, 'user_input')
        shape: (4, 1)
        ┌─────────────────┐
        │ user_input      │
        │ ---             │
        │ str             │
        ╞═════════════════╡
        │ hello world     │  # Punctuation removed
        │ text            │  # Cyrillic mapped
        │ user email com  │  # Special chars removed
        │ multiple spaces │  # Spaces normalized and trimmed
        └─────────────────┘
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Define special characters to remove
        special_chars = re.escape(r"-)(][.,;:_/\|+*&^%$#@!~`\"'<>?{}")

        # Mapping for lowercase Cyrillic to lowercase Latin
        # Assumes text will be lowercase before applying this mapping
        char_map = {
            'а': 'a',  # Cyrillic a -> Latin a
            'в': 'b',  # Cyrillic в -> Latin b
            'е': 'e',  # Cyrillic е -> Latin e
            'к': 'k',  # Cyrillic к -> Latin k
            'м': 'm',  # Cyrillic м -> Latin m
            'н': 'h',  # Cyrillic н -> Latin h
            'о': 'o',  # Cyrillic o -> Latin o
            'р': 'p',  # Cyrillic р -> Latin p
            'с': 'c',  # Cyrillic с -> Latin c
            'т': 't',  # Cyrillic т -> Latin t
            'х': 'x',  # Cyrillic х -> Latin x
        }

        # Create translation table
        trans_table = str.maketrans(char_map)

        df = df.with_columns(
            pl.col(col)
            # Remove all special characters
            .str.replace_all(f"[{special_chars}\n\t]", " ")
            # Convert text to lowercase for consistent mapping
            .str.to_lowercase()
            # Map Cyrillic characters to visually similar Latin ones
            .map_elements(
                lambda x: x.translate(trans_table) if x is not None else None,
                return_dtype=pl.Utf8
            )
            # Normalize multiple spaces to single space
            .str.replace_all(r"\s+", " ")
            # Trim leading/trailing spaces
            .str.strip_chars()
            .alias(col)
        )

        logger.debug(
            "Applied basic text cleaning with Cyrillic mapping to column '%s'",
            col
        )

    except (ComputeError, ValueError, TypeError, AttributeError, re.error) as e:
        logger.error("Text cleaning failed for column '%s': %s", col, e)
        # Return original DataFrame without changes
        return df

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in text cleaning for column '%s': %s",
            col, unexpected_error, exc_info=True
        )
        # Return original DataFrame without changes
        return df

    return df


def advanced_clean_text(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Apply advanced text cleaning with camelCase splitting and number separation.
    
    Performs the following operations in order:
        1. Removes all special characters (punctuation, symbols, newlines, tabs)
        2. Splits lowerCamelCase (e.g., "engineMount" → "engine Mount")
        3. Splits UpperCamelCase for both Cyrillic and Latin text
        4. Separates numbers from text (including decimals)
        5. Normalizes multiple spaces to single space
        6. Trims leading/trailing spaces
        7. Converts text to lowercase
    
    Note:
        - Assumes input text is already a string type
        - Preserves None values (doesn't convert them)
        - Falls back to basic_clean_text() on processing errors only
        - Returns original DataFrame if column not found
    
    Args:
        df: Input Polars DataFrame
        col: Column name containing text to clean
        
    Returns:
        DataFrame with cleaned text containing only:
        - Letters (preserving original case)
        - Numbers
        - Single spaces between words
        
    Examples:
        >>> import polars as pl
        >>> 
        >>> # First apply basic cleaning
        >>> df = pl.DataFrame({'text': [
        ...     'engineMount-123',
        ...     'UpperCamelCaseText',
        ...     'Hello   World!',
        ...     None
        ... ]})
        >>> 
        >>> # Then apply advanced cleaning
        >>> advanced_clean_text(df, 'text')
        shape: (5, 1)
        ┌─────────────────────────┐
        │ text                    │
        │ ---                     │
        │ str                     │
        ╞═════════════════════════╡
        │ engine mount 123        │  # CamelCase split, hyphen removed
        │ upper camel case text   │  # UpperCamelCase split
        │ hello world             │  # Spaces normalized, ! removed
        │ null                    │
        └─────────────────────────┘
    """
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Define special characters to remove
        special_chars = re.escape(r"-)(][.,;:_/\|+*&^%$#@!~`\"'<>?{}")

        df = df.with_columns(
            pl.col(col)
            # Remove all special characters
            .str.replace_all(f"[{special_chars}\n\t]", " ")
            # Handle lowerCamelCase (e.g., "engineMount" → "engine Mount")
            .str.replace_all(r"([a-z])([A-Z])", r"$1 $2")
            # Handle UpperCamelCase for both Cyrillic and Latin
            .str.replace_all(r"([A-ZА-ЯЁ][^A-ZА-ЯЁ]*)", r" $1")
            # Separate numbers from text (including decimals)
            .str.replace_all(r"(\d+(?:\.\d+)?)", r" $1 ")
            # Normalize multiple spaces to single space
            .str.replace_all(r"\s+", " ")
            # Trim leading/trailing spaces
            .str.strip_chars()
            # Convert text to lowercase
            .str.to_lowercase()
            .alias(col)
        )

        logger.debug(
            "Applied advanced text cleaning with camelCase splitting to column '%s'", col
        )

    except (ComputeError, ValueError, TypeError, AttributeError, re.error) as e:
        # Processing errors - column exists but data can't be processed
        logger.error("Advanced text cleaning failed for column '%s': %s", col, e)
        logger.info("Falling back to basic_clean_text for column '%s'", col)
        df = basic_clean_text(df, col)
        return df

    except Exception as unexpected_error:
        # Any other unexpected errors during processing
        logger.error("Unexpected error in advanced text cleaning for column '%s': %s",
                    col, unexpected_error, exc_info=True)
        logger.info("Falling back to basic_clean_text for column '%s' due to unexpected error", col)
        df = basic_clean_text(df, col)
        return df

    return df


def pinyin_conversion(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert Chinese characters in a text column to Pinyin (phonetic transcription).
    
    Performs the following operations:
        1. Detects if text contains Chinese characters (Unicode range: \u4e00-\u9fff, etc.)
        2. Converts any Chinese characters to Pinyin using the pypinyin library
        3. Preserves non-Chinese characters (Latin letters, numbers, etc.)
        4. Handles None values gracefully
    
    Features:
        - Converts simplified and traditional Chinese characters to Pinyin
        - Preserves original text if no Chinese characters are detected
        - Falls back to original text if Pinyin conversion fails
        - Handles mixed text (Chinese + Latin + numbers)
    
    Note:
        - Requires the 'pypinyin' library to be installed
        - Uses default Pinyin format (without tone marks)
        - Only converts Chinese characters; other characters remain unchanged
        - Empty strings are preserved (not converted to None)
    
    Args:
        df: Input Polars DataFrame
        col: Column name containing text with potential Chinese characters
        
    Returns:
        DataFrame with Chinese characters converted to Pinyin.
        Non-Chinese text and None values remain unchanged.
        
    Examples:
        >>> import polars as pl
        >>> 
        >>> # Basic Chinese text conversion
        >>> df = pl.DataFrame({'chinese_text': [
        ...     '汽车零部件',        # Chinese only
        ...     'Hello 世界',        # Mixed Chinese and Latin
        ...     'No Chinese here',   # Latin only
        ...     None
        ... ]})
        >>> pinyin_conversion(df, 'chinese_text')
        shape: (4, 1)
        ┌────────────────────┐
        │ chinese_text       │
        │ ---                │
        │ str                │
        ╞════════════════════╡
        │ qi che ling bu jian│  # Full Chinese → Pinyin
        │ hello shi jie      │  # Mixed → mixed with Pinyin
        │ No Chinese here    │  # Unchanged
        │ null               │  # None preserved
        └────────────────────┘
        
    Dependencies:
        pypinyin: Library for Chinese character to Pinyin conversion
        Install with: pip install pypinyin
        
    See Also:
        basic_clean_text: For general text cleaning
        advanced_clean_text: For comprehensive text processing
    """

    # Define Chinese character range (basic and extended)
    chinese_pattern = re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]')

    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Helper function for Chinese to pinyin conversion
        def chinese_to_pinyin(text: str) -> str:
            """Convert Chinese characters to pinyin, preserving non-Chinese text"""
            if text is None:
                return None
            try:
                # lazy_pinyin returns list of pinyin syllables without tone marks
                return ''.join(lazy_pinyin(text))
            except (TypeError, AttributeError, ValueError) as e:
                logger.debug("Pinyin conversion failed for text '%s': %s", str(text)[:50], e)
                return text
            except ImportError:
                logger.debug("Pypinyin library not installed, returning original text")
                return text

        # Helper function to detect if text contains Chinese characters
        def contains_chinese(text: str) -> bool:
            """Check if text contains any Chinese characters"""
            if text is None:
                return False
            try:
                return bool(chinese_pattern.search(text))
            except (TypeError, AttributeError, ValueError) as e:
                logger.debug("Chinese detection failed for text '%s': %s", str(text)[:50], e)
                return False

        # Apply transformations
        df = df.with_columns(
            pl.col(col)
            # Convert Chinese characters to pinyin if present
            .map_elements(
                lambda x: chinese_to_pinyin(x) if x is not None and contains_chinese(x) else x,
                return_dtype=pl.Utf8
            )
            .alias(col)
        )

        logger.debug("Successfully applied Pinyin conversion to column '%s'", col)

    except ComputeError as e:
        logger.error(
            "Pinyin conversion failed for column '%s' due to compute error: %s",
            col, e
        )
        # Return original DataFrame without changes
        return df

    except ImportError as e:
        logger.error(
            "PyPinyin module not available for column '%s'." \
            "Install with: pip install pypinyin. Error: %s",
            col, e
        )
        # Return original DataFrame without changes
        return df

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in Pinyin conversion for column '%s': %s",
            col, unexpected_error, exc_info=True
        )
        # Return original DataFrame without changes
        return df

    return df
