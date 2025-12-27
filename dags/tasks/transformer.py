"""
Data Transformation Module for MFT Database Project

This module provides comprehensive data transformation and cleaning functionality
for automotive manufacturing data. It handles type conversions, text normalization,
and data quality improvements for multiple DataFrames containing supplier, part,
packaging, and production data.

Version: 1.0.0
Compatibility: Python 3.12.3,
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""
from pathlib import Path
import sys
import re
import logging
from typing import Callable, Dict, TypeAlias
import polars as pl
from pypinyin import lazy_pinyin

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

# Import function extractor from module extractor.py
from dags.tasks.extractor import extractor

# Logger setup
logger = logging.getLogger(__name__)

# Setting the minimum level
logger.setLevel(logging.DEBUG)

# Disable transmission to parent loggers
# Prevent Airflow from intercepting and filtering
logger.propagate = False

# Add a handler
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    # Add formatter
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)-8s] %(message)s '
        '(%(name)s:%(lineno)d)',
        datefmt='%H:%M:%S'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

# Define a type for conversion functions
TransformationFunc: TypeAlias = Callable[[pl.DataFrame, str], pl.DataFrame]

def convert_to_int64(
        df: pl.DataFrame,
        col: str
    ) -> pl.DataFrame:
    '''
    Function converts column data to nullable Int64 type (polars integer with null support)
    and handles conversion from various numeric and string types.
    '''
    try:
        # Check if column exists
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to Int64, handling nulls appropriately
        df = df.with_columns(
            pl.col(col).cast(pl.Int64, strict=False).alias(col)
        )

    except (pl.exceptions.ComputeError, pl.exceptions.ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to Int64: %s", col, e)

        # Fallback: try to convert via string
        try:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False).alias(col)
            )

        except Exception as fallback_error:
            logger.error("Fallback conversion also failed for column '%s': %s", col, fallback_error)

    return df


def convert_to_string(
        df: pl.DataFrame,
        col: str
    ) -> pl.DataFrame:
    '''
    Function converts column data to string type (Utf8) and preserves null values.
    Safely handles various data types including numeric, boolean, and datetime values.
    '''
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        df = df.with_columns(
            pl.col(col).cast(pl.Utf8, strict=False).alias(col)
        )

    except (pl.exceptions.ComputeError, pl.exceptions.ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to string: %s", col, e)

    return df


def convert_to_float(
        df: pl.DataFrame,
        col: str
    ) -> pl.DataFrame:
    '''
    Function converts column data to Float64 type with 2 decimal places rounding
    and preserves null values. Handles numeric conversion safely.
    '''
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to Float64 and round to 2 decimal places
        df = df.with_columns(
            pl.col(col).cast(pl.Float64, strict=False).round(2).alias(col)
        )

    except (pl.exceptions.ComputeError, pl.exceptions.ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to float: %s", col, e)

        # If conversion fails, set entire column to null
        try:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

        except Exception as null_error:
            logger.error("Failed to set column '%s' to null: %s", col, null_error)

    return df


def clean_text_generic(
        df: pl.DataFrame,
        col: str,
        camelcase_pattern: str = r"([A-Z][^A-Z]*)"
    ) -> pl.DataFrame:
    '''
    Generic text cleaning function for different languages.
    '''
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        df = df.with_columns(
            pl.col(col)
            .str.replace_all(camelcase_pattern, r" $1")
            .str.replace_all(r"[-)(.,\n]", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .str.to_titlecase()
            .alias(col)
        )

    except (pl.exceptions.ComputeError, re.error) as e:
        logger.warning("Error cleaning text in column '%s': %s", col, e)

    return df


def clean_supplier_name(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''Optimized version using map_elements for better performance.'''
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found", col)
            return df

        # Pre-compile regex patterns
        camelcase_pattern = re.compile(r"([A-Z][^A-Z]*)")
        punctuation_pattern = re.compile(r"[-)(.,\n]")
        spaces_pattern = re.compile(r"\s+")

        # Batch processing function
        def process_batch(names: pl.Series) -> pl.Series:
            results = []
            for name in names:
                if not name:
                    results.append(None)
                    continue
                try:
                    # Chinese to pinyin
                    pinyin = ''.join(lazy_pinyin(name))
                    # Clean text
                    cleaned = camelcase_pattern.sub(r" $1", pinyin)
                    cleaned = punctuation_pattern.sub(" ", cleaned)
                    cleaned = spaces_pattern.sub(" ", cleaned)
                    results.append(cleaned.strip().title())

                except Exception:
                    # Keep original if error
                    results.append(name)
            return pl.Series(results)

        # Apply in one batch operation
        df = df.with_columns(
            pl.col(col)
            .map_batches(process_batch, return_dtype=pl.Utf8)
            .alias(col)
        )

    except Exception as e:
        logger.warning("Error in supplier cleaning: %s. Using generic.", str(e)[:100])
        df = clean_text_generic(df, col)

    return df


def clean_part_name(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''
    Enhanced part name cleaning with multi-language support.
    Handles Russian, English, and mixed camelCase.
    '''
    special_chars = re.escape(r"-)(][.,;:_/\|+*&^%$#@!~`\"'<>?{}")

    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found", col)
            return df

        # Regular expressions for different languages
        df = df.with_columns(
            pl.col(col)
            # lowerCamelCase for English (small+uppercase)
            .str.replace_all(r"([a-z])([A-Z])", r"$1 $2")
            # UpperCamelCase for Russian
            .str.replace_all(r"([А-ЯЁ][^А-ЯЁ]*)", r" $1")
            # UpperCamelCase for English
            .str.replace_all(r"([A-Z][^A-Z]*)", r" $1")
            # Numbers (including decimals)
            .str.replace_all(r"(\d+(?:\.\d+)?)", r" $1")
            # All special characters → spaces
            .str.replace_all(f"[{special_chars}\n\t]", " ")
            # Multiple spaces → single space
            .str.replace_all(r"\s+", " ")
            # Remove spaces at the edges
            .str.strip_chars()
            # Capital letters at the beginning of each word
            .str.to_titlecase()
            .alias(col)
        )

        return df

    except Exception as e:
        logger.warning("Error in enhanced part name cleaning: %s", e)
        # Fallback to the basic version
        return clean_text_generic(df, col)


# Define column patterns and their corresponding transformation functions
COLUMN_PATTERNS: Dict[str, TransformationFunc] = {
    # Weights and measurements
    '_KG$': convert_to_float,
    '_MM$': convert_to_int64,
    '_M3$': convert_to_float,
    '_M2$': convert_to_float,

    # Quantities
    'STACKING$': convert_to_int64,
    '^PART_PER_': convert_to_int64,
    '^BOX_PER_': convert_to_int64,

    # Codes and types
    '_NUMBER$': convert_to_string,
    '_CODE$': convert_to_string,
    '_TYPE$': convert_to_string,

    # Common string fields
    'CONFIGURATION$': convert_to_string,
    'LOCATION$': convert_to_string,
    'CITY$': convert_to_string,
    'STREET$': convert_to_string,
    'BUILDING$': convert_to_string,
}

SPECIAL_COLUMNS: Dict[str, TransformationFunc] = {
    # Names with special treatment
    'SUPPLIER_NAME': clean_supplier_name,
    'PART_NAME': clean_part_name,

    # Other names
    'MODEL_NAME': convert_to_string,
    'LINE_NAME': convert_to_string,
    'WORKSHOP_NAME': convert_to_string,

    # ENUM поля
    'LOCALIZATION': convert_to_string
}

# Define re constants
SPECIAL_CHARS_REGEX = re.compile(r"[-)(][.,;:_/\|+*&^%$#@!~`\"'<>?{}]")
RUSSIAN_CAMELCASE_REGEX = re.compile(r"([А-ЯЁ][^А-ЯЁ]*)")
ENGLISH_CAMELCASE_REGEX = re.compile(r"([A-Z][^A-Z]*)")
LOWER_CAMELCASE_REGEX = re.compile(r"([a-z])([A-Z])")
NUMBERS_REGEX = re.compile(r"(\d+(?:\.\d+)?)")

def determine_transformation(column_name: str) -> TransformationFunc:
    '''
    Determine the appropriate transformation function based on column name.
    '''
    column_upper = column_name.upper()

    # Check for special columns first
    if column_upper in SPECIAL_COLUMNS:
        return SPECIAL_COLUMNS[column_upper]

    # Check for pattern matches
    for pattern, func in COLUMN_PATTERNS.items():
        if re.search(pattern, column_upper):
            logger.debug(
                "Column '%s' matched pattern '%s' -> %s",
                column_name, pattern, func.__name__
            )
            return func

    # Default to string conversion for unrecognized columns
    logger.debug("No specific transformation found for '%s', defaulting to string", column_name)
    return convert_to_string


def create_transformations(df: pl.DataFrame) -> Dict[str, TransformationFunc]:
    '''
    Create transformation dictionary for a DataFrame based on column names.
    '''
    transformations: Dict[str, TransformationFunc] = {}
    for col in df.columns:
        transformations[col] = determine_transformation(col)
    return transformations


def apply_transformations(df: pl.DataFrame, transformations: Dict[str, TransformationFunc]) -> pl.DataFrame:
    """
    Function applies a set of transformation functions to specified columns in a DataFrame.
    Safely processes each transformation with error handling and continues processing
    other columns even if individual transformations fail.
    """
    result_df = df.clone()
    stats = {'success': 0, 'failed': 0, 'skipped': 0}

    for col, func in transformations.items():
        if col not in result_df.columns:
            logger.warning("Column '%s' not found, skipping", col)
            stats['skipped'] += 1
            continue

        try:
            result_df = func(result_df, col)
            stats['success'] += 1
            logger.info("Applied %s to column '%s'", func.__name__, col)

        except Exception as e:
            logger.warning(
                "Transformation %s failed for column '%s': %s",
                func.__name__, col, str(e)[:100]
            )
            stats['failed'] += 1

    logger.info(
        "Transformations: %d successful, %d failed, %d skipped", 
        stats['success'], stats['failed'], stats['skipped']
    )

    return result_df


def validate_dataframe(df: pl.DataFrame, df_name: str) -> bool:
    '''
    Basic validation of DataFrame structure and content.
    '''
    if df.is_empty():
        logger.info("DataFrame '%s' is empty", df_name)
        return False

    if len(df.columns) == 0:
        logger.warning("DataFrame '%s' has no columns", df_name)
        return False

    logger.info(
        "DataFrame '%s': %d rows, %d columns",
        df_name, df.height, len(df.columns)
    )

    return True


def transformer() -> Dict[str, pl.DataFrame]:
    '''
    Function applies data transformations to multiple DataFrames including type conversions
    and data cleaning operations. Transforms data into appropriate types such as Int64, 
    str, and float, and performs text cleaning for supplier and part names.
    '''
    try:
        # Extract data
        logger.info("Starting data extraction...")
        common_df_dict = extractor()

        # Ensure all DataFrames are polars DataFrames
        for df_name, df in common_df_dict.items():
            if not isinstance(df, pl.DataFrame):
                try:
                    common_df_dict[df_name] = pl.from_pandas(df)
                    logger.info("Converted '%s' to polars DataFrame", df_name)
                except Exception as e:
                    logger.error("Failed to convert '%s': %s", df_name, e)
                    raise

        transformed_dict: Dict[str, pl.DataFrame] = {}

        # Process each DataFrame
        for df_name, df in common_df_dict.items():
            logger.info("Processing DataFrame: %s", df_name)

            # Validate DataFrame
            if not validate_dataframe(df, df_name):
                transformed_dict[f'transformed_{df_name}'] = df
                continue

            try:
                # Create transformations dynamically
                transformations = create_transformations(df)

                # Apply transformations
                transformed_df = apply_transformations(df, transformations)

                # Store result with consistent naming
                result_key = f'transformed_{df_name}'
                transformed_dict[result_key] = transformed_df

                logger.info("Successfully transformed '%s'", df_name)

            except Exception as e:
                logger.error("Error processing '%s': %s", df_name, e)
                # Store original DataFrame on failure
                result_key = f'transformed_{df_name}'
                transformed_dict[result_key] = df
                logger.warning("Stored original DataFrame for '%s' due to failure", df_name)

        # Final summary
        logger.info("=" * 50)
        logger.info("TRANSFORMATION COMPLETE")
        logger.info("=" * 50)
        for key, df in transformed_dict.items():
            logger.info("%s: %d rows, %d columns", key, df.height, len(df.columns))

        return transformed_dict

    except Exception as e:
        logger.error("Unexpected error in transformer: %s", e)
        raise


if __name__ == '__main__':
    transformer()
