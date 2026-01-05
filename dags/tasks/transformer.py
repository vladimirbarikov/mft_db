"""
Data Transformation Module for MFT Database Project

This module provides comprehensive data transformation and cleaning functionality
for automotive manufacturing data. It handles type conversions, text normalization,
and data quality improvements for multiple DataFrames containing supplier, part,
packaging, and production data.

Main Components:
- convert_to_int64(): Converts to nullable integer type with None handling
- convert_to_string(): Converts to string type with missing value preservation
- convert_to_float(): Converts to float with 2-decimal rounding
- clean_text_column(): Universal text cleaning with Chinese detection and CamelCase processing
- apply_transformations(): Applies transformation dictionaries to DataFrames
- transformer(): Main function coordinating all data transformations

Key Features:
- Type conversion functions for Int64, string, and float data types
- Automatic Chinese character detection and pinyin conversion
- Multilingual CamelCase text processing (Cyrillic and Latin)
- Punctuation removal and text normalization
- Comprehensive error handling and logging
- Batch processing of multiple DataFrames

Data Sources:
- Processes DataFrames from extractor module including:
  * main_df: Comprehensive part and packaging data
  * supplier_df: Supplier information
  * part_df: Part master data
  * box_df: Box packaging specifications
  * pallet_df: Pallet packaging specifications
  * model_df: Vehicle model data
  * workshop_df: Production workshop data
  * line_df: Production line data

Dependencies:
- polars: Data manipulation and transformation
- pypinyin: Chinese character to pinyin conversion
- re: Regular expressions for text processing
- logging: Application logging and error tracking

Usage:
    from transformer import transformer
    # Get transformed data
    transformed_data = transformer()
    
    # Access individual DataFrames
    main_df = transformed_data['transformed_main_df']
    supplier_df = transformed_data['transformed_supplier_df']

Note: Designed for integration with Apache Airflow DAGs and SQLAlchemy database operations.

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engeneering Center
Created: 2025-10-25
Last Modified: 2026-01-05
License: MIT
Status: Production
"""
from pathlib import Path
import sys
import re
from typing import Callable, Dict
import polars as pl
from pypinyin import lazy_pinyin

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

# Logger setup
from config import get_logger
logger = get_logger(__name__)

# Import function extractor from module extractor.py
from dags.tasks.extractor import extractor


def convert_to_int64(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''
    Function converts column data to nullable Int64 type (polars integer with null support)
    and handles conversion from various numeric and string types.
    
    Arguments:
    - df: polars DataFrame instance to be modified
    - col: Column name to convert to Int64 type
    
    Returns:
    - df: Modified DataFrame with converted column
    
    Notes:
    - Uses pl.Int64 which supports null values
    - Preserves null values for missing data
    - Suitable for integer columns that may contain missing values
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


def convert_to_string(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''
    Function converts column data to string type (Utf8) and preserves null values.
    Safely handles various data types including numeric, boolean, and datetime values.
    
    Arguments:
    - df: polars DataFrame instance to be modified
    - col: Column name to convert to string type
    
    Returns:
    - df: Modified DataFrame with converted column
    
    Notes:
    - Converts non-null values to string using cast to Utf8
    - Preserves null values
    - Suitable for text columns that may contain mixed data types
    '''
    try:
        if col not in df.columns:
            logger.warning("Column '%s' not found in DataFrame", col)
            return df

        # Convert to string type (Utf8 in polars)
        df = df.with_columns(
            pl.col(col).cast(pl.Utf8, strict=False).alias(col)
        )

    except (pl.exceptions.ComputeError, pl.exceptions.ColumnNotFoundError) as e:
        logger.warning("Error converting column '%s' to string: %s", col, e)

    return df


def convert_to_float(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''
    Function converts column data to Float64 type with 2 decimal places rounding
    and preserves null values. Handles numeric conversion safely.
    
    Arguments:
    - df: polars DataFrame instance to be modified
    - col: Column name to convert to float type
    
    Returns:
    - df: Modified DataFrame with converted column
    
    Notes:
    - Rounds numeric values to 2 decimal places using round(2)
    - Preserves null values
    - Correctly handles conversion errors by replacing non-numeric values with Null
    - Suitable for monetary values, measurements, and other decimal data
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
            logger.error("Failed to set column '%s' to float: %s", col, null_error)

    return df


def clean_text_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    '''
    Universal text cleaning function that applies appropriate transformations
    based on content detection.
    
    Processing logic:
    1. Detects if text contains Chinese characters
    2. For Chinese text: converts to pinyin, then applies text cleaning
    3. For non-Chinese text: applies multilingual text cleaning directly
    4. Always removes punctuation, normalizes spaces, and applies title case
    
    Arguments:
    - df: polars DataFrame instance to be modified
    - col: Column name containing text to clean
    
    Returns:
    - df: Modified DataFrame with cleaned text
    
    Notes:
    - Automatically detects Chinese characters in text
    - Preserves null values
    - Handles multilingual text (Cyrillic, Latin, Chinese)

    Raises:
    pl.exceptions.ComputeError: If Polars computation fails
    re.error: If regex pattern compilation fails
    ImportError: If pypinyin import fails
    '''

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
            '''Convert Chinese characters to pinyin'''
            if name is None:
                return None
            try:
                return ''.join(lazy_pinyin(name))
            except Exception:
                return name

        # Helper function to detect if text contains Chinese characters
        def contains_chinese(text: str) -> bool:
            '''Check if text contains Chinese characters'''
            if text is None:
                return False
            return bool(chinese_pattern.search(text))

        # Apply transformations
        df = df.with_columns(
            pl.col(col)
            # Step 1: Convert Chinese characters to pinyin if present
            .map_elements(
                lambda x: chinese_to_pinyin(x) if x and contains_chinese(x) else x,
                return_dtype=pl.Utf8
            )
            # Step 2: Handle lowerCamelCase (e.g., "engineMount" → "engine Mount")
            .str.replace_all(r"([a-z])([A-Z])", r"$1 $2")
            # Step 3: Handle UpperCamelCase for both Cyrillic and Latin
            .str.replace_all(r"([A-ZА-ЯЁ][^A-ZА-ЯЁ]*)", r" $1")
            # Step 4: Separate numbers from text (including decimals)
            .str.replace_all(r"(\d+(?:\.\d+)?)", r" $1 ")
            # Step 5: Remove all special characters
            .str.replace_all(f"[{special_chars}\n\t]", " ")
            # Step 6: Normalize multiple spaces to single space
            .str.replace_all(r"\s+", " ")
            # Step 7: Trim leading/trailing spaces
            .str.strip_chars()
            # Step 8: Apply title case
            .str.to_titlecase()
            .alias(col)
        )

        logger.debug("Successfully cleaned text column '%s'", col)

    except (pl.exceptions.ComputeError, re.error, ImportError) as e:
        logger.warning("Error cleaning text in column '%s': %s", col, e)

        # Apply basic cleaning as fallback
        logger.info("Applying basic cleaning as fallback for column '%s'", col)
        try:
            df = df.with_columns(
                pl.col(col)
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_titlecase()
                .alias(col)
            )

        except Exception as fallback_error:
            logger.error(
                "Basic fallback cleaning also failed for column '%s': %s",
                col, fallback_error
            )

    return df


def apply_transformations(df: pl.DataFrame, transformations: Dict[str, Callable]) -> pl.DataFrame:
    """
    Function applies a set of transformation functions to specified columns in a DataFrame.
    Safely processes each transformation with error handling and continues processing
    other columns even if individual transformations fail.
    
    Arguments:
    - df: Original polars DataFrame instance to be transformed
    - transformations: Dict[str, Callable[[pl.DataFrame, str], pl.DataFrame]]
    - Format: {column_name: transformation_function}
    
    Returns:
    - result_df: New DataFrame instance with applied transformations
    
    Notes:
    - Creates a copy of the original DataFrame to avoid modifying source data
    - Skips columns that are not present in the DataFrame
    - Continues processing other columns if a transformation fails for one column
    - Logs detailed warnings for failed transformations with column names
    - Preserves original column values if transformation fails
    """
    result_df = df.clone()
    successful_transformations = 0
    failed_transformations = 0
    skipped_columns = 0

    for col, func in transformations.items():
        if col not in result_df.columns:
            logger.warning("Column '%s' not found in DataFrame, skipping transformation", col)
            skipped_columns += 1
            continue

        try:
            # Apply the transformation function
            result_df = func(result_df, col)
            successful_transformations += 1
            logger.debug("Successfully applied %s to column '%s'", func.__name__, col)

        except (pl.exceptions.ComputeError, pl.exceptions.ColumnNotFoundError) as e:
            logger.warning(
                "Transformation %s failed for column '%s': %s", 
                func.__name__, col, e
            )
            failed_transformations += 1

        except Exception as e:
            logger.error(
                "Unexpected error during %s transformation of column '%s': %s", 
                func.__name__, col, e
            )
            failed_transformations += 1

    logger.info(
        "Transformations completed: %d successful, %d failed, %d skipped", 
        successful_transformations, failed_transformations, skipped_columns
    )

    return result_df


def transformer():
    '''
    Function applies data transformations to multiple DataFrames including type conversions
    and data cleaning operations. Transforms data into appropriate types such as Int64, 
    Utf8, and float, and performs text cleaning for supplier and part names.
    
    The function processes the following DataFrames:
    - main_df: Primary DataFrame with comprehensive part and packaging data
    - supplier_df: Supplier information data
    - part_df: Part master data
    - box_df: Box packaging specifications
    - pallet_df: Pallet packaging specifications
    - model_df: Vehicle model data
    - workshop_df: Production workshop data
    - line_df: Production line data
    
    Arguments:
    - None: The function internally calls extractor() to obtain source data
    
    Returns:
    - transformed_df_dict: Dictionary containing all transformed DataFrames with keys:
        * 'transformed_main_df': Transformed main DataFrame
        * 'transformed_supplier_df': Transformed supplier DataFrame
        * 'transformed_part_df': Transformed part DataFrame
        * 'transformed_box_df': Transformed box DataFrame
        * 'transformed_pallet_df': Transformed pallet DataFrame
        * 'transformed_model_df': Transformed model DataFrame
        * 'transformed_workshop_df': Transformed workshop DataFrame
        * 'transformed_line_df': Transformed line DataFrame
    
    Transformation types applied:
    - convert_to_int64: Converts to nullable integer type with null for missing values
    - convert_to_string: Converts to string type with null for missing values
    - convert_to_float: Converts to float type with rounding to 2 decimal places
    - clean_text_column: Universal text cleaning with automatic Chinese detection and CamelCase processing
    '''
    try:
        common_df_dict = extractor()
        transformed_df_dict = {}

        # Ensure all DataFrames in common_df_dict are polars DataFrames
        # If extractor returns pandas DataFrames, convert them to polars
        for df_name, df in common_df_dict.items():
            if not isinstance(df, pl.DataFrame):
                try:
                    common_df_dict[df_name] = pl.from_pandas(df)
                    logger.info("Converted '%s' from pandas to polars DataFrame", df_name)
                except Exception as conversion_error:
                    logger.error("Failed to convert '%s' to polars: %s", df_name, conversion_error)
                    raise

        # main_df dataframe transformations definition
        main_df_transformations = {
            'PART_NUMBER': convert_to_string,
            'PART_NAME': clean_text_column,
            'PART_WEIGHT_KG': convert_to_float,
            'PART_PER_VEHICLE': convert_to_int64,
            'CONFIGURATION': convert_to_string,
            'MODEL_CODE': convert_to_string,
            'MODEL_NAME': convert_to_string,
            'LINE_CODE': convert_to_string,
            'LINE_NAME': convert_to_string,
            'WORKSHOP_CODE': convert_to_string,
            'WORKSHOP_NAME': convert_to_string,
            'PART_PER_BOX': convert_to_int64,
            'BOX_NUMBER': convert_to_string,
            'BOX_TYPE': convert_to_string,
            'BOX_WEIGHT_KG': convert_to_float,
            'BOX_LENGTH_MM': convert_to_int64,
            'BOX_WIDTH_MM': convert_to_int64,
            'BOX_HEIGHT_MM': convert_to_int64,
            'BOX_VOL_M3': convert_to_float,
            'BOX_AREA_M2': convert_to_float,
            'BOX_STACKING': convert_to_int64,
            'BOX_PER_PALLET': convert_to_int64,
            'PALLET_NUMBER': convert_to_string,
            'PALLET_TYPE': convert_to_string,
            'PALLET_WEIGHT_KG': convert_to_float,
            'PALLET_LENGTH_MM': convert_to_int64,
            'PALLET_WIDTH_MM': convert_to_int64,
            'PALLET_HEIGHT_MM': convert_to_int64,
            'PALLET_VOL_M3': convert_to_float,
            'PALLET_AREA_M2': convert_to_float,
            'PALLET_STACKING': convert_to_int64,
            'SUPPLIER_NAME': clean_text_column,
            'LOCATION': convert_to_string,
            'CITY': convert_to_string,
            'STREET': convert_to_string,
            'BUILDING': convert_to_string,
            'LOCALIZATION': convert_to_string
        }

        # supplier_df dataframe transformations definition
        supplier_df_transformations = {
            'SUPPLIER_NAME': clean_text_column,
            'LOCATION': convert_to_string,
            'CITY': convert_to_string,
            'STREET': convert_to_string,
            'BUILDING': convert_to_string,
            'LOCALIZATION': convert_to_string
        }

        # part_df dataframe transformations definition
        part_df_transformations = {
            'PART_NUMBER': convert_to_string,
            'PART_NAME': clean_text_column,
            'PART_WEIGHT_KG': convert_to_float
        }

        # box_df dataframe transformations definition
        box_df_transformations = {
            'BOX_NUMBER': convert_to_string,
            'BOX_TYPE': convert_to_string,
            'BOX_WEIGHT_KG': convert_to_float,
            'BOX_LENGTH_MM': convert_to_int64,
            'BOX_WIDTH_MM': convert_to_int64,
            'BOX_HEIGHT_MM': convert_to_int64,
            'BOX_VOL_M3': convert_to_float,
            'BOX_AREA_M2': convert_to_float,
            'BOX_STACKING': convert_to_int64
        }

        # pallet_df dataframe transformations definition
        pallet_df_transformations = {
            'PALLET_NUMBER': convert_to_string,
            'PALLET_TYPE': convert_to_string,
            'PALLET_WEIGHT_KG': convert_to_float,
            'PALLET_LENGTH_MM': convert_to_int64,
            'PALLET_WIDTH_MM': convert_to_int64,
            'PALLET_HEIGHT_MM': convert_to_int64,
            'PALLET_VOL_M3': convert_to_float,
            'PALLET_AREA_M2': convert_to_float,
            'PALLET_STACKING': convert_to_int64
        }

        # model_df dataframe transformations definition
        model_df_transformations = {
            'MODEL_CODE': convert_to_string,
            'MODEL_NAME': convert_to_string
        }

        # workshop_df dataframe transformations definition
        workshop_df_transformations = {
            'WORKSHOP_CODE': convert_to_string,
            'WORKSHOP_NAME': convert_to_string
        }

        # line_df dataframe transformations definition
        line_df_transformations = {
            'LINE_CODE': convert_to_string,
            'LINE_NAME': convert_to_string
        }

        # Process each DataFrame with error handling
        dataframes_to_process = [
            ('main_df', main_df_transformations, 'transformed_main_df'),
            ('supplier_df', supplier_df_transformations, 'transformed_supplier_df'),
            ('part_df', part_df_transformations, 'transformed_part_df'),
            ('box_df', box_df_transformations, 'transformed_box_df'),
            ('pallet_df', pallet_df_transformations, 'transformed_pallet_df'),
            ('model_df', model_df_transformations, 'transformed_model_df'),
            ('workshop_df', workshop_df_transformations, 'transformed_workshop_df'),
            ('line_df', line_df_transformations, 'transformed_line_df')
        ]

        for df_name, transformations, result_key in dataframes_to_process:
            try:
                if df_name not in common_df_dict:
                    logger.warning("DataFrame '%s' not found in common_df_dict", df_name)
                    continue

                df = common_df_dict[df_name]
                if df.is_empty():
                    logger.info("DataFrame '%s' is empty, skipping transformation", df_name)
                    transformed_df_dict[result_key] = df
                    continue

                # Apply transformations to the DataFrame
                transformed_df = apply_transformations(df, transformations)
                transformed_df_dict[result_key] = transformed_df
                logger.info("Transformation of '%s' completed successfully", df_name)

            except (KeyError, AttributeError, TypeError) as e:
                logger.error(
                    "Error processing DataFrame '%s': %s", 
                    df_name, e
                )
                # Store original DataFrame if transformation fails
                transformed_df_dict[result_key] = common_df_dict[df_name]
                logger.warning(
                    "Stored original DataFrame for '%s' due to transformation failure",
                    df_name
                )

            except Exception as e:
                logger.error(
                    "Unexpected error processing DataFrame '%s': %s", 
                    df_name, e
                )
                # Store original DataFrame if transformation fails
                transformed_df_dict[result_key] = common_df_dict[df_name]

        logger.info(
            "All transformations completed. Processed %d DataFrames",
            len(transformed_df_dict)
        )
        return transformed_df_dict

    except (FileNotFoundError, ValueError, pl.exceptions.NoDataError) as e:
        logger.error("Error during data extraction: %s", e)
        raise

    except Exception as e:
        logger.error("Unexpected error in transformer function: %s", e)
        raise

if __name__ == '__main__':
    transformer()
