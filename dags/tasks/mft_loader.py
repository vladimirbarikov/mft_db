# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Data Loading Module for Material Flow Table Database.

This module provides comprehensive functionality for bulk loading transformed
manufacturing data into a PostgreSQL database. Refactored to eliminate code
duplication with mapper.py and fix transactional issues.

Key Changes:
    - Removed _resolve_core_entity_foreign_keys() - using mapper directly
    - Split core entity loading into two phases with explicit commit
    - Simplified _prepare_junction_dataframes() - removed duplicate validations
    - Fixed transactional issues with mapper creation

Dependencies:
    - SQLAlchemy for database operations
    - Polars for DataFrame handling
    - mapper.py for ID resolution and mapping
    - config.columns_config for table requirements validation

Maintainer: PLD Engineering Center
Version: 1.0.0
Compatibility: Python 3.12.3+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Created: 2026-01-12
Last Modified: 2025-03-13
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
from typing import Any, Optional
import traceback

# Third-party imports
import polars as pl
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError, ProgrammingError

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from config.columns_config import MFT_TABLE_REQUIREMENTS
from dags.tasks.connector import initialize_database
from dags.tasks.mft_mapper import create_mft_mapper
from database.database import (
    # Entity tables
    SupplierData, PartData, BoxData, PalletData,
    ModelData, ConfigurationData, WorkshopData, LineData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel, PartToLine
)

# Logger setup
logger = get_logger(__name__)


def disable_foreign_keys(engine: Engine) -> None:
    """
    Temporarily disable foreign key constraints for PostgreSQL bulk operations.
    
    Args:
        engine: SQLAlchemy database engine instance
        
    Sets session_replication_role to 'replica' to bypass FK checks during bulk load.
    """
    try:
        with engine.begin() as connection:
            connection.execute(text('SET session_replication_role = replica;'))
            logger.info("Foreign key constraints disabled.")

    except SQLAlchemyError as e:
        logger.warning("Could not disable foreign keys due to SQLAlchemy error: %s", e)
    except Exception as unexpected_error:
        logger.warning("Unexpected error while disabling foreign keys: %s", unexpected_error)


def enable_foreign_keys(engine: Engine) -> None:
    """
    Re-enable foreign key constraints after bulk loading operations.
    
    Args:
        engine: SQLAlchemy database engine instance
        
    Restores default session_replication_role to re-enable FK validation.
    Raises exception if re-enable fails to ensure data integrity.
    """
    try:
        with engine.begin() as connection:
            connection.execute(text('SET session_replication_role = DEFAULT;'))
            logger.info("Foreign key constraints enabled.")

    except SQLAlchemyError as e:
        logger.error("Could not enable foreign keys due to SQLAlchemy error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error while enabling foreign keys: %s", e)
        raise


def _bulk_insert_dataframe(
    engine: Engine,
    df: pl.DataFrame,
    table_name: str,
    model_class,
    required_columns_check: bool = True,
    conflict_columns: Optional[list[str]] = None,
    constraint_name: Optional[str] = None
) -> int:
    """
    Internal function for bulk insertion of Polars DataFrame data.
    
    Args:
        engine: SQLAlchemy database engine
        df: Polars DataFrame with data to insert
        table_name: Name of target table for logging
        model_class: SQLAlchemy model class for the table
        required_columns_check: Whether to validate required columns
        conflict_columns: List of columns to check for conflicts (for ON CONFLICT DO NOTHING)
        constraint_name: Explicit constraint name to use (alternative to conflict_columns)
        
    Returns:
        Number of records successfully inserted
        
    Performs upsert with conflict handling on primary key constraint.
    - If conflict_columns provided, uses ON CONFLICT (columns) DO NOTHING
    - If constraint_name provided, uses ON CONFLICT ON CONSTRAINT constraint_name DO NOTHING
    - Otherwise falls back to constraint detection
    """
    if df is None or df.is_empty():
        logger.debug("Skipping %s - no data.", table_name)
        return 0

    # Checking required columns for core entity tables only
    if required_columns_check:
        if table_name in MFT_TABLE_REQUIREMENTS:
            required_cols = MFT_TABLE_REQUIREMENTS[table_name]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                logger.error(
                    "Cannot load %s: missing required columns: %s. Available: %s.",
                    table_name, missing_cols, list(df.columns)
                )
                return 0

            # Validate foreign key relationships
            if table_name == 'part_data':
                null_supplier_count = df.filter(pl.col('supplier_id').is_null()).height
                if null_supplier_count > 0:
                    logger.error(
                        "Cannot load %s: found %d records with NULL supplier_id.",
                        table_name, null_supplier_count
                    )
                    return 0

            elif table_name == 'line_data':
                null_workshop_count = df.filter(pl.col('workshop_id').is_null()).height
                if null_workshop_count > 0:
                    logger.error(
                        "Cannot load %s: found %d records with NULL workshop_id.",
                        table_name, null_workshop_count
                    )
                    return 0

    try:
        records = df.to_dicts()

        # Constraint name mapping for junction tables
        junction_table_constraints = {
            'part_to_box': 'part_to_box_pkey',
            'box_to_pallet': 'box_to_pallet_pkey',
            'part_to_model': 'part_to_model_pkey',
            'part_to_line': 'part_to_line_pkey'
        }

        # Mapping for unique constraints core entity tables
        core_table_unique_constraints = {
            'supplier_data': ['supplier_name'],
            'part_data': ['part_number'],
            'model_data': ['model_code'],
            'configuration_data': ['configuration'],
            'workshop_data': ['workshop_code'],
            'line_data': ['line_code'],
            # For box_data and pallet_data, we use a composite constraint
            'box_data': ['box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm'],
            'pallet_data': ['pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm']
        }

        # constraint_name = junction_table_constraints.get(table_name, f"{table_name}_pkey")

        with engine.begin() as connection:
            stmt = pg_insert(model_class.__table__).values(records)
            # stmt = stmt.on_conflict_do_nothing(constraint=constraint_name)

            # Defining a way to handle conflicts
            if conflict_columns is not None:
                # We use the specified columns for conflict
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
                logger.debug(
                    "Using conflict columns for %s: %s",
                    table_name, conflict_columns
                )

            elif constraint_name is not None:
                # Using the specified constraint
                stmt = stmt.on_conflict_do_nothing(constraint=constraint_name)
                logger.debug(
                    "Using constraint for %s: %s",
                    table_name, constraint_name
                )

            elif table_name in core_table_unique_constraints:
                # For core entity tables, we use their unique constraints
                conflict_cols = core_table_unique_constraints[table_name]
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                logger.debug(
                    "Using auto-detected unique columns for %s: %s",
                    table_name, conflict_cols
                )

            else:
                # For the rest of the tables, we use the primary key constraint
                constraint_name = junction_table_constraints.get(table_name, f"{table_name}_pkey")
                stmt = stmt.on_conflict_do_nothing(constraint=constraint_name)
                logger.debug(
                    "Using primary key constraint for %s: %s",
                    table_name, constraint_name
                )

            result = connection.execute(stmt)
            inserted_count = result.rowcount

            # Logging results
            total_records = len(records)
            if inserted_count < total_records:
                skipped = total_records - inserted_count
                logger.info(
                    "Inserted %d/%d records into %s (%d duplicates skipped based on unique constraints).",
                    inserted_count, total_records, table_name, skipped
                )
            else:
                logger.info(
                    "Loaded %d records into %s",
                    inserted_count, table_name
                )

            return inserted_count

    except (IntegrityError, DataError, ProgrammingError) as e:
        logger.error("Database error loading %s: %s", table_name, e)

        # Check if it's a constraint error that we can fallback
        error_str = str(e)
        if "constraint" in error_str.lower() and "does not exist" in error_str.lower():
            logger.warning(
                "Constraint not found for %s, retrying with regular insert",
                table_name
            )
            try:
                with engine.begin() as connection:
                    insert_stmt = model_class.__table__.insert()
                    result = connection.execute(insert_stmt, records)
                    logger.info(
                        "Loaded %d records into %s (regular insert)",
                        len(records), table_name
                    )
                    return len(records)

            except (IntegrityError, DataError, ProgrammingError) as e2:
                logger.error("Regular insert failed for %s: %s", table_name, e2)

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error during regular insert for %s: %s",
                    table_name, unexpected_error
                )

        return 0

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error loading %s: %s", table_name, e)
        return 0

    except Exception as unexpected_error:
        logger.error("Unexpected error loading %s: %s", table_name, unexpected_error)
        logger.debug(traceback.format_exc())
        return 0


def _process_dependent_entity_with_mapper(
    engine: Engine,
    df: pl.DataFrame,
    table_name: str,
    mapper,
    unique_cols,
    return_df: bool = False
) -> Optional[pl.DataFrame] | int:
    """
    Process and load dependent entity tables using mapper for foreign key resolution.
    
    Args:
        engine: SQLAlchemy database engine
        df: Input DataFrame with text references
        table_name: Name of target table ('line_data' or 'part_data')
        mapper: Mapper instance for ID resolution
        unique_cols: Column(s) defining uniqueness for deduplication
        return_df: If True, returns processed DataFrame instead of loading to DB
        
    Returns:
        If return_df=True: Processed Polars DataFrame or None
        If return_df=False: Number of records successfully loaded
        
    Converts text codes to IDs using mapper, filters invalid references,
    removes duplicates, and loads the processed data.
    """
    if df.is_empty():
        return 0

    result_df = df.clone()
    null_count = 0

    try:
        if table_name == 'line_data':
            # Convert workshop_code to workshop_id using mapper
            if 'workshop_code' in result_df.columns:
                workshop_ids = []
                for workshop_code in result_df['workshop_code'].to_list():
                    workshop_id = mapper.get_id('workshop_code', workshop_code)
                    workshop_ids.append(workshop_id)
                    if workshop_id is None:
                        null_count += 1

                result_df = result_df.with_columns(pl.Series('workshop_id', workshop_ids))
                result_df = result_df.drop('workshop_code')

        elif table_name == 'part_data':
            # Convert supplier_name to supplier_id using mapper
            if 'supplier_name' in result_df.columns:
                supplier_ids = []
                for supplier_name in result_df['supplier_name'].to_list():
                    supplier_id = mapper.get_id('supplier_name', supplier_name)
                    supplier_ids.append(supplier_id)
                    if supplier_id is None:
                        null_count += 1

                result_df = result_df.with_columns(pl.Series('supplier_id', supplier_ids))
                result_df = result_df.drop('supplier_name')

        # Log warnings about null foreign keys
        if null_count > 0:
            logger.warning(
                "Found %d records with invalid references in %s (will be filtered out).",
                null_count, table_name
            )
            # Filter out records with null foreign keys
            result_df = result_df.filter(
                pl.col('workshop_id' if table_name == 'line_data' else 'supplier_id').is_not_null()
            )

        # Remove duplicates
        if unique_cols:
            if isinstance(unique_cols, str):
                unique_cols_list = [unique_cols]
            else:
                unique_cols_list = unique_cols

            missing_cols = [
                col for col in unique_cols_list if col not in result_df.columns
            ]
            if not missing_cols:
                initial_count = result_df.height
                result_df = result_df.unique(subset=unique_cols_list, keep='first')
                removed_count = initial_count - result_df.height
                if removed_count > 0:
                    logger.debug(
                        "Removed %d duplicates from %s based on unique columns %s",
                        removed_count, table_name, unique_cols_list
                    )

        # Return DataFrame if requested
        if return_df:
            return result_df

        # Otherwise load to database
        model_class = LineData if table_name == 'line_data' else PartData
        return _bulk_insert_dataframe(
            engine, result_df, table_name, model_class, required_columns_check=True
        )

    except (AttributeError, KeyError, ValueError) as e:
        logger.error(
            "Data processing error for %s with mapper: %s",
            table_name, e
        )
        logger.debug(traceback.format_exc())
        return 0

    except (IntegrityError, DataError, ProgrammingError) as e:
        logger.error(
            "Database error loading %s after mapper processing: %s",
            table_name, e
        )
        logger.debug(traceback.format_exc())
        return 0

    except SQLAlchemyError as e:
        logger.error(
            "SQLAlchemy error for %s: %s",
            table_name, e
        )
        logger.debug(traceback.format_exc())
        return 0

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error processing %s with mapper: %s",
            table_name, unexpected_error
        )
        logger.debug(traceback.format_exc())
        return 0


def load_core_entity_tables(
    transformed_data: dict[str, pl.DataFrame],
    engine: Optional[Engine] = None,
    resolve_foreign_keys: bool = True
) -> dict[str, int]:
    """
    Load core entity tables into the database from transformed data.
    
    Args:
        transformed_data: Dictionary mapping table keys to Polars DataFrames
        engine: Optional SQLAlchemy engine (creates new if not provided)
        resolve_foreign_keys: Whether to process dependent tables with FK resolution
        
    Returns:
        Dictionary mapping table names to number of records loaded
        
    Performs two-phase loading:
    1. Independent tables (no foreign keys)
    2. Commit transaction
    3. Create mapper
    4. Dependent tables using mapper for FK resolution
    """
    logger.info("Starting core entity tables loading...")

    # Initialize database if engine not provided
    if engine is None:
        engine = initialize_database(create_tables=True)
        if not engine:
            logger.error("Failed to initialize database!")
            return {}

    results = {}

    try:
        # Load independent tables (no foreign keys)
        logger.info("Loading independent tables...")

        independent_tables = [
            (
                'transformed_supplier_df',
                SupplierData,
                'supplier_data',
                'supplier_name'
            ),
            (
                'transformed_box_df',
                BoxData,
                'box_data',
                ['box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm']
            ),
            (
                'transformed_pallet_df',
                PalletData,
                'pallet_data',
                ['pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm']
            ),
            (
                'transformed_model_df',
                ModelData,
                'model_data',
                'model_code'
            ),
            (
                'transformed_configuration_df',
                ConfigurationData,
                'configuration_data',
                'configuration'
            ),
            (
                'transformed_workshop_df',
                WorkshopData,
                'workshop_data',
                'workshop_code'
            ),
        ]

        # Mapping for unique constraints (to transfer into _bulk_insert_dataframe)
        conflict_columns_map = {
            'supplier_data': ['supplier_name'],
            'box_data': ['box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm'],
            'pallet_data': ['pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm'],
            'model_data': ['model_code'],
            'configuration_data': ['configuration'],
            'workshop_data': ['workshop_code']
        }

        disable_foreign_keys(engine)

        for df_key, model_class, table_name, unique_cols in independent_tables:
            if df_key not in transformed_data:
                logger.warning("Missing DataFrame: %s", df_key)
                results[table_name] = 0
                continue

            df = transformed_data[df_key]
            if df is None or df.is_empty():
                logger.info("Skipping %s - DataFrame is empty", table_name)
                results[table_name] = 0
                continue

            # Remove duplicates
            if unique_cols:
                if isinstance(unique_cols, str):
                    unique_cols_list = [unique_cols]
                else:
                    unique_cols_list = unique_cols

                missing_cols = [col for col in unique_cols_list if col not in df.columns]
                if not missing_cols:
                    initial_count = df.height
                    df = df.unique(subset=unique_cols_list, keep='first')
                    removed_count = initial_count - df.height
                    if removed_count > 0:
                        logger.debug("Removed %d duplicates from %s", removed_count, table_name)

            # Load data with conflict columns for unique constraints
            records_loaded = _bulk_insert_dataframe(
                engine, df, table_name, model_class,
                required_columns_check=True,
                conflict_columns=conflict_columns_map.get(table_name)
            )
            results[table_name] = records_loaded

        # Commit loading independent tables so mapper can see the data
        logger.info("Committing loading independent tables transactions...")
        with engine.begin() as connection:
            connection.execute(text("COMMIT"))

        enable_foreign_keys(engine)

        # Load dependent tables (with foreign keys) using mapper
        if resolve_foreign_keys:
            logger.info("Loading dependent tables with foreign keys...")

            # Create mapper AFTER loading independent tables commit
            mapper = None
            try:
                mapper = create_mft_mapper(engine)
                logger.info("Mapper created successfully for foreign key resolution.")

            except (SQLAlchemyError, ProgrammingError) as e:
                logger.error("Database error creating mapper: %s", e)
                logger.warning("Skipping dependent tables due to mapper creation failure!")
                return results

            except Exception as unexpected_error:
                logger.error("Unexpected error creating mapper: %s", unexpected_error)
                logger.warning("Skipping dependent tables due to mapper creation failure!")
                return results

            dependent_tables = [
                (
                    'transformed_line_df',
                    'line_data',
                    'line_code'
                ),
                (
                    'transformed_part_df',
                    'part_data',
                    'part_number'
                ),
            ]

            conflict_columns_dependent = {
                'line_data': ['line_code'],
                'part_data': ['part_number']
            }

            disable_foreign_keys(engine)

            for df_key, table_name, unique_cols in dependent_tables:
                if df_key not in transformed_data:
                    logger.warning("Missing DataFrame: %s", df_key)
                    results[table_name] = 0
                    continue

                df = transformed_data[df_key]
                if df is None or df.is_empty():
                    logger.info("Skipping %s - DataFrame is empty", table_name)
                    results[table_name] = 0
                    continue

                # Process and load using mapper for FK resolution
                result = _process_dependent_entity_with_mapper(
                    engine, df, table_name, mapper, unique_cols, return_df=True
                )

                if isinstance(result, pl.DataFrame):
                    processed_df = result
                    if not processed_df.is_empty():
                        model_class = LineData if table_name == 'line_data' else PartData
                        records_loaded = _bulk_insert_dataframe(
                            engine, processed_df, table_name, model_class,
                            required_columns_check=True,
                            conflict_columns=conflict_columns_dependent.get(table_name)
                        )
                        results[table_name] = records_loaded
                    else:
                        logger.info(
                            "Processed DataFrame for %s is empty",
                            table_name
                        )
                        results[table_name] = 0
                else:
                    # If int is returned (something went wrong)
                    logger.error(
                        "Expected DataFrame but got %s for %s",
                        type(result), table_name
                    )
                    results[table_name] = 0

            enable_foreign_keys(engine)

            # Clear mapper cache
            if mapper:
                mapper.clear_cache()
                logger.debug("Cleared mapper cache")

    except (IntegrityError, DataError, ProgrammingError) as e:
        logger.error("Database error loading entity tables: %s", e)
        logger.debug(traceback.format_exc())

        # Always re-enable foreign keys on error
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error:
            logger.error("Unexpected error re-enabling foreign keys: %s", unexpected_error)

        return {}

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error loading entity tables: %s", e)
        logger.debug(traceback.format_exc())

        # Always re-enable foreign keys on error
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error:
            logger.error("Unexpected error re-enabling foreign keys: %s", unexpected_error)

        return {}

    except Exception as unexpected_error:
        logger.error("Unexpected error loading entity tables: %s", unexpected_error)
        logger.debug(traceback.format_exc())

        # Always re-enable foreign keys on error
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error2:
            logger.error("Unexpected error re-enabling foreign keys: %s", unexpected_error2)

        return {}

    # Calculate and log totals
    total_records = sum(results.values())
    tables_with_data = [k for k, v in results.items() if v > 0]

    logger.info(
        "Core entity tables loading completed.\n"
        "Total records loaded: %d.\n"
        "Tables with data: %s",
        total_records,
        ', '.join(tables_with_data) or 'none'
    )

    return results


def _prepare_junction_dataframes(
    junction_dict: dict[str, pl.DataFrame],
    mapper
) -> dict[str, dict[str, Any]]:
    """
    Prepare junction table DataFrames by replacing text values with foreign key IDs.
    
    Args:
        junction_dict: Dictionary mapping junction types to DataFrames
        mapper: Mapper instance for ID resolution
        
    Returns:
        Dictionary with processed DataFrames, model classes, and junction types
        
    Relies on mapper.map_junction_records() for all validation and processing,
    eliminating duplicate validation logic.
    """
    mapped_data = {}

    # Mapping between DataFrame keys and mapper junction types
    junction_type_mapping = {
        'part_to_box': 'part_to_box_composite',
        'box_to_pallet': 'box_to_pallet_composite',
        'part_to_model': 'part_to_model',
        'part_to_line': 'part_to_line'
    }

    # Model class mapping
    model_class_map = {
        'part_to_box_composite': PartToBox,
        'box_to_pallet_composite': BoxToPallet,
        'part_to_model': PartToModel,
        'part_to_line': PartToLine
    }

    # Deduplication configuration
    deduplication_config = {
        'part_to_box_composite': ['part_id', 'box_id'],
        'box_to_pallet_composite': ['part_id', 'box_id', 'pallet_id'],
        'part_to_model': ['part_id', 'model_id', 'configuration_id'],
        'part_to_line': ['part_id', 'line_id']
    }

    for df_key in ['part_to_box', 'box_to_pallet', 'part_to_model', 'part_to_line']:
        if df_key not in junction_dict:
            logger.debug("Missing junction DataFrame: %s", df_key)
            continue

        df = junction_dict[df_key]
        if df is None or df.is_empty():
            logger.debug("Empty junction DataFrame: %s", df_key)
            continue

        junction_type = junction_type_mapping.get(df_key)
        if not junction_type:
            logger.error("No junction type mapping for: %s", df_key)
            continue

        model_class = model_class_map.get(junction_type)
        if not model_class:
            logger.error("No model class for junction type: %s", junction_type)
            continue

        try:
            logger.debug("Mapping junction records for '%s'", df_key)

            # Delegate ALL processing to mapper
            mapped_records = mapper.map_junction_records(df, junction_type)

            if not mapped_records:
                logger.debug("No valid mapped records for: %s", junction_type)
                continue

            # Convert to DataFrame
            df_mapped = pl.DataFrame(mapped_records)

            if df_mapped.is_empty():
                logger.debug("Empty DataFrame after mapping for: %s", junction_type)
                continue

            # Remove duplicates using mapper's configuration
            unique_cols = deduplication_config.get(junction_type)
            if unique_cols:
                missing_cols = [col for col in unique_cols if col not in df_mapped.columns]
                if not missing_cols:
                    initial_count = df_mapped.height
                    df_mapped = df_mapped.unique(subset=unique_cols, keep='first')
                    removed_count = initial_count - df_mapped.height
                    if removed_count > 0:
                        logger.info(
                            "Removed %d duplicate relationships for %s",
                            removed_count, df_key
                        )

            # Store processed data
            mapped_data[df_key] = {
                'df': df_mapped,
                'model_class': model_class,
                'junction_type': junction_type
            }

            logger.debug("Mapped %d records for %s", df_mapped.height, junction_type)

        except (AttributeError, KeyError, ValueError) as e:
            logger.error(
                "Data processing error mapping junction records for %s: %s",
                df_key, e
            )
            logger.debug(traceback.format_exc())
            continue

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping junction records for %s: %s",
                df_key, unexpected_error
            )
            logger.debug(traceback.format_exc())
            continue

    return mapped_data


def load_junction_tables(
    junction_dict: dict[str, pl.DataFrame],
    engine: Optional[Engine] = None,
    preserve_cache: bool = False
) -> dict[str, int]:
    """
    Load junction tables (many-to-many relationships) into the database.
    
    Args:
        junction_dict: Dictionary mapping junction types to DataFrames
        engine: Optional SQLAlchemy engine (creates new if not provided)
        preserve_cache: Whether to keep mapper cache after loading
        
    Returns:
        Dictionary mapping table names to number of records loaded
        
    Validates core entity tables exist, creates mapper, pre-loads mappings,
    processes junction data through mapper, and performs bulk insert.
    """
    logger.info("Starting junction tables loading...")

    # Initialize database if engine not provided
    if engine is None:
        engine = initialize_database(create_tables=False)
        if not engine:
            logger.error("Failed to initialize database!")
            return {}

    # Verify core entity tables are loaded
    try:
        with engine.begin() as conn:
            critical_tables = {
                'part_data': "SELECT COUNT(*) FROM part_data",
                'box_data': "SELECT COUNT(*) FROM box_data",
                'model_data': "SELECT COUNT(*) FROM model_data",
                'line_data': "SELECT COUNT(*) FROM line_data"
            }

            missing_tables = []
            for table_name, query in critical_tables.items():
                try:
                    count = conn.execute(text(query)).scalar()
                    if count == 0:
                        missing_tables.append(table_name)
                        logger.warning("Table %s is empty (0 records)", table_name)
                    else:
                        logger.debug("Table %s has %d records", table_name, count)

                except (SQLAlchemyError, ProgrammingError) as e:
                    missing_tables.append(table_name)
                    logger.debug("Error checking table %s: %s", table_name, e)

                except Exception as unexpected_error:
                    missing_tables.append(table_name)
                    logger.debug(
                        "Unexpected error checking table %s: %s",
                        table_name, unexpected_error
                    )

            if missing_tables:
                logger.error(
                    "Cannot load junction tables: missing or empty entity tables: %s\n"
                    "Please load core entity tables first.",
                    missing_tables
                )
                return {}

    except (SQLAlchemyError, ProgrammingError) as check_error:
        logger.error(
            "Database error checking entity table status: %s",
            check_error
        )
        return {}

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error checking entity table status: %s",
            unexpected_error
        )
        return {}

    # Create mapper
    try:
        mapper = create_mft_mapper(engine)
        if mapper is None:
            logger.error("Mapper creation returned None!")
            return {}

    except (SQLAlchemyError, ProgrammingError) as e:
        logger.error("Database error creating mapper: %s", e)
        return {}

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error creating mapper: %s",
            unexpected_error
        )
        return {}

    # Pre-load all mappings for performance
    try:
        logger.info("Pre-loading all ID mappings...")

        mappings_to_load = [
            ('suppliers', mapper.get_supplier_mapping),
            ('parts', mapper.get_part_mapping),
            ('boxes', mapper.get_box_mapping),
            ('pallets', mapper.get_pallet_mapping),
            ('models', mapper.get_model_mapping),
            ('configurations', mapper.get_configuration_mapping),
            ('workshops', mapper.get_workshop_mapping),
            ('lines', mapper.get_line_mapping)
        ]

        empty_mappings = []
        for name, load_func in mappings_to_load:
            try:
                mapping = load_func()
                if not mapping:
                    empty_mappings.append(name)
                    logger.warning("Empty mapping for %s", name)
                else:
                    logger.debug("Mapping '%s' has %d entries", name, len(mapping))

            except (SQLAlchemyError, ProgrammingError) as e:
                logger.error(
                    "Database error loading mapping for %s: %s",
                    name, e
                )
                empty_mappings.append(name)

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error loading mapping for %s: %s",
                    name, unexpected_error
                )
                empty_mappings.append(name)

        if empty_mappings:
            logger.warning(
                "Some mappings are empty: %s. Junction records referencing these will fail.",
                empty_mappings
            )

        mapper.log_mapping_statistics()

    except Exception as unexpected_error:
        logger.error("Failed to load mappings: %s", unexpected_error)
        if mapper:
            mapper.clear_cache()
        return {}

    results = {}

    try:
        # Disable foreign keys for bulk loading
        disable_foreign_keys(engine)

        # Prepare data using mapper (simplified)
        mapped_data = _prepare_junction_dataframes(junction_dict, mapper)

        if not mapped_data:
            logger.info("No junction data to load")
            enable_foreign_keys(engine)
            return {}

        # Load sequence
        load_sequence = ['part_to_box', 'box_to_pallet', 'part_to_model', 'part_to_line']

        for df_key in load_sequence:
            if df_key not in mapped_data:
                results[df_key] = 0
                continue

            data = mapped_data[df_key]
            model_class = data['model_class']
            df = data['df']
            table_name = model_class.__tablename__

            records_loaded = _bulk_insert_dataframe(
                engine, df, table_name, model_class,
                required_columns_check=False  # Mapper already validated
            )
            results[table_name] = records_loaded

        # Re-enable foreign keys
        enable_foreign_keys(engine)

    except (IntegrityError, DataError, ProgrammingError) as e:
        logger.error("Database error loading junction tables: %s", e)
        logger.debug(traceback.format_exc())

        # Always try to re-enable foreign keys
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error re-enabling foreign keys: %s",
                unexpected_error
            )

        return {}

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error loading junction tables: %s", e)
        logger.debug(traceback.format_exc())

        # Always try to re-enable foreign keys
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error re-enabling foreign keys: %s",
                unexpected_error
            )

        return {}

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error loading junction tables: %s",
            unexpected_error
        )
        logger.debug(traceback.format_exc())

        # Always try to re-enable foreign keys
        try:
            logger.warning("Attempting to re-enable foreign keys after error...")
            enable_foreign_keys(engine)

        except (SQLAlchemyError, ProgrammingError) as fk_error:
            logger.error("Failed to re-enable foreign keys: %s", fk_error)

        except Exception as unexpected_error2:
            logger.error(
                "Unexpected error re-enabling foreign keys: %s",
                unexpected_error2
            )

        return {}

    finally:
        # Cache management
        if mapper:
            if preserve_cache:
                logger.debug("Preserving mapper cache as requested")
            else:
                mapper.clear_cache()
                logger.debug("Cleared mapper cache")

    # Calculate and log totals
    total_records = sum(results.values())
    tables_with_data = [k for k, v in results.items() if v > 0]

    logger.info(
        "Junction tables loading completed.\n"
        "Total records loaded: %d.\n"
        "Tables with data: %s",
        total_records,
        ', '.join(tables_with_data) or 'none'
    )

    return results
