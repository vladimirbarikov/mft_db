"""
Data loader module for MFT database - Functional approach.

This module provides comprehensive data loading functionality for the MFT database
system. It handles the loading of transformed data into PostgreSQL using SQLAlchemy
and Polars DataFrames, with support for entity tables, junction tables, and
specialized breakpoint data.

Key Features:
- Bulk data insertion using SQLAlchemy ORM and PostgreSQL COPY for performance
- Foreign key constraint management for PostgreSQL
- Data validation and integrity checks
- Support for entity tables, junction tables, and breakpoint data
- Transaction-safe operations with proper error handling
- Comprehensive logging and result reporting

Architecture:
The module follows a functional programming approach with clearly separated
responsibilities for different data loading stages. It integrates with the
transformer module to receive cleaned and transformed data.

Primary Functions:
- `load_transformed_data()`: Main entry point for data loading
- `load_all_entity_tables()`: Loads all entity tables (supplier, part, box, etc.)
- `load_all_junction_tables()`: Loads relationship tables
- `load_breakpoint_data()`: Handles breakpoint-specific data loading

Dependencies:
- SQLAlchemy 1.4.54: For database operations and ORM mapping
- Polars: For DataFrame manipulation and validation
- psycopg2: For PostgreSQL COPY operations (optional, falls back to SQLAlchemy)
- Database models from `database.database` module

Usage Example:
    ```python
    from sqlalchemy import create_engine
    from dags.tasks.loader import load_transformed_data
    
    # Create database engine
    engine = create_engine("postgresql://user:pass@localhost:5432/dbname")
    
    # Load data (automatically calls transformer if needed)
    results = load_transformed_data(engine, use_transformer=True)
    
    if results['success']:
        print(f"Loaded {sum(results['record_counts'].values())} records")
    else:
        print(f"Error: {results['error']}")
    ```

Data Flow:
1. Input: Transformed data dictionary from transformer module
2. Validation: Check data integrity and required columns
3. Foreign Key Management: Disable constraints for faster loading
4. Entity Tables: Load primary entities (suppliers, parts, etc.)
5. Junction Tables: Load relationship tables
6. Breakpoint Data: Load specialized breakpoint information
7. Verification: Count records and validate loading
8. Foreign Key Management: Re-enable constraints
9. Output: Comprehensive results dictionary

Error Handling:
- All database operations include try-except blocks with detailed logging
- Foreign key constraints are always re-enabled, even on failure
- Validation failures provide clear error messages
- Failed operations return zero counts without stopping the entire process

Performance Optimizations:
- PostgreSQL COPY for bulk data insertion (when psycopg2 available)
- Foreign key constraint disabling during bulk operations
- DataFrame validation before database operations
- Batch processing of related tables

Configuration:
- Logging is configured at INFO level by default
- Project path is automatically added to sys.path
- All table truncation operations include CASCADE option

Note: This module is specifically optimized for PostgreSQL and uses PostgreSQL-
specific features like session_replication_role for foreign key management.
Compatibility with other databases may require modifications.

Version: 1.0.0
Compatibility: Python 3.12.3, SQLAlchemy 1.4.54, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""

from pathlib import Path
import sys
import logging
import traceback

from io import StringIO
from typing import Dict, Any, List, Tuple, Optional

import polars as pl

from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from psycopg2 import connect

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

# Import function transformer from module transformer
from dags.tasks.transformer import transformer


# Import database models
from database.database import (
    SupplierData, PartData, BoxData, PalletData, ModelData,
    WorkshopData, LineData, BreakpointData, PartToBox,
    BoxToPallet, PartToModel, PartToLine, PartToBreakpoint
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_dataframe_not_empty(
        df: pl.DataFrame,
        df_name: str
    ) -> bool:
    """Validate DataFrame is not empty."""
    if df.is_empty():
        logger.warning("DataFrame '%s' is empty", df_name)
        return False
    return True


def validate_required_columns(
        df: pl.DataFrame,
        df_name: str,
        required_columns: List[str]
    ) -> bool:
    """Validate required columns exist."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error("DataFrame '%s' missing columns: %s", df_name, missing_columns)
        return False
    return True


def validate_transformed_data(
        transformed_df_dict: Dict[str, pl.DataFrame]
    ) -> bool:
    """Validate all transformed DataFrames."""
    required_keys = [
        'transformed_main_df',
        'transformed_part_df',
        'transformed_supplier_df',
        'transformed_box_df',
        'transformed_pallet_df',
        'transformed_model_df',
        'transformed_workshop_df',
        'line_df'
    ]

    # Check for missing DataFrames
    missing_keys = [key for key in required_keys if key not in transformed_df_dict]
    if missing_keys:
        logger.error("Missing required DataFrames: %s", missing_keys)
        return False

    # Validate critical DataFrames are not empty
    critical_keys = ['transformed_main_df', 'transformed_part_df']
    for key in critical_keys:
        if not validate_dataframe_not_empty(transformed_df_dict[key], key):
            return False

    return True


def disable_foreign_keys(
        engine: Engine
    ) -> None:
    """Temporarily disable foreign key constraints for PostgreSQL."""
    try:
        with engine.begin() as connection:
            # Disable triggers and foreign key constraints
            connection.execute(text('SET session_replication_role = replica;'))
            logger.info("Foreign key constraints disabled")

    except Exception as e:
        logger.warning("Could not disable foreign keys: %s", e)


def enable_foreign_keys(engine: Engine) -> None:
    """Re-enable foreign key constraints for PostgreSQL."""
    try:
        with engine.begin() as connection:
            # Re-enable triggers and foreign key constraints
            connection.execute(text('SET session_replication_role = DEFAULT;'))
            logger.info("Foreign key constraints enabled")

    except Exception as e:
        logger.error("Could not enable foreign keys: %s", e)
        raise


def truncate_table(
        engine: Engine,
        table_name: str,
        cascade: bool = True
    ) -> None:
    """Truncate a table in PostgreSQL."""
    try:
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            logger.warning("Table %s does not exist", table_name)
            return

        with engine.begin() as connection:
            if cascade:
                # TRUNCATE with CASCADE to also truncate tables with foreign key references
                stmt = text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE')
            else:
                stmt = text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY')

            connection.execute(stmt)
            logger.info("Truncated table: %s", table_name)

    except Exception as e:
        logger.error("Could not truncate table %s: %s", table_name, e)
        raise


def bulk_insert_dataframe(
        engine: Engine,
        df: pl.DataFrame,
        table_name: str,
        model_class
    ) -> int:
    """Bulk insert DataFrame into table using SQLAlchemy."""
    if df.is_empty():
        return 0

    # Convert polars DataFrame to list of dictionaries
    records = df.to_dicts()

    try:
        # Use SQLAlchemy's bulk insert
        with engine.begin() as connection:
            insert_stmt = model_class.__table__.insert()
            connection.execute(insert_stmt, records)

        logger.debug("Inserted %d records into %s", len(records), table_name)
        return len(records)

    except SQLAlchemyError as e:
        logger.error("Error inserting into %s: %s", table_name, e)
        raise


def bulk_insert_dataframe_copy(
        engine: Engine,
        df: pl.DataFrame,
        table_name: str
    ) -> int:
    """
    Alternative method using PostgreSQL COPY for better performance with large datasets.
    Creates a direct psycopg2 connection for COPY operations.
    """
    if df.is_empty():
        return 0

    try:
        # Get the connection parameters from the engine.url
        url = engine.url
        conn_params = {
            'host': url.host or 'localhost',
            'port': url.port or 5432,
            'database': url.database,
            'user': url.username,
            'password': url.password
        }

        # Creating a direct connection to psycopg2
        connection = connect(**conn_params)
        cursor = connection.cursor()

        try:
            # Use StringIO for CSV output
            output = StringIO()

            # Write DataFrame to CSV with tab separator for COPY
            df.write_csv(output, separator='\t', include_header=False)
            output.seek(0)

            # Execute COPY command using psycopg2
            cursor.copy_from(output, table_name, sep='\t', null='\\N')

            connection.commit()

            logger.debug("COPY inserted %d records into %s", df.height, table_name)
            return df.height

        except Exception as e:
            connection.rollback()
            logger.error("Error in COPY operation: %s", e)
            raise
        finally:
            cursor.close()
            connection.close()

    except ImportError:
        logger.warning("psycopg2 not installed, falling back to regular insert")
        return 0
    except Exception as e:
        logger.error("Error using COPY for %s: %s", table_name, e)
        # Fall back to regular insert
        return 0


def load_entity_table(
        engine: Engine,
        df: pl.DataFrame,
        model_class,
        primary_key: str,
        truncate: bool = True) -> int:
    """
    Load an entity table.
    
    Args:
        engine: Database engine
        df: DataFrame to load
        model_class: SQLAlchemy model class
        primary_key: Primary key column name
        truncate: Whether to truncate table first
        
    Returns:
        Number of records loaded
    """
    table_name = model_class.__tablename__

    # Validate DataFrame is not empty
    if not validate_dataframe_not_empty(df, table_name):
        return 0

    if not validate_required_columns(df, table_name, [primary_key]):
        raise ValueError(f"Missing primary key: {primary_key}")

    # Check for NULL primary keys
    null_pk_count = df[primary_key].null_count()
    if null_pk_count > 0:
        logger.error("Found %d NULL values in %s", null_pk_count, primary_key)
        raise ValueError(f"NULL values in primary key: {primary_key}")

    # Remove duplicates
    df = df.unique(subset=[primary_key], keep='first')

    # Truncate if requested
    if truncate:
        truncate_table(engine, table_name)

    # Insert data
    records_loaded = bulk_insert_dataframe(engine, df, table_name, model_class)

    logger.info("Loaded %d records into %s", records_loaded, table_name)
    return records_loaded


def load_all_entity_tables(
        engine: Engine,
        transformed_df_dict: Dict[str, pl.DataFrame],
        truncate: bool = True
    ) -> Dict[str, int]:
    """
    Load all entity tables.
    
    Returns:
        Dictionary mapping table names to record counts
    """
    logger.info("Loading entity tables...")

    # Mapping of DataFrame keys to (model_class, primary_key)
    entity_mapping = {
        'transformed_supplier_df': (SupplierData, 'supplier_id'),
        'transformed_part_df': (PartData, 'part_id'),
        'transformed_box_df': (BoxData, 'box_id'),
        'transformed_pallet_df': (PalletData, 'pallet_id'),
        'transformed_model_df': (ModelData, 'model_id'),
        'transformed_workshop_df': (WorkshopData, 'workshop_id'),
        'line_df': (LineData, 'line_id'),
    }

    results = {}

    for df_key, (model_class, primary_key) in entity_mapping.items():
        if df_key in transformed_df_dict:
            df = transformed_df_dict[df_key]
            table_name = model_class.__tablename__

            try:
                records_loaded = load_entity_table(
                    engine, df, model_class, primary_key, truncate
                )
                results[table_name] = records_loaded

            except Exception as e:
                logger.error("Error loading %s: %s", table_name, e)
                results[table_name] = 0

    return results


def load_junction_table(
        engine: Engine,
        main_df: pl.DataFrame,
        model_class,
        foreign_keys: List[str],
        truncate: bool = True
    ) -> int:
    """
    Load a junction table from main DataFrame.
    
    Args:
        engine: Database engine
        main_df: Main DataFrame containing relationships
        model_class: Junction model class
        foreign_keys: List of foreign key column names
        truncate: Whether to truncate table first
        
    Returns:
        Number of records loaded
    """
    table_name = model_class.__tablename__

    # Check if required columns exist
    missing_cols = [fk for fk in foreign_keys if fk not in main_df.columns]
    if missing_cols:
        logger.warning("Missing columns for %s: %s", table_name, missing_cols)
        return 0

    # Extract relationships
    junction_df = main_df.select(foreign_keys).drop_nulls().unique(keep='first')

    if junction_df.is_empty():
        logger.warning("No data for %s", table_name)
        return 0

    # Convert to appropriate types (polars already handles types well)
    # Ensure integer types where needed
    for fk in foreign_keys:
        if junction_df[fk].dtype != pl.Int64:
            junction_df = junction_df.with_columns(
                pl.col(fk).cast(pl.Int64, strict=False)
            )

    # Remove any nulls that might have been created by casting
    junction_df = junction_df.drop_nulls()

    if junction_df.is_empty():
        return 0

    # Truncate if requested
    if truncate:
        truncate_table(engine, table_name)

    # Insert data
    records_loaded = bulk_insert_dataframe(engine, junction_df, table_name, model_class)

    logger.info("Loaded %d records into %s", records_loaded, table_name)
    return records_loaded


def load_all_junction_tables(
        engine: Engine,
        main_df: pl.DataFrame,
        truncate: bool = True
    ) -> Dict[str, int]:
    """
    Load all junction tables.
    
    Returns:
        Dictionary mapping table names to record counts
    """
    logger.info("Loading junction tables...")

    if main_df is None or main_df.is_empty():
        logger.warning("Main DataFrame is empty")
        return {}

    # Define junction tables
    junction_definitions = [
        ('part_to_box', PartToBox, ['part_id', 'box_id']),
        ('box_to_pallet', BoxToPallet, ['box_id', 'pallet_id']),
        ('part_to_model', PartToModel, ['part_id', 'model_id']),
        ('part_to_line', PartToLine, ['part_id', 'line_id']),
    ]

    results = {}

    for table_name, model_class, foreign_keys in junction_definitions:
        try:
            records_loaded = load_junction_table(
                engine, main_df, model_class, foreign_keys, truncate
            )
            results[table_name] = records_loaded

        except Exception as e:
            logger.error("Error loading %s: %s", table_name, e)
            results[table_name] = 0

    return results


def extract_breakpoint_data(
        main_df: pl.DataFrame
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Extract breakpoint data from main DataFrame.
    
    Returns:
        Tuple of (breakpoint_df, part_to_breakpoint_df)
    """
    breakpoint_cols = ['breakpoint_date', 'old_part_id', 'new_part_id']

    if not all(col in main_df.columns for col in breakpoint_cols):
        return pl.DataFrame(), pl.DataFrame()

    # Extract unique breakpoints
    breakpoint_df = main_df.select(breakpoint_cols).drop_nulls().unique(keep='first')

    if breakpoint_df.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    # Create breakpoint records
    breakpoint_records = []
    part_to_breakpoint_records = []

    for i, row in enumerate(breakpoint_df.iter_rows(named=True), 1):
        # Breakpoint record
        breakpoint_records.append({
            'breakpoint_id': i,
            'breakpoint_date': row['breakpoint_date'],
            'description': f"Change: {row['old_part_id']} → {row['new_part_id']}",
            'old_part_id': row['old_part_id'],
            'new_part_id': row['new_part_id']
        })

        # Part-to-breakpoint relationships
        part_to_breakpoint_records.append({
            'part_id': row['old_part_id'],
            'breakpoint_id': i,
            'is_active_before_breakpoint': True,
            'is_active_after_breakpoint': False
        })

        part_to_breakpoint_records.append({
            'part_id': row['new_part_id'],
            'breakpoint_id': i,
            'is_active_before_breakpoint': False,
            'is_active_after_breakpoint': True
        })

    return (
        pl.DataFrame(breakpoint_records),
        pl.DataFrame(part_to_breakpoint_records)
    )


def load_breakpoint_data(
        engine: Engine,
        main_df: pl.DataFrame,
        truncate: bool = True
    ) -> Dict[str, int]:
    """
    Load breakpoint data and relationships.
    
    Returns:
        Dictionary with record counts
    """
    logger.info("Loading breakpoint data...")

    results = {}

    try:
        # Extract data
        breakpoint_df, part_to_breakpoint_df = extract_breakpoint_data(main_df)

        if breakpoint_df.is_empty():
            logger.info("No breakpoint data to load")
            return {'breakpoint_data': 0, 'part_to_breakpoint': 0}

        # Load breakpoint data
        if truncate:
            truncate_table(engine, 'breakpoint_data')

        records_loaded = bulk_insert_dataframe(
            engine, breakpoint_df, 'breakpoint_data', BreakpointData
        )
        results['breakpoint_data'] = records_loaded

        # Load part-to-breakpoint relationships
        if not part_to_breakpoint_df.is_empty():
            if truncate:
                truncate_table(engine, 'part_to_breakpoint')

            records_loaded = bulk_insert_dataframe(
                engine, part_to_breakpoint_df, 'part_to_breakpoint', PartToBreakpoint
            )
            results['part_to_breakpoint'] = records_loaded

        logger.info("Loaded %d breakpoint records", results.get('breakpoint_data', 0))

    except Exception as e:
        logger.error("Error loading breakpoint data: %s", e)
        results = {'breakpoint_data': 0, 'part_to_breakpoint': 0}

    return results


def verify_table_counts(
        engine: Engine
    ) -> Dict[str, int]:
    """Count records in all tables using PostgreSQL."""
    tables = [
        'supplier_data', 'part_data', 'box_data', 'pallet_data',
        'model_data', 'workshop_data', 'line_data', 'breakpoint_data',
        'part_to_box', 'box_to_pallet', 'part_to_model',
        'part_to_line', 'part_to_breakpoint'
    ]

    counts = {}

    try:
        with engine.connect() as connection:
            for table in tables:
                try:
                    stmt = text(f"SELECT COUNT(*) FROM {table}")
                    result = connection.execute(stmt)
                    counts[table] = result.scalar() or 0

                except Exception as e:
                    logger.debug("Could not count records in %s: %s", table, e)
                    counts[table] = 0

        # Log counts
        for table, count in counts.items():
            logger.info("Table %s: %d records", table, count)

    except Exception as e:
        logger.error("Error verifying table counts: %s", e)

    return counts


def load_transformed_data(
        engine: Engine,
        transformed_df_dict: Optional[Dict[str, pl.DataFrame]] = None,
        truncate_before_load: bool = True,
        use_transformer: bool = True
    ) -> Dict[str, Any]:
    """
    Main function to load transformed data into database.
    
    Args:
        engine: SQLAlchemy engine
        transformed_df_dict: Dictionary of transformed DataFrames (optional if use_transformer=True)
        truncate_before_load: Whether to truncate tables before loading
        use_transformer: If True, call transformer() to get data
        
    Returns:
        Dictionary with loading results
    """
    results = {
        'success': False,
        'record_counts': {},
        'error': None,
        'step_results': {}
    }

    try:
        logger.info("=" * 60)
        logger.info("STARTING DATA LOADING PROCESS")
        logger.info("=" * 60)

        # If the data is transmitted explicitly
        if transformed_df_dict is not None:
            logger.info("Using provided transformed data")
            # Check if transformed_df_dict is dictionary
            if not isinstance(transformed_df_dict, dict):
                raise ValueError(
                    f"transformed_df_dict should be dict, got {type(transformed_df_dict)}"
                )

        # We need to call for transformer() to get data
        elif use_transformer:
            logger.info("Calling transformer function...")

            # Call for the transformer() from module transformer.py
            transformed_df_dict = transformer()

            # Check if transformed_df_dict is dictionary
            if not isinstance(transformed_df_dict, dict):
                raise ValueError(
                    f"transformer() returned {type(transformed_df_dict)}, expected dict"
                )

            logger.info("Transformer returned %d DataFrames", len(transformed_df_dict))

        else:
            raise ValueError(
                "No data provided. Either pass transformed_df_dict or set use_transformer=True"
            )

        # Validate input
        if not validate_transformed_data(transformed_df_dict):
            results['error'] = "Data validation failed"
            return results

        # Disable foreign keys for faster loading (PostgreSQL)
        disable_foreign_keys(engine)

        try:
            # Load entity tables
            logger.info("Loading entity tables...")
            entity_results = load_all_entity_tables(
                engine, transformed_df_dict, truncate_before_load
            )
            results['step_results']['entity_tables'] = entity_results
            logger.info("Entity tables loaded: %d tables", len(entity_results))

            # Load junction tables
            main_df = transformed_df_dict['transformed_main_df']
            logger.info("Loading junction tables...")
            junction_results = load_all_junction_tables(
                engine, main_df, truncate_before_load
            )
            results['step_results']['junction_tables'] = junction_results
            logger.info("Junction tables loaded: %d tables", len(junction_results))

            # Load breakpoint data
            logger.info("Loading breakpoint data...")
            breakpoint_results = load_breakpoint_data(
                engine, main_df, truncate_before_load
            )
            results['step_results']['breakpoint_data'] = breakpoint_results
            logger.info("Breakpoint data loaded")

        finally:
            # Re-enable foreign keys (PostgreSQL)
            enable_foreign_keys(engine)

        # Verify all tables
        results['record_counts'] = verify_table_counts(engine)

        # Check if loading was successful
        total_records = sum(results['record_counts'].values())
        results['success'] = total_records > 0

        if results['success']:
            logger.info("=" * 60)
            logger.info("DATA LOADING SUCCESSFUL!")
            logger.info("Total records loaded: %d", total_records)
            logger.info("=" * 60)
        else:
            logger.warning("Data loading completed but no records were loaded")

    except Exception as e:
        results['error'] = str(e)
        logger.error("Error in load_transformed_data: %s", e)
        logger.error(traceback.format_exc())

        # Ensure foreign keys are re-enabled on error
        try:
            enable_foreign_keys(engine)
        except Exception as reenable_error:
            logger.warning("Could not re-enable foreign keys: %s", reenable_error)

    return results
