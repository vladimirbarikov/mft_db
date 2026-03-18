# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Breakpoint Object Mapping Module for Material Flow Table Database.

This module provides comprehensive functionality for mapping external object
identifiers to database primary keys specifically for breakpoint change tracking.
It serves as a bridge between transformed breakpoint data (which contains text
references) and the database (which uses UUIDs), enabling referential integrity
during historical data loading.

Key Features:
    - Text-to-ID mapping for all breakpoint-related entities (parts, breakpoints,
      models, suppliers, lines)
    - Support for composite primary key (part_id, breakpoint_id, model_id)
    - Memory-efficient caching of ID mappings
    - Before-change value snapshots preserved as text (no ID mapping)
    - Integration with columns_config.py for column definitions

Architecture:
    The module follows the same caching-first approach as MFTObjectMapper:
    1. On-demand loading of ID mappings from database
    2. In-memory caching for high-performance lookups
    3. Automatic cache management with manual clearing capability
    
    IMPORTANT: This module assumes data has already been validated during the
    transformation phase (including ENUM validation). It only performs ID lookups,
    not data validation.

Configuration Source:
    This module uses constants from columns_config.py:
        - BP_JUNCTION_REQUIRED: Required columns for breakpoint junction
        - BP_JUNCTION_OPTIONAL: Optional columns for breakpoint junction
        - BP_LOOKUP_TABLES: Tables needed for ID lookups

Dependencies:
    - SQLAlchemy 1.4.54+ for ORM and database abstraction
    - Polars for DataFrame operations (junction table mapping)
    - PostgreSQL 12+ as the source database for ID lookups

Database Models:
    The mapper interacts with the following tables from database.py:
    
    Core Entity Tables (for ID lookups):
        - SupplierData: supplier_name → supplier_id
        - PartData: part_number → part_id
        - ModelData: model_code → model_id
        - LineData: line_code → line_id
        - BreakpointData: breakpoint_number → breakpoint_id
    
    Junction Table (target for mapping):
        - PartToBreakpoint: Tracks engineering changes with composite key
          (part_id, breakpoint_id, model_id) and before-change snapshots

Column Mappings:
    The mapper uses the following column-to-model mappings:

    | Column Name     | Source Table    | Lookup Field      | Target ID     |
    |-----------------|-----------------|-------------------|---------------|
    | part_number     | PartData        | part_number       | part_id       |
    | breakpoint_number| BreakpointData | breakpoint_number | breakpoint_id |
    | model_code      | ModelData       | model_code        | model_id      |
    | supplier_name   | SupplierData    | supplier_name     | supplier_id   |
    | line_code       | LineData        | line_code         | line_id       |

Performance Considerations:
    - Lazy loading: Mappings are loaded on first use
    - Memory caching: Reduces database queries for repeated lookups
    - Bulk loading: Pre-load methods for performance-critical operations
    - Cache statistics: Built-in logging for performance monitoring

Security Notes:
    - Read-only operations only (no data modification)
    - Input validation for required fields
    - No SQL injection risk (uses ORM with parameterized queries)

Usage Example:
    ```
    from dags.tasks.bp_mapper import create_breakpoint_mapper

    # Create mapper (after entity tables are loaded):
    mapper = create_breakpoint_mapper()

    # Pre-load all mappings for bulk operations:
    mapper.pre_load_all_mappings()

    # Map breakpoint junction records:
    records = mapper.map_breakpoint_records(df, 'part_to_breakpoint')

    # Check mapping statistics:
    mapper.log_mapping_statistics()

    # Clear cache when done:
    mapper.clear_cache()
    ```

Integration with ETL Pipeline:
    EXTRACT → TRANSFORM → MAP → LOAD
                            ↑
                        bp_mapper.py
                        (ID lookups only)
    - EXTRACT: Raw data from Excel
    - TRANSFORM: Data cleaning and ENUM validation (enum_validator.py)
    - MAP: Text-to-ID conversion (this module)
    - LOAD: Database insertion (bp_loader.py)

Error Handling:
- Missing required fields: Returns None with debug logging
- Failed ID lookups: Returns None with warning logging
- Database errors: Caught and logged, empty mapping returned
- Unexpected errors: Caught, logged, and handled gracefully

Integration Notes:
- Must be created AFTER core entity tables are loaded (depends on their IDs)
- Used by bp_loader.py for historical change processing
- Cache should be cleared after bulk operations to free memory
- Designed for read-heavy, write-light scenarios

Version: 1.0.0
Compatibility: Python 3.12.3+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-03-18
Last Modified: 2026-03-18
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
from typing import Any, Optional

# Third-party imports
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from config.columns_config import (
    BP_JUNCTION_REQUIRED,
    BP_JUNCTION_OPTIONAL,
    BP_LOOKUP_TABLES
)
from dags.tasks.connector import initialize_database
from database.database import (
    SupplierData, PartData, ModelData, LineData, BreakpointData
)

# Logger setup
logger = get_logger(__name__)


class BreakpointObjectMapper:
    """
    Main mapper class for converting breakpoint external identifiers to database primary keys.
    
    [docstring остается без изменений]
    """

    # Configuration with table names (following MFTObjectMapper pattern)
    COLUMN_TO_MODEL = {
        # Supplier mappings
        'supplier_name': (SupplierData, 'supplier_name', 'supplier_id'),
        'supplier_id': (SupplierData, 'supplier_id', 'supplier_id'),

        # Part mappings
        'part_number': (PartData, 'part_number', 'part_id'),
        'part_id': (PartData, 'part_id', 'part_id'),

        # Model mappings
        'model_code': (ModelData, 'model_code', 'model_id'),
        'model_id': (ModelData, 'model_id', 'model_id'),

        # Line mappings
        'line_code': (LineData, 'line_code', 'line_id'),
        'line_id': (LineData, 'line_id', 'line_id'),

        # Breakpoint mappings
        'breakpoint_number': (BreakpointData, 'breakpoint_number', 'breakpoint_id'),
        'breakpoint_id': (BreakpointData, 'breakpoint_id', 'breakpoint_id'),
    }

    def __init__(
            self,
            session: Session
        ):
        """
        Initialize breakpoint mapper with database session.
        
        Args:
            session: SQLAlchemy Session for database operations
        
        Example:
            >>> session_factory = sessionmaker(bind=engine)
            >>> mapper = BreakpointObjectMapper(session_factory())
        """
        self.session = session
        self._cached_mappings = {}
        logger.debug("BreakpointObjectMapper initialized with change tracking support")

    def get_id(
            self,
            column_name: str,
            value: Any
        ) -> Optional[str]:
        """
        Get database ID for given column value.
        
        Handles standard text-to-ID lookups for all breakpoint-related entities.
        
        Args:
            column_name: Column to look up ('part_number', 'model_code', etc.)
            value: Value to look up (string)
                
        Returns:
            Database ID as string or None if not found
            
        Examples:
            >>> mapper.get_id('part_number', 'ABC-123')
            >>> mapper.get_id('model_code', 'a01')
            >>> mapper.get_id('breakpoint_number', 'BP-2025-001')
        """
        if not value or (isinstance(value, str) and value.strip() == ''):
            return None

        # Standard lookup for non-composite columns
        if column_name not in self.COLUMN_TO_MODEL:
            logger.warning(
                "Unknown column for mapping: %s",
                column_name
            )
            return None

        model_class, lookup_column, id_column = self.COLUMN_TO_MODEL[column_name]

        # Get or create mapping
        cache_key = f"{model_class.__name__}_{lookup_column}"
        if cache_key not in self._cached_mappings:
            self._load_mapping(model_class, lookup_column, id_column, cache_key)

        mapping = self._cached_mappings[cache_key]

        # Clean the value for lookup
        clean_value = str(value).strip()
        result = mapping.get(clean_value)

        if result:
            logger.debug(
                "Found ID for %s='%s': %s",
                column_name, clean_value, result
            )
        else:
            logger.debug(
                "No ID found for %s='%s'",
                column_name, clean_value
            )

        return result

    def _load_mapping(
            self,
            model_class,
            lookup_column: str,
            id_column: str,
            cache_key: str
        ):
        """
        Load mapping from database into cache.
        
        Args:
            model_class: SQLAlchemy model class
            lookup_column: Column name for lookups
            id_column: Column name containing the ID
            cache_key: Key to store the mapping in cache
        """
        logger.debug(
            "Loading mapping for %s.%s -> %s",
            model_class.__name__,
            lookup_column,
            id_column
        )

        try:
            # Query all records
            query = self.session.query(
                getattr(model_class, lookup_column),
                getattr(model_class, id_column)
            ).all()

            # Create mapping dictionary
            mapping = {}
            for lookup_val, db_id in query:
                if lookup_val is not None:
                    mapping[str(lookup_val).strip()] = db_id

            self._cached_mappings[cache_key] = mapping

            # Logging information about uploaded mappings
            total_count = len(mapping)
            logger.info("Loaded mapping %s: %d entries",
                cache_key,
                total_count
            )

            # Debug sample
            if mapping:
                sample_size = min(5, total_count)
                sample_items = list(mapping.items())[:sample_size]
                logger.debug(
                    "Sample values (first %d): %s",
                    sample_size,
                    sample_items
                )

        except SQLAlchemyError as e:
            logger.error(
                "Database error loading mapping for %s: %s",
                cache_key, e
            )
            self._cached_mappings[cache_key] = {}

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Data error loading mapping for %s: %s",
                cache_key, e
            )
            self._cached_mappings[cache_key] = {}

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error loading mapping for %s: %s",
                cache_key, unexpected_error
            )
            self._cached_mappings[cache_key] = {}

    # ========== SPECIALIZED METHODS FOR BREAKPOINT JUNCTION TABLE ==========

    def _map_part_to_breakpoint(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map part_to_breakpoint junction record with before-change values.
        
        Handles the complex composite primary key (part_id, breakpoint_id, model_id)
        and preserves before-change snapshots as text (no ID mapping).
        
        IMPORTANT: This function assumes data has already been validated during
        the transformation phase. It only performs ID lookups.
        
        Args:
            record: Dict with fields from PART_TO_BREAKPOINT_COLS configuration
                Required fields (from BP_JUNCTION_REQUIRED):
                    - part_number: Part number being changed
                    - breakpoint_number: Breakpoint identifier
                    - model_code: Model this change applies to
                
                Optional fields (from BP_JUNCTION_OPTIONAL):
                    - action: Type of change (already validated by enum_validator)
                    - supplier_name: Current/new supplier name (for after-change)
                    - line_code: Current/new line code (for after-change)
                    - part_number_before_change: Previous part number (snapshot)
                    - supplier_name_before_change: Previous supplier name (snapshot)
                    - line_name_before_change: Previous line name (snapshot)
                    - localization_before_change: Localization before change (already validated)
        
        Returns:
            Dict with all fields mapped to database IDs or None if mapping fails
            
        Notes:
            - action and localization_before_change are already validated by enum_validator
            - before-change fields (*_before_change) are preserved as text snapshots
            - supplier_id and line_id are required (must be present after mapping)
        """
        try:
            # Check for all required columns from BP_JUNCTION_REQUIRED
            required_cols = BP_JUNCTION_REQUIRED['part_to_breakpoint']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in part_to_breakpoint record",
                        col
                    )
                    return None
                if not record[col] or (isinstance(record[col], str) and record[col].strip() == ''):
                    logger.debug(
                        "Required column '%s' has empty value in record",
                        col
                    )
                    return None

            # Mapping part_number → part_id
            part_id = self.get_id('part_number', record['part_number'])
            if not part_id:
                logger.warning(
                    "No part_id found for part_number: %s",
                    record['part_number']
                )
                return None

            # Mapping breakpoint_number → breakpoint_id
            breakpoint_id = self.get_id('breakpoint_number', record['breakpoint_number'])
            if not breakpoint_id:
                logger.warning(
                    "No breakpoint_id found for breakpoint_number: %s",
                    record['breakpoint_number']
                )
                return None

            # Mapping model_code → model_id
            model_id = self.get_id('model_code', record['model_code'])
            if not model_id:
                logger.warning(
                    "No model_id found for model_code: %s",
                    record['model_code']
                )
                return None

            # Initialize result with required fields (composite primary key)
            result = {
                'part_id': part_id,
                'breakpoint_id': breakpoint_id,
                'model_id': model_id
            }

            # Get optional fields configuration
            optional_cols = BP_JUNCTION_OPTIONAL['part_to_breakpoint']

            # Track which required optional fields are present
            has_supplier = False
            has_line = False

            # Process each optional field according to its type
            for col in optional_cols:
                if col not in record or record[col] is None:
                    continue

                value = record[col]

                # Handle different types of optional fields
                if col == 'action':
                    # Action is already validated by enum_validator
                    result['action'] = str(value).strip().lower()

                elif col == 'supplier_name':
                    # Supplier name needs to be mapped to ID
                    has_supplier = True
                    supplier_id = self.get_id('supplier_name', value)
                    if supplier_id:
                        result['supplier_id'] = supplier_id
                    else:
                        logger.warning(
                            "No supplier_id found for supplier_name: %s",
                            value
                        )
                        # Still required field in database - can't proceed without it
                        return None

                elif col == 'line_code':
                    # Line code needs to be mapped to ID
                    has_line = True
                    line_id = self.get_id('line_code', value)
                    if line_id:
                        result['line_id'] = line_id
                    else:
                        logger.warning(
                            "No line_id found for line_code: %s",
                            value
                        )
                        # Still required field in database - can't proceed without it
                        return None

                elif col == 'localization_before_change':
                    # Localization is already validated by enum_validator
                    result['localization_before_change'] = str(value).strip().lower()

                elif col in [
                    'part_number_before_change',
                    'supplier_name_before_change',
                    'line_name_before_change'
                ]:
                    # Before-change fields are preserved as text snapshots
                    str_value = str(value).strip()
                    if str_value:
                        result[col] = str_value

                else:
                    # For any other optional fields, preserve as is
                    if value is not None:
                        result[col] = value

            # Set default values for fields that might be missing
            if 'action' not in result:
                result['action'] = 'no data'

            if 'localization_before_change' not in result:
                result['localization_before_change'] = 'no data'

            # Verify that required fields are present
            if not has_supplier and 'supplier_id' not in result:
                logger.warning(
                    "Missing supplier_name in record for part %s",
                    record['part_number']
                )
                return None

            if not has_line and 'line_id' not in result:
                logger.warning(
                    "Missing line_code in record for part %s",
                    record['part_number']
                )
                return None

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping part_to_breakpoint record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping part_to_breakpoint record: %s",  # Исправлено!
                unexpected_error
            )
            return None

    # ========== MAIN MAPPING METHODS ==========

    def get_supplier_mapping(self) -> dict[str, Any]:
        """Get supplier_name → supplier_id mapping."""
        cache_key = "SupplierData_supplier_name"
        if cache_key not in self._cached_mappings:
            self._load_mapping(SupplierData, 'supplier_name', 'supplier_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_part_mapping(self) -> dict[str, Any]:
        """Get part_number → part_id mapping."""
        cache_key = "PartData_part_number"
        if cache_key not in self._cached_mappings:
            self._load_mapping(PartData, 'part_number', 'part_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_model_mapping(self) -> dict[str, Any]:
        """Get model_code → model_id mapping."""
        cache_key = "ModelData_model_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(ModelData, 'model_code', 'model_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_line_mapping(self) -> dict[str, Any]:
        """Get line_code → line_id mapping."""
        cache_key = "LineData_line_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(LineData, 'line_code', 'line_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_breakpoint_mapping(self) -> dict[str, Any]:
        """Get breakpoint_number → breakpoint_id mapping."""
        cache_key = "BreakpointData_breakpoint_number"
        if cache_key not in self._cached_mappings:
            self._load_mapping(BreakpointData, 'breakpoint_number', 'breakpoint_id', cache_key)
        return self._cached_mappings[cache_key]

    def pre_load_all_mappings(self):
        """
        Pre-load all required mappings for breakpoint processing.
        
        This method loads all mappings defined in BP_LOOKUP_TABLES
        from columns_config.py to optimize performance for bulk operations.
        
        The method dynamically determines which mappings to load based on
        the configuration, making it easy to add new lookup tables in the future.
        
        Example:
            >>> mapper.pre_load_all_mappings()
            >>> # All mappings are now cached and ready for fast lookups
        """
        logger.info("Pre-loading all breakpoint mappings from BP_LOOKUP_TABLES...")

        # Define mapping between table names in BP_LOOKUP_TABLES and loader methods
        mapping_methods = {
            'supplier': self.get_supplier_mapping,
            'part': self.get_part_mapping,
            'model': self.get_model_mapping,
            'line': self.get_line_mapping,
            'breakpoint': self.get_breakpoint_mapping,
        }

        # Define cache key mapping for verification
        cache_key_map = {
            'supplier': "SupplierData_supplier_name",
            'part': "PartData_part_number",
            'model': "ModelData_model_code",
            'line': "LineData_line_code",
            'breakpoint': "BreakpointData_breakpoint_number",
        }

        # Track loading statistics
        loaded_count = 0
        failed_count = 0
        skipped_count = 0
        expected_tables = list(BP_LOOKUP_TABLES.keys())

        # Track specific tables for detailed reporting
        loaded_tables = []
        failed_tables = []
        skipped_tables = []

        # Load each mapping type defined in BP_LOOKUP_TABLES
        for table_name in expected_tables:
            if table_name in mapping_methods:
                try:
                    # Attempt to load the mapping
                    mapping_methods[table_name]()

                    # Verify mapping was loaded successfully
                    cache_key = cache_key_map.get(table_name)
                    if cache_key and cache_key in self._cached_mappings:
                        loaded_count += 1
                        loaded_tables.append(table_name)
                        logger.debug("Successfully loaded mapping for: %s", table_name)
                    else:
                        failed_count += 1
                        failed_tables.append(table_name)
                        logger.error(
                            "Mapping method for '%s' executed but cache key '%s' not found",
                            table_name, cache_key
                        )

                except SQLAlchemyError as e:
                    failed_count += 1
                    failed_tables.append(table_name)
                    logger.error(
                        "Database error loading mapping for table '%s': %s",
                        table_name, str(e)
                    )
                except (ValueError, TypeError, AttributeError) as e:
                    failed_count += 1
                    failed_tables.append(table_name)
                    logger.error(
                        "Data error loading mapping for table '%s': %s",
                        table_name, str(e)
                    )
                except Exception as unexpected_error:
                    failed_count += 1
                    failed_tables.append(table_name)
                    logger.error(
                        "Unexpected error loading mapping for table '%s': %s",
                        table_name, str(unexpected_error)
                    )
            else:
                skipped_count += 1
                skipped_tables.append(table_name)
                logger.warning(
                    "No mapping method found for table: '%s'. "
                    "Available methods: %s",
                    table_name, ', '.join(mapping_methods.keys())
                )

        # Log cache statistics
        total_entries = sum(len(mapping) for mapping in self._cached_mappings.values())

        # Comprehensive summary based on results
        if loaded_count == len(expected_tables):
            logger.info(
                "All breakpoint mappings pre-loaded successfully: "
                "%d/%d tables loaded, %d total entries",
                loaded_count, len(expected_tables), total_entries
            )
            if loaded_tables:
                logger.debug("Loaded mappings for: %s", ', '.join(loaded_tables))

        elif loaded_count > 0:
            logger.warning(
                "Partial breakpoint mappings loaded: "
                "%d/%d tables loaded, %d failed, %d skipped, %d total entries",
                loaded_count, len(expected_tables),
                failed_count, skipped_count, total_entries
            )

            if loaded_tables:
                logger.info("Successfully loaded: %s", ', '.join(loaded_tables))
            if failed_tables:
                logger.warning("Failed to load: %s", ', '.join(failed_tables))
            if skipped_tables:
                logger.warning("Skipped (no mapping method): %s", ', '.join(skipped_tables))

        else:
            logger.error(
                "Failed to load any breakpoint mappings: "
                "0/%d tables loaded, %d failed, %d skipped",
                len(expected_tables), failed_count, skipped_count
            )

            if failed_tables:
                logger.error("Failed tables: %s", ', '.join(failed_tables))
            if skipped_tables:
                logger.error("Skipped tables (no mapping method): %s", ', '.join(skipped_tables))

            # Provide troubleshooting information
            missing_methods = [t for t in expected_tables if t not in mapping_methods]
            if missing_methods:
                logger.error(
                    "Missing mapping methods for: %s. Add to mapping_methods dictionary.",
                    missing_methods
                )

            logger.error(
                "Troubleshooting tips:\n"
                "  - Verify database connection\n"
                "  - Check that core entity tables are populated\n"
                "  - Verify BP_LOOKUP_TABLES configuration: %s",
                expected_tables
            )

    def map_breakpoint_records(
        self,
        breakpoint_df,
        junction_type: str
    ) -> list[dict[str, Any]]:
        """
        Map breakpoint junction records with before/after value support.
        
        Args:
            breakpoint_df: Polars DataFrame with breakpoint junction data
                          (already transformed and validated)
            junction_type: Type of junction table to map
                (currently only 'part_to_breakpoint' supported)
                
        Returns:
            List of dicts with database IDs ready for insertion
            
        Examples:
            >>> records = mapper.map_breakpoint_records(df, 'part_to_breakpoint')
            >>> print(f"Successfully mapped {len(records)} records")
        """
        logger.info(
            "Mapping breakpoint records for: %s",
            junction_type
        )

        if junction_type not in BP_JUNCTION_REQUIRED:
            logger.error(
                "Unknown junction type: %s. Available types: %s",
                junction_type, list(BP_JUNCTION_REQUIRED.keys())
            )
            return []

        # Mapping junction types to corresponding handlers
        handler_map = {
            'part_to_breakpoint': self._map_part_to_breakpoint,
        }

        if junction_type not in handler_map:
            logger.error(
                "Unsupported junction type: %s",
                junction_type
            )
            return []

        handler = handler_map[junction_type]

        try:
            # Convert Polars DataFrame to list of dicts
            if hasattr(breakpoint_df, 'to_dicts'):
                records = breakpoint_df.to_dicts()
            else:
                # Fallback for pandas or other formats
                records = breakpoint_df.to_dict('records')

        except (ValueError, TypeError, AttributeError) as e:
            logger.error("Error converting DataFrame to dicts: %s", e)
            return []

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error converting DataFrame: %s",
                unexpected_error
            )
            return []

        mapped_records = []
        skipped = 0
        skipped_reasons = {}

        for idx, record in enumerate(records):
            try:
                mapped_record = handler(record)
                if mapped_record:
                    mapped_records.append(mapped_record)
                else:
                    skipped += 1
                    # Track reason for skipping (first few records only)
                    if skipped <= 10:
                        # Determine reason for skip
                        missing_fields = []
                        for req_field in BP_JUNCTION_REQUIRED['part_to_breakpoint']:
                            if req_field not in record or not record[req_field]:
                                missing_fields.append(req_field)

                        if missing_fields:
                            reason = f"Missing required fields: {missing_fields}"
                        else:
                            # Check which ID mapping failed
                            if not self.get_id('part_number', record.get('part_number')):
                                reason = "Failed part_number mapping"
                            elif not self.get_id('breakpoint_number', record.get('breakpoint_number')):
                                reason = "Failed breakpoint_number mapping"
                            elif not self.get_id('model_code', record.get('model_code')):
                                reason = "Failed model_code mapping"
                            elif not self.get_id('supplier_name', record.get('supplier_name')):
                                reason = "Failed supplier_name mapping"
                            elif not self.get_id('line_code', record.get('line_code')):
                                reason = "Failed line_code mapping"
                            else:
                                reason = "Unknown mapping failure"

                        skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error in handler for record %d: %s",
                    idx, unexpected_error
                )
                skipped += 1

        # Logging statistics
        logger.info(
            "Breakpoint mapping completed for '%s': total=%d, mapped=%d, skipped=%d",
            junction_type, len(records), len(mapped_records), skipped
        )

        # Log skipped reasons summary
        if skipped_reasons:
            logger.info("Skip reasons summary: %s", skipped_reasons)

        # Debug information
        if mapped_records:
            sample_size = min(5, len(mapped_records))
            sample = mapped_records[:sample_size]
            logger.debug("First %d mapped records: %s", sample_size, sample)

        return mapped_records

    def log_mapping_statistics(self) -> int:
        """
        Log statistics about all loaded mappings.
        
        Returns:
            int: Total number of cached mapping entries
            
        Example:
            >>> total = mapper.log_mapping_statistics()
            ============================================================
            BREAKPOINT MAPPING STATISTICS
            ============================================================
            SupplierData_supplier_name: 1250 entries
            PartData_part_number: 15420 entries
            ModelData_model_code: 8 entries
            LineData_line_code: 45 entries
            BreakpointData_breakpoint_number: 342 entries
            ------------------------------------------------------------
            Total cached mappings: 17065 entries
            ============================================================
        """
        total_entries = 0

        logger.info("=" * 60)
        logger.info("BREAKPOINT MAPPING STATISTICS")
        logger.info("=" * 60)

        for cache_key, mapping in sorted(self._cached_mappings.items()):
            count = len(mapping)
            total_entries += count
            logger.info("%s: %d entries", cache_key, count)

        logger.info("-" * 60)
        logger.info("Total cached mappings: %d entries", total_entries)
        logger.info("=" * 60)

        return total_entries

    def clear_cache(self):
        """Clear cached mappings to free memory."""
        cache_size = sum(len(mapping) for mapping in self._cached_mappings.values())
        logger.info("Clearing breakpoint cache with %d total entries.", cache_size)

        self._cached_mappings.clear()
        logger.debug("Breakpoint cache cleared.")


def create_breakpoint_mapper(engine=None) -> BreakpointObjectMapper:
    """
    Factory function to create BreakpointObjectMapper.

    Args:
        engine: Optional SQLAlchemy database engine (new one created if None)
                
    Returns:
        BreakpointObjectMapper instance ready for use
        
    Raises:
        SQLAlchemyError: If database connection fails
        RuntimeError: If mapper cannot be created
        
    Critical Timing:
        Mapper MUST be created AFTER core entity tables are loaded.
        This ensures all required ID mappings exist in the database.
        
    Example:
        >>> mapper = create_breakpoint_mapper()
        >>> # Test a simple lookup to verify mapper works
        >>> test_id = mapper.get_id('model_code', 'a01')
        >>> if test_id:
        ...     print("Mapper ready - model a01 found")
    """
    try:
        if engine is None:
            engine = initialize_database(create_tables=False)

        # Create session factory and session
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        mapper = BreakpointObjectMapper(session)
        logger.info("BreakpointObjectMapper created successfully.")

        return mapper

    except SQLAlchemyError as e:
        logger.error("Database error creating breakpoint mapper: %s", e)
        raise

    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Configuration error creating breakpoint mapper: %s", e)
        raise RuntimeError(
            f"Failed to create breakpoint mapper: {e}"
        ) from e

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error creating breakpoint mapper: %s",
            unexpected_error
        )
        raise RuntimeError(
            f"Unexpected error creating breakpoint mapper: {unexpected_error}"
        ) from unexpected_error
