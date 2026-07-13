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
    - ACTION-based business logic (replace/delete/add/update/no data)
    - Temporary record creation for new entities
    - Integration with columns_config.py for column definitions

Architecture:
    The module follows the same caching-first approach as MFTObjectMapper:
    1. On-demand loading of ID mappings from database
    2. In-memory caching for high-performance lookups
    3. Automatic cache management with manual clearing capability
    4. ACTION-based handlers for different change types
    
    IMPORTANT: This module assumes data has already been validated during the
    transformation phase (including ENUM validation). It performs ID lookups
    and applies business logic based on ACTION type.

ARCHITECTURE NOTES:

    BP Pipeline follows the CDC (Change Data Capture) pattern:
    
    1. EXTRACT: Raw data from Excel files
    2. TRANSFORM: Data cleaning, type conversion, ENUM validation
    3. MAP: ID lookups + ACTION-based business logic (this module)
    4. LOAD: Database insertion into part_to_breakpoint (history table)
    
    Key Characteristics of BP Mapper (CDC Pattern):
    
    - CREATES NEW RECORDS: Creates parts, lines, suppliers when needed
    - MODIFIES EXISTING DATA: Updates active flags, creates new versions
    - PRESERVES HISTORY: Stores before-change values as text snapshots
    - SUPPORTS TIME SERIES: Tracks when changes occurred via breakpoints
    - ACTION-BASED LOGIC: Different handlers for replace/delete/add/update
    - TEMPORARY RECORD TRACKING: Tracks new entities for loader
    
    Why BP Mapper Contains Business Logic:
    
    - Breakpoints represent ENGINEERING CHANGES over time
    - Each action type (replace/delete/add/update) requires specific logic
    - New entities must be created when referenced parts/lines don't exist
    - Historical snapshots must be preserved before changes
    - Time series data requires tracking when changes are effective
    
    Example of BP Mapper's Complexity (Replace Action):
    
        Input from transformer: {
            'part_number': 'OLD-001',           # Old part to replace
            'part_number_after_change': 'NEW-001', # New part
            'breakpoint_number': 'BP-2025-001',
            'model_code': 'MODEL-A',
            'action': 'replace',
            'line_code_after_change': 'LINE-01',
            'supplier_name': 'New Supplier'
        }
        
        Output from mapper: {
            'part_id': 'uuid-old-part',          # ID of old part
            'breakpoint_id': 'uuid-bp-001',
            'model_id': 'uuid-model-a',
            'action': 'replace',
            'part_number_before_change': 'OLD-001',
            'supplier_name_before_change': 'Old Supplier',
            'localization_before_change': 'local',
            # Metadata for loader
            '_new_part_id': 'uuid-new-part',     # Created new part
            '_new_line_id': 'uuid-line-01',      # Created new line
            '_new_part_created': True,
            '_new_line_created': True,
            '_update_part_supplier': True,
            '_update_part_line': True
        }
    
    This complexity is necessary because:
        - Engineering changes affect multiple systems
        - Historical data must be preserved for audits
        - New parts/lines may be introduced with the change
        - Time series queries require accurate change tracking

Configuration Source:
    This module uses constants from columns_config.py:
        - BP_JUNCTION_REQUIRED: Required columns for breakpoint junction
        - BP_LOOKUP_TABLES: Tables needed for ID lookups
        - BP_REQUIRED_FIELDS_BY_ACTION: Required fields per action
        - BP_DEFAULT_VALUES: Default values for missing data
        - BP_VALIDATION_RULES: Validation rules for mappings
        - BP_LOGGING_CONFIG: Logging configuration

Dependencies:
    - SQLAlchemy for ORM and database abstraction
    - PostgreSQL as the source database for ID lookups

Database Models:
    The mapper interacts with the following tables from database.py:
    
    Core Entity Tables (for ID lookups and record creation):
        - SupplierData: supplier_name → supplier_id
        - PartData: part_number → part_id
        - ModelData: model_code → model_id
        - LineData: line_code → line_id
        - BreakpointData: breakpoint_number → breakpoint_id
        - WorkshopData: for default workshop in line creation
    
    Junction Table (target for mapping):
        - PartToBreakpoint: Tracks engineering changes with composite key
          (part_id, breakpoint_id, model_id) and before-change snapshots
        - PartToLine: For creating part-line relationships

Column Mappings:
    The mapper uses the following column-to-model mappings:

    | Column Name      | Source Table    | Lookup Field      | Target ID     |
    |------------------|-----------------|-------------------|---------------|
    | part_number      | PartData        | part_number       | part_id       |
    | breakpoint_number| BreakpointData  | breakpoint_number | breakpoint_id |
    | model_code       | ModelData       | model_code        | model_id      |
    | supplier_name    | SupplierData    | supplier_name     | supplier_id   |
    | line_code        | LineData        | line_code         | line_id       |

ACTION-Based Processing:
    The mapper handles different action types with specific logic:
    
    - REPLACE: Old part replaced by new part number
        * Creates new part, line, supplier if needed
        * Preserves old values in before-change fields
        * Updates relationships for new part
        * Example: Part OLD-001 replaced by NEW-001 from breakpoint BP-2025-001
    
    - DELETE: Part removed from production
        * Flags part for soft deletion
        * Preserves old values in before-change fields
        * Example: Part OBS-001 removed from all models after breakpoint BP-2025-002
    
    - ADD: New part introduced
        * Creates new part, line, supplier if needed
        * No before-change values
        * Example: New part NEW-002 added for model Z from breakpoint BP-2025-003
    
    - UPDATE: Part attributes changed without part number change
        * Updates supplier and/or line relationships
        * Preserves old values in before-change fields
        * DESCRIPTION field may contain change details
        * Example: Supplier changed for part ABC-001 from breakpoint BP-2025-004
    
    - NO DATA: Unknown action - flagged for manual review
        * Preserves available data
        * Sets _needs_manual_review flag
        * Example: Incomplete data requiring manual investigation

Comparison with MFT Mapper:
    | Aspect              | MFT Mapper (ETL)          | BP Mapper (CDC)           |
    |---------------------|---------------------------|---------------------------|
    | Primary Purpose     | ID lookup only            | ID lookup + business logic|
    | Creates new records | No                        | Yes (parts, lines, supp.) |
    | Modifies data       | No                        | Yes (soft delete, update) |
    | Business logic      | None                      | ACTION-based handlers     |
    | Optional fields     | Simple copy               | Action-specific handling  |
    | Time series support | No                        | Yes (breakpoint tracking) |
    | Complexity          | Low                       | High                      |
    | Transaction scope   | Read-only                 | Creates new records       |

Performance Considerations:
    - Lazy loading: Mappings are loaded on first use
    - Memory caching: Reduces database queries for repeated lookups
    - Bulk loading: Pre-load methods for performance-critical operations
    - Cache statistics: Built-in logging for performance monitoring
    - Temporary record tracking: For new entities created during mapping
    - Warning: Batch processing >100 records triggers performance warnings

Security Notes:
    - Read-only operations for ID lookups (get_id method)
    - Write operations only for temporary record creation
    - Input validation for required fields
    - No SQL injection risk (uses ORM with parameterized queries)

Usage Example:
    ```
    from dags.tasks.bp_mapper import create_bp_mapper

    Create mapper (after entity tables are loaded)
    mapper = create_bp_mapper()

    Pre-load all mappings for bulk operations
    mapper.pre_load_all_mappings()

    Map breakpoint junction records
    records = mapper.map_breakpoint_records(record_dicts, 'part_to_breakpoint')

    Get temporary records created during mapping
    temp_records = mapper.get_temp_records()
    print(f"Created {len(temp_records['parts'])} new parts")

    Check mapping statistics
    mapper.log_mapping_statistics()
    print(f"Processed {mapper.get_stats()['processed']} records")

    Clear cache when done
    mapper.clear_cache()
    ```
Integration with ETL Pipeline:
    EXTRACT → TRANSFORM → MAP → LOAD
                            ↑
                        bp_mapper.py
                        (ID lookups + ACTION logic)

Pipeline Flow:
    1. EXTRACT: Raw data from Excel (bp_extractor.py)
    2. TRANSFORM: Data cleaning and ENUM validation (bp_transformer.py)
    3. MAP: Text-to-ID conversion + ACTION business logic (this module)
    4. LOAD: Database insertion into part_to_breakpoint (bp_loader.py)

Critical Timing:
    - Mapper MUST be created AFTER core entity tables are loaded
    - Core tables provide ID mappings for lookups
    - New entities created during mapping are temporary (flush, no commit)
    - Loader commits all changes (new entities + history records)

Error Handling:
    - Missing required fields: Returns None with debug logging
    - Failed ID lookups: Returns None with warning logging
    - Database errors: Caught and logged, empty mapping returned
    - Unexpected errors: Caught, logged, and handled gracefully
    - Action-specific validation: Ensures required fields per action
    - Skip tracking: First 10 skipped records have detailed reasons logged

Integration Notes:
    - Must be created AFTER core entity tables are loaded (depends on their IDs)
    - Used by bp_loader.py for historical change processing
    - Cache should be cleared after bulk operations to free memory
    - Temporary records must be committed or rolled back by loader
    - Designed for read-heavy scenarios with occasional temp record creation
    - Session flush is used to get IDs without committing (allows rollback)
    - Metadata fields (prefixed with '_') are for loader instructions

Version: 1.0.0
Compatibility: Python 3.14.4+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-03-18
Last Modified: 2026-03-21
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import logging
from datetime import datetime
from typing import Any, Optional, Dict, List, Tuple, Union, cast

# Third-party imports
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
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
    BP_LOOKUP_TABLES,
    BP_REQUIRED_FIELDS_BY_ACTION,
    BP_DEFAULT_VALUES,
    BP_VALIDATION_RULES,
    BP_LOGGING_CONFIG
)
from dags.tasks.connector import initialize_database
from database.database import (
    SupplierData, PartData, ModelData, LineData, BreakpointData,
    WorkshopData, PartToLine
)

# Logger setup
logger = get_logger(__name__)

# Type aliases for better readability
StrMappingDict = Dict[str, str]  # Simple key-value mapping (e.g., part_number -> part_id)
ModelInstance = Union[SupplierData, PartData, ModelData, LineData, BreakpointData, WorkshopData]
ObjMappingDict = Dict[str, ModelInstance]  # For storing model objects by ID
CacheDict = Dict[str, Union[StrMappingDict, ObjMappingDict]]  # Cache can contain both types

TempRecordsDict = Dict[str, List[ModelInstance]]
StatsDict = Dict[str, int]
RecordDict = Dict[str, Any]
MappedRecordDict = Dict[str, Any]


class BreakpointObjectMapper:
    """
    Main mapper class for converting breakpoint external identifiers to database primary keys.

    This class extends the basic ID mapping with ACTION-based business logic,
    temporary record creation, and comprehensive statistics tracking.

    Attributes:
        session (Session): SQLAlchemy database session
        _cached_mappings (CacheDict): Cache of ID mappings and model objects
        _temp_records (TempRecordsDict): Temporary records created during mapping
        _stats (StatsDict): Mapping statistics (all integers)
        _by_action (Dict[str, int]): Statistics broken down by action type
    """

    # Configuration with table names (following MFTObjectMapper pattern)
    COLUMN_TO_MODEL: Dict[str, Tuple[Any, str, str]] = {
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
    ) -> None:
        """
        Initialize breakpoint mapper with database session.
    
        Args:
            session: SQLAlchemy Session for database operations
    
        Example:
            >>> session_factory = sessionmaker(bind=engine)
            >>> mapper = BreakpointObjectMapper(session_factory())
        """
        self.session = session
        self._cached_mappings: CacheDict = {}
        self._temp_records: TempRecordsDict = {
            'parts': [],
            'lines': [],
            'suppliers': []
        }
        self._stats: StatsDict = {
            'processed': 0,
            'created_parts': 0,
            'created_lines': 0,
            'created_suppliers': 0,
            'errors': 0,
            'warnings': 0,
            'skipped': 0
        }
        self._by_action: Dict[str, int] = {}

        logger.debug(
            "BreakpointObjectMapper initialized with change tracking support"
        )

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
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return None

        try:
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

            mapping = self._cached_mappings.get(cache_key, {})

            if not isinstance(mapping, dict):
                logger.error(
                    "Cache key %s contains wrong type",
                    cache_key
                )
                return None

            # Clean the value for lookup
            clean_value = str(value).strip()
            result = mapping.get(clean_value)

            # Result should be string if found
            if result is not None and isinstance(result, str):
                logger.debug(
                    "Found ID for %s='%s': %s",
                    column_name, clean_value, result
                )
                return result

            logger.debug(
                "No ID found for %s='%s'",
                column_name, clean_value
            )
            return None

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in get_id for %s='%s': %s",
                error_type, column_name, value, e
            )
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in get_id for %s='%s': %s",
                column_name, value, unexpected_error
            )
            return None

    def _load_mapping(
        self,
        model_class: Any,
        lookup_column: str,
        id_column: str,
        cache_key: str
    ) -> None:
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
            mapping: StrMappingDict = {}

            for lookup_val, db_id in query:
                if lookup_val is not None:
                    # Convert to string and strip
                    str_val = str(lookup_val).strip()
                    str_id = str(db_id).strip()
                    mapping[str_val] = str_id

            self._cached_mappings[cache_key] = mapping

            # Logging information about uploaded mappings
            total_count = len(mapping)
            logger.info(
                "Loaded mapping %s: %d entries",
                cache_key, total_count
            )

            # Debug sample
            if mapping and logger.isEnabledFor(logging.DEBUG):
                sample_size = min(5, total_count)
                sample_items = list(mapping.items())[:sample_size]
                logger.debug(
                    "Sample values (first %d): %s",
                    sample_size, sample_items
                )

        except SQLAlchemyError as e:
            logger.error(
                "Database error loading mapping for %s: %s",
                cache_key, e
            )
            self._cached_mappings[cache_key] = {}

        except (ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s loading mapping for %s: %s",
                error_type, cache_key, e
            )
            self._cached_mappings[cache_key] = {}

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error loading mapping for %s: %s",
                cache_key, unexpected_error, exc_info=True
            )
            self._cached_mappings[cache_key] = {}

    # ========== TEMPORARY RECORD CREATION METHODS ==========

    def _create_temp_part(
        self,
        part_number: str,
        supplier_id: Optional[str] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Create a temporary part record and return its ID.
        This does NOT commit - just flushes to get ID.

        Args:
            part_number: Part number to create
            supplier_id: Optional supplier ID

        Returns:
            Tuple of (part_id, created_flag)
        """
        try:
            part_name = f"{BP_DEFAULT_VALUES['part_name_prefix']}: {part_number}"

            new_part = PartData(
                part_number=part_number,
                part_name=part_name,
                supplier_id=supplier_id
            )
            self.session.add(new_part)
            self.session.flush()  # Get ID without commit

            part_id = str(new_part.part_id) if new_part.part_id else None

            if not part_id:
                logger.error(
                    "Failed to get part_id after flush for part %s",
                    part_number
                )
                return None, False

            # Track temp record for potential rollback
            self._temp_records['parts'].append(new_part)

            # Update cache - part_number -> part_id mapping (STRING TO STRING)
            part_num_cache_key = 'PartData_part_number'
            part_num_cache = self._cached_mappings.get(part_num_cache_key)

            if part_num_cache is None:
                part_num_cache = {}
                self._cached_mappings[part_num_cache_key] = part_num_cache

            if isinstance(part_num_cache, dict):
                str_cache = cast(StrMappingDict, part_num_cache)
                str_cache[part_number] = part_id

            # Update cache - part_id -> part object mapping (STRING TO MODEL)
            part_id_cache_key = 'PartData_part_id'
            part_id_cache = self._cached_mappings.get(part_id_cache_key)

            if part_id_cache is None:
                part_id_cache = {}
                self._cached_mappings[part_id_cache_key] = part_id_cache

            if isinstance(part_id_cache, dict):
                obj_cache = cast(ObjMappingDict, part_id_cache)
                obj_cache[part_id] = new_part

            self._stats['created_parts'] += 1
            logger.info(
                "Created temp part: %s with ID: %s",
                part_number, part_id
            )
            return part_id, True

        except SQLAlchemyError as e:
            logger.error(
                "Database error creating temp part %s: %s",
                part_number, e
            )
            return None, False

        except (ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s creating temp part %s: %s",
                error_type, part_number, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating temp part %s: %s",
                part_number, unexpected_error, exc_info=True
            )
            return None, False

    def _create_temp_line(
        self,
        line_code: str,
        line_name: Optional[str] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Create a temporary line record and return its ID.
        This does NOT commit - just flushes to get ID.

        Args:
            line_code: Line code to create
            line_name: Optional line name

        Returns:
            Tuple of (line_id, created_flag)
        """
        try:
            # Get default workshop
            workshop = self.session.query(WorkshopData).filter(
                WorkshopData.workshop_code == BP_DEFAULT_VALUES['workshop_default_code']
            ).first()

            if not workshop:
                logger.error(
                    "No default workshop found for line creation"
                )
                return None, False

            final_line_name = line_name or f"{BP_DEFAULT_VALUES['line_name_prefix']} {line_code}"
            new_line = LineData(
                line_code=line_code,
                line_name=final_line_name,
                workshop_id=workshop.workshop_id
            )
            self.session.add(new_line)
            self.session.flush()  # Get ID without commit

            line_id = str(new_line.line_id) if new_line.line_id else None

            if not line_id:
                logger.error(
                    "Failed to get line_id after flush for line %s",
                    line_code
                )
                return None, False

            # Track temp record
            self._temp_records['lines'].append(new_line)

            # Update cache - line_code -> line_id mapping (STRING TO STRING)
            line_code_cache_key = 'LineData_line_code'
            line_code_cache = self._cached_mappings.get(line_code_cache_key)
            if line_code_cache is None:
                line_code_cache = {}
                self._cached_mappings[line_code_cache_key] = line_code_cache

            if isinstance(line_code_cache, dict):
                str_cache = cast(StrMappingDict, line_code_cache)
                str_cache[line_code] = line_id

            # Update cache - line_id -> line object mapping (STRING TO MODEL)
            line_id_cache_key = 'LineData_line_id'
            line_id_cache = self._cached_mappings.get(line_id_cache_key)

            if line_id_cache is None:
                line_id_cache = {}
                self._cached_mappings[line_id_cache_key] = line_id_cache

            if isinstance(line_id_cache, dict):
                obj_cache = cast(ObjMappingDict, line_id_cache)
                obj_cache[line_id] = new_line

            self._stats['created_lines'] += 1
            logger.info(
                "Created temp line: %s with ID: %s",
                line_code, line_id
            )
            return line_id, True

        except SQLAlchemyError as e:
            logger.error(
                "Database error creating temp line %s: %s",
                line_code, e
            )
            return None, False

        except (ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s creating temp line %s: %s",
                error_type, line_code, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating temp line %s: %s",
                line_code, unexpected_error, exc_info=True
            )
            return None, False

    def _create_temp_supplier(
        self,
        supplier_name: str,
        localization: Optional[str] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Create a temporary supplier record and return its ID.
        This does NOT commit - just flushes to get ID.

        Args:
            supplier_name: Supplier name to create
            localization: Localization status

        Returns:
            Tuple of (supplier_id, created_flag)
        """
        try:
            final_localization = localization or BP_DEFAULT_VALUES['localization']

            new_supplier = SupplierData(
                supplier_name=supplier_name,
                localization=final_localization
            )
            self.session.add(new_supplier)
            self.session.flush()  # Get ID without commit

            supplier_id = str(new_supplier.supplier_id) if new_supplier.supplier_id else None

            if not supplier_id:
                logger.error("Failed to get supplier_id after flush for supplier %s", supplier_name)
                return None, False

            # Track temp record
            self._temp_records['suppliers'].append(new_supplier)

            # Update cache - supplier_name -> supplier_id mapping (STRING TO STRING)
            supplier_name_cache_key = 'SupplierData_supplier_name'
            supplier_name_cache = self._cached_mappings.get(supplier_name_cache_key)

            if supplier_name_cache is None:
                supplier_name_cache = {}
                self._cached_mappings[supplier_name_cache_key] = supplier_name_cache

            if isinstance(supplier_name_cache, dict):
                str_cache = cast(StrMappingDict, supplier_name_cache)
                str_cache[supplier_name] = supplier_id

            # Update cache - supplier_id -> supplier object mapping (STRING TO MODEL)
            supplier_id_cache_key = 'SupplierData_supplier_id'
            supplier_id_cache = self._cached_mappings.get(supplier_id_cache_key)

            if supplier_id_cache is None:
                supplier_id_cache = {}
                self._cached_mappings[supplier_id_cache_key] = supplier_id_cache

            if isinstance(supplier_id_cache, dict):
                obj_cache = cast(ObjMappingDict, supplier_id_cache)
                obj_cache[supplier_id] = new_supplier

            self._stats['created_suppliers'] += 1
            logger.info(
                "Created temp supplier: %s with ID: %s",
                supplier_name,
                supplier_id
            )
            return supplier_id, True

        except SQLAlchemyError as e:
            logger.error(
                "Database error creating temp supplier %s: %s",
                supplier_name, e
            )
            return None, False

        except (ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s creating temp supplier %s: %s",
                error_type, supplier_name, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating temp supplier %s: %s",
                supplier_name, unexpected_error, exc_info=True
            )
            return None, False

    def _get_object_by_id(
        self,
        cache_key: str,
        obj_id: str
    ) -> Optional[ModelInstance]:
        """
        Get model object by ID from cache.

        Args:
            cache_key: Cache key for object mapping
            obj_id: Object ID

        Returns:
            Model instance or None if not found
        """
        try:
            obj_cache = self._cached_mappings.get(cache_key)

            if obj_cache and isinstance(obj_cache, dict):
                obj_dict = cast(ObjMappingDict, obj_cache)
                return obj_dict.get(obj_id)

            return None

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s getting object by ID from cache %s: %s",
                error_type, cache_key, e
            )
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error getting object by ID from cache %s: %s",
                cache_key, unexpected_error
            )
            return None

    def _get_or_create_part(
        self,
        part_number: str,
        action_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Get existing part ID or create temporary new part.

        Args:
            part_number: Part number to find or create
            action_data: Additional data for part creation

        Returns:
            Tuple of (part_id, created_flag)
        """
        if not part_number or (isinstance(part_number, str) and part_number.strip() == ''):
            logger.debug(
                "Skipping part number creation - empty part number"
            )
            return None, False

        try:
            # Check cache first
            part_id = self.get_id('part_number', part_number)
            if part_id:
                return part_id, False

            # Create new temp part
            supplier_id: Optional[str] = None
            if action_data and 'supplier_name' in action_data:
                supplier_name = action_data['supplier_name']
                if isinstance(supplier_name, str) and supplier_name.strip():
                    supplier_id_result, _ = self._get_or_create_supplier(
                        supplier_name,
                        action_data.get('localization')
                    )
                    supplier_id = supplier_id_result

            return self._create_temp_part(part_number, supplier_id)

        except (KeyError, ValueError, TypeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _get_or_create_part for %s: %s",
                error_type, part_number, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _get_or_create_part for %s: %s",
                part_number, unexpected_error
            )
            return None, False

    def _get_or_create_line(
        self,
        line_code: str,
        line_name: Optional[str] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Get existing line ID or create temporary new line.

        Args:
            line_code: Line code to find or create
            line_name: Optional line name

        Returns:
            Tuple of (line_id, created_flag)
        """
        if not line_code or (isinstance(line_code, str) and line_code.strip() == ''):
            logger.debug(
                "Skipping line creation - empty line code"
            )
            return None, False

        try:
            # Check cache first
            line_id = self.get_id('line_code', line_code)
            if line_id:
                # Update line name in cache only - loader will handle DB update
                if line_name and line_name.strip():
                    line_obj = self._get_object_by_id('LineData_line_id', line_id)
                    if line_obj and isinstance(line_obj, LineData):
                        # Getting the current line name as a string
                        current_name = str(line_obj.line_name) if line_obj.line_name else None
                        if current_name != line_name:
                            # Using setattr to avoid mypy error
                            setattr(line_obj, 'line_name', line_name)
                            logger.debug(
                                "Updated line name in cache for line %s: %s -> %s",
                                line_code, current_name, line_name
                            )
                return line_id, False

            return self._create_temp_line(line_code, line_name)

        except (KeyError, ValueError, TypeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _get_or_create_line for %s: %s",
                error_type, line_code, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _get_or_create_line for %s: %s",
                line_code, unexpected_error
            )
            return None, False

    def _get_or_create_supplier(
        self,
        supplier_name: str,
        localization: Optional[str] = None
    ) -> Tuple[Optional[str], bool]:
        """
        Get existing supplier ID or create temporary new supplier.

        Args:
            supplier_name: Supplier name to find or create
            localization: Localization status

        Returns:
            Tuple of (supplier_id, created_flag)
        """
        if not supplier_name or (isinstance(supplier_name, str) and supplier_name.strip() == ''):
            logger.debug(
                "Skipping supplier creation - empty supplier name"
            )
            return None, False

        try:
            # Check cache first
            supplier_id = self.get_id('supplier_name', supplier_name)
            if supplier_id:
                # Update localization in cache only
                if localization:
                    supplier_obj = self._get_object_by_id('SupplierData_supplier_id', supplier_id)
                    if supplier_obj and isinstance(supplier_obj, SupplierData):
                        # Getting the current localization as a string
                        current_localization = str(supplier_obj.localization) if supplier_obj.localization else None
                        # Using setattr to avoid mypy error
                        setattr(supplier_obj, 'localization', localization)
                        logger.debug(
                            "Updated localization in cache for supplier %s: %s -> %s",
                            supplier_name, current_localization, localization
                        )
                return supplier_id, False

            return self._create_temp_supplier(supplier_name, localization)

        except (KeyError, ValueError, TypeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _get_or_create_supplier for %s: %s",
                error_type, supplier_name, e
            )
            return None, False

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _get_or_create_supplier for %s: %s",
                supplier_name, unexpected_error
            )
            return None, False

    # ========== ACTION VALIDATION METHODS ==========

    def _validate_action_data(
            self,
            record: RecordDict,
            action: str
        ) -> Tuple[bool, List[str]]:
        """
        Validate that required fields for action are present.

        Args:
            record: Record dictionary
            action: Action type

        Returns:
            Tuple of (is_valid, list_of_missing_fields)
        """
        try:
            required_fields = BP_REQUIRED_FIELDS_BY_ACTION.get(action, [])
            missing: List[str] = []

            for field in required_fields:
                field_value = record.get(field)
                if field_value is None or (
                    isinstance(field_value, str) and field_value.strip() == ''
                ):
                    missing.append(field)

            if missing:
                logger.warning(
                    "Missing required fields for %s: %s",
                    action,
                    missing
                )
                self._stats['warnings'] += 1

            return len(missing) == 0, missing

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s validating action data for %s: %s",
                error_type, action, e
            )
            self._stats['errors'] += 1
            return False, [str(e)]

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error validating action data for %s: %s",
                action, unexpected_error
            )
            self._stats['errors'] += 1
            return False, [str(unexpected_error)]

    # ========== ACTION HANDLER METHODS ==========

    def _handle_replace_action(
            self,
            record: RecordDict,
            base_data: RecordDict
        ) -> Optional[RecordDict]:
        """
        Handle REPLACE action - prepare data for replacing old part with new part.

        Args:
            record: Original record with all fields
            base_data: Base mapped data (part_id, breakpoint_id, model_id)

        Returns:
            Mapped record with metadata or None if mapping fails
        """
        try:
            part_number = record.get('part_number')
            part_number_after = record.get('part_number_after_change')

            logger.info(
                "Handling REPLACE action: %s -> %s",
                part_number,
                part_number_after
            )

            # Validate required fields
            is_valid, _ = self._validate_action_data(record, 'replace')
            if not is_valid:
                self._stats['errors'] += 1
                return None

            # Get or create new part
            new_part_id: Optional[str] = None
            if part_number_after and isinstance(part_number_after, str) and part_number_after.strip():
                new_part_id, _ = self._get_or_create_part(
                    part_number_after.strip(),
                    {
                        'supplier_name': record.get('supplier_name'),
                        'localization': record.get('localization_after_change')
                    }
                )

            # Get or create new line
            new_line_id: Optional[str] = None
            new_line_created = False
            line_code_after = record.get('line_code_after_change')
            if line_code_after and isinstance(line_code_after, str) and line_code_after.strip():
                new_line_id, new_line_created = self._get_or_create_line(
                    line_code_after.strip(),
                    record.get('line_name_after_change')
                )

            # Get or create new supplier
            new_supplier_id: Optional[str] = None
            supplier_name = record.get('supplier_name')
            if supplier_name and isinstance(supplier_name, str) and supplier_name.strip():
                new_supplier_id, _ = self._get_or_create_supplier(
                    supplier_name.strip(),
                    record.get('localization_after_change')
                )

            # Creating a PART-TO-LINE connection for a new part
            if new_part_id and new_line_id:
                # Check if such a connection already exists
                existing_ptl = self.session.query(PartToLine).filter(
                    PartToLine.part_id == new_part_id,
                    PartToLine.line_id == new_line_id
                ).first()
                # Creating a new connection
                if not existing_ptl:
                    part_to_line = PartToLine(
                        part_id=new_part_id,
                        line_id=new_line_id
                    )
                    self.session.add(part_to_line)
                    logger.debug(
                        "Created PartToLine relation for new part %s and line %s",
                        part_number_after,
                        record.get('line_code_after_change')
                    )

            # Get old values for before fields
            old_part_id = base_data.get('part_id')
            if not isinstance(old_part_id, str):
                old_part_id = None

            old_supplier_name: Optional[str] = None
            old_localization = BP_DEFAULT_VALUES['localization']

            if old_part_id:
                # Try to get old supplier info
                old_part = self._get_object_by_id('PartData_part_id', old_part_id)
                if old_part and isinstance(old_part, PartData):
                    # Checking for supplier_id
                    if old_part.supplier_id:
                        supplier_id = str(old_part.supplier_id)
                        old_supplier = self._get_object_by_id('SupplierData_supplier_id', supplier_id)
                        if old_supplier and isinstance(old_supplier, SupplierData):
                            old_supplier_name = str(old_supplier.supplier_name) if old_supplier.supplier_name else None
                            old_localization = str(old_supplier.localization) if old_supplier.localization else BP_DEFAULT_VALUES['localization']

            # Prepare result with metadata for loader
            result: RecordDict = {
                **base_data,
                'supplier_id': new_supplier_id,
                'line_id': new_line_id,
                'action': 'replace',
                'part_number_before_change': part_number,
                'supplier_name_before_change': record.get('supplier_name_before_change', old_supplier_name),
                'localization_before_change': record.get('localization_before_change', old_localization),
                'line_name_before_change': record.get('line_name_before_change'),
                # Metadata for loader
                '_new_part_id': new_part_id,
                '_new_part_created': new_part_id is not None,
                '_new_line_id': new_line_id,
                '_new_line_created': new_line_created,
                '_update_part_supplier': True,
                '_update_part_line': True
            }

            self._stats['processed'] += 1
            self._by_action['replace'] = self._by_action.get('replace', 0) + 1
            return result

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _handle_replace_action for part %s: %s",
                error_type, record.get('part_number'), e
            )
            self._stats['errors'] += 1
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _handle_replace_action for part %s: %s",
                record.get('part_number'), unexpected_error, exc_info=True
            )
            self._stats['errors'] += 1
            return None

    def _handle_delete_action(
        self,
        record: RecordDict,
        base_data: RecordDict
    ) -> Optional[RecordDict]:
        """
        Handle DELETE action - prepare data for soft deleting old part.
        With a soft delete, the part_id continues to exist and be used.
        IMPORTANT: Sets is_active=False, deactivated_at, and deactivated_by_breakpoint_id

        Args:
            record: Original record with all fields
            base_data: Base mapped data (part_id, breakpoint_id, model_id)

        Returns:
            Mapped record with metadata or None if mapping fails
        """
        try:
            part_number = record.get('part_number')

            logger.info(
                "Handling DELETE action: %s",
                part_number
            )

            # Validate required fields
            is_valid, _ = self._validate_action_data(record, 'delete')
            if not is_valid:
                self._stats['errors'] += 1
                return None

            # Getting the ID of the part to be deleted
            old_part_id = self.get_id('part_number', part_number)
            if not old_part_id:
                logger.error("Cannot delete non-existent part: %s", part_number)
                self._stats['errors'] += 1
                return None

            # Get breakpoint_id from base_data
            breakpoint_id = base_data.get('breakpoint_id')

            # Get breakpoint_date for deactivation timestamp
            deactivation_date = datetime.now()
            if breakpoint_id and isinstance(breakpoint_id, str):
                bp_data = self._get_object_by_id('BreakpointData_breakpoint_id', breakpoint_id)
                if bp_data and isinstance(bp_data, BreakpointData):
                    deactivation_date = bp_data.breakpoint_date

            # Get old supplier info
            old_supplier_name: Optional[str] = None
            old_localization = BP_DEFAULT_VALUES['localization']

            old_part = self._get_object_by_id('PartData_part_id', old_part_id)
            if old_part_id and isinstance(old_part, PartData):
                if old_part.supplier_id:
                    supplier_id = str(old_part.supplier_id)
                    old_supplier = self._get_object_by_id('SupplierData_supplier_id', supplier_id)
                    if old_supplier and isinstance(old_supplier, SupplierData):
                        old_supplier_name = str(old_supplier.supplier_name) if old_supplier.supplier_name else None
                        old_localization = str(old_supplier.localization) if old_supplier.localization else BP_DEFAULT_VALUES['localization']

            logger.info(
                "DELETE action prepared: part %s (ID: %s) will be deactivated at %s",
                part_number, old_part_id, deactivation_date
            )

            # Prepare result with metadata for loader
            result: RecordDict = {
                **base_data,
                'supplier_id': None,
                'line_id': None,
                'action': 'delete',
                'part_number_before_change': part_number,
                'supplier_name_before_change': record.get('supplier_name_before_change', old_supplier_name),
                'localization_before_change': record.get('localization_before_change', old_localization),
                'line_name_before_change': record.get('line_name_before_change'),
                # Metadata for loader
                '_soft_delete': True,
                '_part_to_deactivate': old_part_id,
                '_deactivation_reason': 'delete_action',
                '_remove_part_line_relations': True,
                '_deactivation_breakpoint_id': breakpoint_id,
                '_deactivation_date': deactivation_date,
                '_set_inactive': True,
            }

            self._stats['processed'] += 1
            self._by_action['delete'] = self._by_action.get('delete', 0) + 1
            return result

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _handle_delete_action for part %s: %s",
                error_type, record.get('part_number'), e
            )
            self._stats['errors'] += 1
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _handle_delete_action for part %s: %s",
                record.get('part_number'), unexpected_error
            )
            self._stats['errors'] += 1
            return None

    def _handle_add_action(
        self,
        record: RecordDict,
        base_data: RecordDict
    ) -> Optional[RecordDict]:
        """
        Handle ADD action - prepare data for adding new part.

        Args:
            record: Original record with all fields
            base_data: Base mapped data (part_id, breakpoint_id, model_id)

        Returns:
            Mapped record with metadata or None if mapping fails
        """
        try:
            part_number_after = record.get('part_number_after_change')

            logger.info(
                "Handling ADD action: %s",
                part_number_after
            )

            # Validate required fields
            is_valid, _ = self._validate_action_data(record, 'add')
            if not is_valid:
                self._stats['errors'] += 1
                return None

            # Create new part
            new_part_id: Optional[str] = None
            if part_number_after and isinstance(part_number_after, str) and part_number_after.strip():
                new_part_id, _ = self._get_or_create_part(
                    part_number_after.strip(),
                    {
                        'supplier_name': record.get('supplier_name'),
                        'localization': record.get('localization_after_change')
                    }
                )

            # Get or create new line
            new_line_id: Optional[str] = None
            new_line_created = False
            line_code_after = record.get('line_code_after_change')
            if line_code_after and isinstance(line_code_after, str) and line_code_after.strip():
                new_line_id, new_line_created = self._get_or_create_line(
                    line_code_after.strip(),
                    record.get('line_name_after_change')
                )

            # Get or create new supplier
            new_supplier_id: Optional[str] = None
            supplier_name = record.get('supplier_name')
            if supplier_name and isinstance(supplier_name, str) and supplier_name.strip():
                new_supplier_id, _ = self._get_or_create_supplier(
                    supplier_name.strip(),
                    record.get('localization_after_change')
                )

            # Creating a PART-TO-LINE connection for a new part
            if new_part_id and new_line_id:
                # Check if such a connection already exists
                existing_ptl = self.session.query(PartToLine).filter(
                    PartToLine.part_id == new_part_id,
                    PartToLine.line_id == new_line_id
                ).first()
                # Creating a new connection
                if not existing_ptl:
                    part_to_line = PartToLine(
                        part_id=new_part_id,
                        line_id=new_line_id
                    )
                    self.session.add(part_to_line)
                    logger.debug(
                        "Created PartToLine relation for new part %s and line %s",
                        part_number_after,
                        record.get('line_code_after_change')
                    )

            # Prepare result with metadata for loader
            result: RecordDict = {
                **base_data,
                'part_id': new_part_id,  # Override part_id with new part
                'supplier_id': new_supplier_id,
                'line_id': new_line_id,
                'action': 'add',
                'part_number_before_change': None,
                'supplier_name_before_change': None,
                'localization_before_change': None,
                'line_name_before_change': None,
                # Metadata for loader
                '_new_part_id': new_part_id,
                '_new_part_created': True,
                '_new_line_id': new_line_id,
                '_new_line_created': new_line_created,
                '_create_part_line_relation': new_line_id is not None
            }

            self._stats['processed'] += 1
            self._by_action['add'] = self._by_action.get('add', 0) + 1
            return result

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _handle_add_action for part %s: %s",
                error_type, record.get('part_number_after_change'), e
            )
            self._stats['errors'] += 1
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _handle_add_action for part %s: %s",
                record.get('part_number_after_change'), unexpected_error
            )
            self._stats['errors'] += 1
            return None

    def _handle_update_action(
        self,
        record: RecordDict,
        base_data: RecordDict
    ) -> Optional[RecordDict]:
        """
        Handle UPDATE action - prepare data for updating part attributes.

        Args:
            record: Original record with all fields
            base_data: Base mapped data (part_id, breakpoint_id, model_id)

        Returns:
            Mapped record with metadata or None if mapping fails
        """
        try:
            part_number = record.get('part_number')

            logger.info(
                "Handling UPDATE action: %s",
                part_number
            )

            # Validate required fields
            is_valid, _ = self._validate_action_data(record, 'update')
            if not is_valid:
                self._stats['errors'] += 1
                return None

            part_id = base_data.get('part_id')
            if not isinstance(part_id, str):
                part_id = None

            # Get or create new line if provided
            new_line_id: Optional[str] = None
            new_line_created = False
            line_code_after = record.get('line_code_after_change')
            if line_code_after and isinstance(line_code_after, str) and line_code_after.strip():
                new_line_id, new_line_created = self._get_or_create_line(
                    line_code_after.strip(),
                    record.get('line_name_after_change')
                )

            # Get or create new supplier if provided
            new_supplier_id: Optional[str] = None
            supplier_name = record.get('supplier_name')
            if supplier_name and isinstance(supplier_name, str) and supplier_name.strip():
                new_supplier_id, _ = self._get_or_create_supplier(
                    supplier_name.strip(),
                    record.get('localization_after_change')
                )

            # Updating a PART-TO-LINE connection if line is changed
            if part_id and new_line_id:
                # Check if such a connection already exists
                existing_ptl = self.session.query(PartToLine).filter(
                    PartToLine.part_id == part_id,
                    PartToLine.line_id == new_line_id
                ).first()
                # Creating a new connection
                if not existing_ptl:
                    part_to_line = PartToLine(
                        part_id=part_id,
                        line_id=new_line_id
                    )
                    self.session.add(part_to_line)
                    logger.debug(
                        "Created PartToLine relation for part %s and new line %s",
                        part_number,
                        line_code_after
                    )

            # Get old values for before fields
            old_line_id = self.get_id('line_code', record.get('line_code_before_change'))
            old_line: Optional[LineData] = None
            if old_line_id:
                old_line_obj = self._get_object_by_id('LineData_line_id', old_line_id)
                if old_line_obj and isinstance(old_line_obj, LineData):
                    old_line = old_line_obj

            old_supplier_name: Optional[str] = None
            old_localization = BP_DEFAULT_VALUES['localization']

            if part_id:
                old_part = self._get_object_by_id('PartData_part_id', part_id)
                if old_part and isinstance(old_part, PartData):
                    if old_part.supplier_id:
                        supplier_id = str(old_part.supplier_id)
                        old_supplier = self._get_object_by_id('SupplierData_supplier_id', supplier_id)
                        if old_supplier and isinstance(old_supplier, SupplierData):
                            old_supplier_name = str(old_supplier.supplier_name) if old_supplier.supplier_name else None
                            old_localization = str(old_supplier.localization) if old_supplier.localization else BP_DEFAULT_VALUES['localization']

            # Prepare result with metadata for loader
            result: RecordDict = {
                **base_data,
                'supplier_id': new_supplier_id,
                'line_id': new_line_id,
                'action': 'update',
                'part_number_before_change': part_number,
                'supplier_name_before_change': record.get('supplier_name_before_change', old_supplier_name),
                'localization_before_change': record.get('localization_before_change', old_localization),
                'line_name_before_change': record.get('line_name_before_change', old_line.line_name if old_line else None),
                # Metadata for loader
                '_update_supplier': new_supplier_id is not None,
                '_update_line': new_line_id is not None,
                '_new_line_id': new_line_id,
                '_new_line_created': new_line_created,
                '_description': record.get('description')
            }

            self._stats['processed'] += 1
            self._by_action['update'] = self._by_action.get('update', 0) + 1
            return result

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _handle_update_action for part %s: %s",
                error_type, record.get('part_number'), e
            )
            self._stats['errors'] += 1
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _handle_update_action for part %s: %s",
                record.get('part_number'), unexpected_error
            )
            self._stats['errors'] += 1
            return None

    def _handle_no_data_action(
        self,
        record: RecordDict,
        base_data: RecordDict
    ) -> Optional[RecordDict]:
        """
        Handle NO DATA action - flag for manual processing.

        Args:
            record: Original record with all fields
            base_data: Base mapped data (part_id, breakpoint_id, model_id)

        Returns:
            Mapped record with metadata or None if mapping fails
        """
        logger.warning("Handling NO DATA action - requires manual intervention")

        try:
            result: RecordDict = {
                **base_data,
                'supplier_id': None,
                'line_id': None,
                'action': 'no data',
                'part_number_before_change': record.get('part_number'),
                'supplier_name_before_change': None,
                'localization_before_change': None,
                'line_name_before_change': None,
                # Metadata for loader
                '_needs_manual_review': True
            }

            self._stats['warnings'] += 1
            self._by_action['no data'] = self._by_action.get('no data', 0) + 1
            return result

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s in _handle_no_data_action for part %s: %s",
                error_type, record.get('part_number'), e
            )
            self._stats['errors'] += 1
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _handle_no_data_action for part %s: %s",
                record.get('part_number'), unexpected_error, exc_info=True
            )
            self._stats['errors'] += 1
            return None

    # ========== MAIN JUNCTION MAPPING METHOD ==========

    def _map_part_to_breakpoint(
        self,
        record: RecordDict
    ) -> Optional[RecordDict]:
        """
        Map part_to_breakpoint junction record with before-change values.

        Handles the complex composite primary key (part_id, breakpoint_id, model_id)
        and applies ACTION-based business logic.

        IMPORTANT: This function assumes data has already been validated during
        the transformation phase. It only performs ID lookups and action handling.

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
                    - line_code_before_change: Previous line code
                    - line_code_after_change: New line code
                    - line_name_after_change: New line name
                    - localization_after_change: New localization
                    - part_number_after_change: New part number
                    - description: Change description (for UPDATE actions)

        Returns:
            Dict with all fields mapped to database IDs or None if mapping fails

        Notes:
            - action and localization_before_change are already validated by enum_validator
            - before-change fields (*_before_change) are preserved as text snapshots
            - supplier_id and line_id are required (must be present after mapping)
            - Returns metadata for loader to handle specific actions
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
                value = record[col]
                if value is None or (
                    isinstance(value, str) and value.strip() == ''
                ):
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

            # Initialize base data with required fields (composite primary key)
            base_data: RecordDict = {
                'part_id': part_id,
                'breakpoint_id': breakpoint_id,
                'model_id': model_id
            }

            # Get action type
            action = str(record.get('action', 'no data')).lower().strip()

            # Route to appropriate action handler
            handler_map = {
                'replace': self._handle_replace_action,
                'delete': self._handle_delete_action,
                'add': self._handle_add_action,
                'update': self._handle_update_action,
                'no data': self._handle_no_data_action
            }

            handler = handler_map.get(action, self._handle_no_data_action)
            result = handler(record, base_data)

            if result:
                logger.debug(
                    "Successfully mapped %s record for part %s",
                    action,
                    record['part_number']
                )

            return result

        except (KeyError, ValueError, TypeError) as e:
            error_type = type(e).__name__
            logger.error(
                "%s mapping part_to_breakpoint record for part %s: %s",
                error_type, record.get('part_number'), e
            )
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping part_to_breakpoint record for part %s: %s",
                record.get('part_number'), unexpected_error, exc_info=True
            )
            return None

    # ========== SPECIALIZED MAPPING METHODS ==========

    def get_supplier_mapping(self) -> StrMappingDict:
        """Get supplier_name → supplier_id mapping."""
        cache_key = "SupplierData_supplier_name"
        if cache_key not in self._cached_mappings:
            self._load_mapping(SupplierData, 'supplier_name', 'supplier_id', cache_key)

        mapping = self._cached_mappings.get(cache_key, {})
        if isinstance(mapping, dict):
            # Filter to only string keys and values
            result: StrMappingDict = {}
            for k, v in mapping.items():
                if isinstance(k, str) and isinstance(v, str):
                    result[k] = v
            return result
        return {}

    def get_part_mapping(self) -> StrMappingDict:
        """Get part_number → part_id mapping."""
        cache_key = "PartData_part_number"
        if cache_key not in self._cached_mappings:
            self._load_mapping(PartData, 'part_number', 'part_id', cache_key)

        mapping = self._cached_mappings.get(cache_key, {})
        if isinstance(mapping, dict):
            result: StrMappingDict = {}
            for k, v in mapping.items():
                if isinstance(k, str) and isinstance(v, str):
                    result[k] = v
            return result
        return {}

    def get_model_mapping(self) -> StrMappingDict:
        """Get model_code → model_id mapping."""
        cache_key = "ModelData_model_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(ModelData, 'model_code', 'model_id', cache_key)

        mapping = self._cached_mappings.get(cache_key, {})
        if isinstance(mapping, dict):
            result: StrMappingDict = {}
            for k, v in mapping.items():
                if isinstance(k, str) and isinstance(v, str):
                    result[k] = v
            return result
        return {}

    def get_line_mapping(self) -> StrMappingDict:
        """Get line_code → line_id mapping."""
        cache_key = "LineData_line_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(LineData, 'line_code', 'line_id', cache_key)

        mapping = self._cached_mappings.get(cache_key, {})
        if isinstance(mapping, dict):
            result: StrMappingDict = {}
            for k, v in mapping.items():
                if isinstance(k, str) and isinstance(v, str):
                    result[k] = v
            return result
        return {}

    def get_breakpoint_mapping(self) -> StrMappingDict:
        """Get breakpoint_number → breakpoint_id mapping."""
        cache_key = "BreakpointData_breakpoint_number"
        if cache_key not in self._cached_mappings:
            self._load_mapping(BreakpointData, 'breakpoint_number', 'breakpoint_id', cache_key)

        mapping = self._cached_mappings.get(cache_key, {})
        if isinstance(mapping, dict):
            result: StrMappingDict = {}
            for k, v in mapping.items():
                if isinstance(k, str) and isinstance(v, str):
                    result[k] = v
            return result
        return {}

    def pre_load_all_mappings(self) -> None:
        """
        Pre-load all required mappings for breakpoint processing.

        This method loads all mappings defined in BP_LOOKUP_TABLES
        from columns_config.py to optimize performance for bulk operations.
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
        loaded_tables: List[str] = []
        failed_tables: List[str] = []
        skipped_tables: List[str] = []

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
                        logger.debug(
                            "Successfully loaded mapping for: %s",
                            table_name
                        )
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
                    "No mapping method found for table: '%s'. Available methods: %s",
                    table_name, ', '.join(mapping_methods.keys())
                )

        # Log cache statistics
        total_entries = 0
        for mapping in self._cached_mappings.values():
            if isinstance(mapping, dict):
                total_entries += len(mapping)

        # Comprehensive summary based on results
        if loaded_count == len(expected_tables):
            logger.info(
                "All breakpoint mappings pre-loaded successfully: %d/%d tables loaded, %d total entries",
                loaded_count, len(expected_tables), total_entries
            )
            if loaded_tables and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Loaded mappings for: %s",
                    ', '.join(loaded_tables)
                )

        elif loaded_count > 0:
            logger.warning(
                "Partial breakpoint mappings loaded: %d/%d tables loaded, %d failed, %d skipped, %d total entries",
                loaded_count, len(expected_tables), failed_count, skipped_count, total_entries
            )

            if loaded_tables:
                logger.info(
                    "Successfully loaded: %s",
                    ', '.join(loaded_tables)
                )
            if failed_tables:
                logger.warning(
                    "Failed to load: %s",
                    ', '.join(failed_tables)
                )
            if skipped_tables:
                logger.warning(
                    "Skipped (no mapping method): %s",
                    ', '.join(skipped_tables)
                )

        else:
            logger.error(
                "Failed to load any breakpoint mappings: 0/%d tables loaded, %d failed, %d skipped",
                len(expected_tables), failed_count, skipped_count
            )

            if failed_tables:
                logger.error(
                    "Failed tables: %s",
                    ', '.join(failed_tables)
                )
            if skipped_tables:
                logger.error(
                    "Skipped tables (no mapping method): %s",
                    ', '.join(skipped_tables)
                )

            # Provide troubleshooting information
            missing_methods = [t for t in expected_tables if t not in mapping_methods]
            if missing_methods:
                logger.error(
                    "Missing mapping methods for: %s. Add to mapping_methods dictionary.",
                    missing_methods
                )

    def map_breakpoint_records(
        self,
        records: List[RecordDict],
        junction_type: str
    ) -> List[RecordDict]:
        """
        Map breakpoint junction records with before/after value support.

        Args:
            records: List of dictionaries with breakpoint junction data
                    (already transformed and validated)
            junction_type: Type of junction table to map
                (currently only 'part_to_breakpoint' supported)

        Returns:
            List of dicts with database IDs ready for insertion

        Examples:
            >>> records = mapper.map_breakpoint_records(record_dicts, 'part_to_breakpoint')
            >>> print(f"Successfully mapped {len(records)} records")
        """
        logger.info(
            "Mapping breakpoint records for: %s",
            junction_type
        )

        if not records:
            logger.info("No records to map for %s", junction_type)
            return []

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

        # Reset stats for this batch if configured
        if BP_LOGGING_CONFIG.get('reset_stats_per_batch', False):
            self._stats = {
                'processed': 0,
                'created_parts': 0,
                'created_lines': 0,
                'created_suppliers': 0,
                'errors': 0,
                'warnings': 0,
                'skipped': 0
            }
            self._by_action = {}

        mapped_records: List[RecordDict] = []
        skipped = 0
        skipped_reasons: Dict[str, int] = {}

        for idx, record in enumerate(records):
            try:
                mapped_record = handler(record)
                if mapped_record:
                    mapped_records.append(mapped_record)
                else:
                    skipped += 1
                    self._stats['skipped'] += 1

                    # Track reason for skipping (first few records only)
                    if skipped <= 10:
                        # Determine reason for skip
                        missing_fields = []
                        for req_field in BP_JUNCTION_REQUIRED['part_to_breakpoint']:
                            field_value = record.get(req_field)
                            if req_field not in record or field_value is None or (
                                isinstance(field_value, str) and field_value.strip() == ''
                            ):
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
                            else:
                                reason = "Unknown mapping failure"

                        skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1

            except KeyError as e:
                logger.error(
                    "KeyError for record %d: missing key '%s' in record: %s",
                    idx, e, record
                )
                skipped += 1
                self._stats['errors'] += 1
                skipped_reasons[f"KeyError: {e}"] = skipped_reasons.get(f"KeyError: {e}", 0) + 1

            except (ValueError, TypeError, AttributeError) as e:
                error_type = type(e).__name__
                logger.error(
                    "%s for record %d: %s",
                    error_type, idx, e
                )
                logger.debug("Problematic record: %s", record)
                skipped += 1
                self._stats['errors'] += 1
                skipped_reasons[f"{error_type}: {e}"] = skipped_reasons.get(f"{error_type}: {e}", 0) + 1

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error in handler for record %d: %s, record: %s",
                    idx, unexpected_error, record
                )
                skipped += 1
                self._stats['errors'] += 1

        # Logging statistics
        logger.info(
            "Breakpoint mapping completed for '%s': total=%d, mapped=%d, skipped=%d",
            junction_type, len(records), len(mapped_records), skipped
        )

        # Log skipped reasons summary
        if skipped_reasons:
            logger.info(
                "Skip reasons summary: %s",
                skipped_reasons
            )

        # Log action distribution
        if self._by_action:
            logger.info(
                "Action distribution: %s",
                self._by_action
            )

        # Debug information
        if mapped_records and BP_LOGGING_CONFIG.get('log_sample_records', True) and logger.isEnabledFor(logging.DEBUG):
            sample_size = min(5, len(mapped_records))
            sample = mapped_records[:sample_size]
            logger.debug(
                "First %d mapped records: %s",
                sample_size, sample
            )

        return mapped_records

    # ========== UTILITY METHODS ==========

    def get_temp_records(self) -> TempRecordsDict:
        """
        Get all temporary records created during mapping.
        Used by loader to know what needs to be committed.

        Returns:
            Dictionary with lists of temporary records by type
        """
        return self._temp_records

    def get_stats(self) -> StatsDict:
        """
        Get current mapping statistics.
        
        Returns:
            Dictionary with mapping statistics
        """
        return self._stats.copy()

    def get_action_stats(self) -> Dict[str, int]:
        """
        Get statistics broken down by action type.

        Returns:
            Dictionary with action statistics
        """
        return self._by_action.copy()

    def clear_temp_records(self) -> None:
        """Clear temporary records (call after successful load)."""
        self._temp_records = {
            'parts': [],
            'lines': [],
            'suppliers': []
        }
        logger.debug("Temporary records cleared")

    def log_mapping_statistics(self) -> int:
        """
        Log statistics about all loaded mappings.

        Returns:
            int: Total number of cached mapping entries
        """
        try:
            total_entries = 0

            logger.info("=" * 60)
            logger.info("BREAKPOINT MAPPING STATISTICS")
            logger.info("=" * 60)

            for cache_key, mapping in sorted(self._cached_mappings.items()):
                if isinstance(mapping, dict):
                    count = len(mapping)
                    total_entries += count
                    logger.info("%s: %d entries", cache_key, count)

            logger.info("-" * 60)
            logger.info(
                "Total cached mappings: %d entries",
                total_entries
            )

            # Log temporary records stats
            if any(self._temp_records.values()):
                logger.info("-" * 60)
                logger.info("TEMPORARY RECORDS CREATED:")
                logger.info("  Parts: %d", len(self._temp_records['parts']))
                logger.info("  Lines: %d", len(self._temp_records['lines']))
                logger.info("  Suppliers: %d", len(self._temp_records['suppliers']))

            # Log processing stats
            if any(v for v in self._stats.values()):
                logger.info("-" * 60)
                logger.info("PROCESSING STATISTICS:")
                logger.info("  Processed: %d", self._stats['processed'])
                logger.info("  Created parts: %d", self._stats['created_parts'])
                logger.info("  Created lines: %d", self._stats['created_lines'])
                logger.info("  Created suppliers: %d", self._stats['created_suppliers'])
                logger.info("  Errors: %d",self._stats['errors'])
                logger.info("  Warnings: %d", self._stats['warnings'])
                logger.info("  Skipped: %d", self._stats['skipped'])

                if self._by_action:
                    logger.info("  By action: %s",self._by_action)

            logger.info("=" * 60)

            return total_entries

        except KeyError as e:
            logger.error(
                "KeyError while logging mapping statistics: %s", e
            )
            return 0

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Data error while logging mapping statistics: %s", e
            )
            return 0

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error while logging mapping statistics: %s",
                unexpected_error, exc_info=True
            )
            return 0

    def clear_cache(self) -> None:
        """Clear cached mappings to free memory."""
        cache_size = 0
        for mapping in self._cached_mappings.values():
            if isinstance(mapping, dict):
                cache_size += len(mapping)

        logger.info(
            "Clearing breakpoint cache with %d total entries.",
            cache_size
        )

        self._cached_mappings.clear()
        logger.debug("Breakpoint cache cleared.")

    def validate_mappings(
        self,
        breakpoint_records: List[RecordDict],
        ptb_records: List[RecordDict]
    ) -> Tuple[bool, List[str]]:
        """
        Validate mappings before passing to loader.
        This is pre-loading validation only.

        Args:
            breakpoint_records: List of dictionaries with breakpoint data
            ptb_records: List of dictionaries with mapped part_to_breakpoint data

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        try:
            errors: List[str] = []

            # Check for missing breakpoint_ids
            missing_bp = [r for r in breakpoint_records if not r.get('breakpoint_id')]
            if missing_bp:
                bp_numbers = [r.get('BREAKPOINT_NUMBER') for r in missing_bp if r.get('BREAKPOINT_NUMBER')]
                errors.append(f"Missing breakpoint_ids for: {bp_numbers}")

            # Check part_to_breakpoint for required fields
            if ptb_records:
                for field in BP_VALIDATION_RULES['required_always']:
                    missing = [r for r in ptb_records if not r.get(field)]
                    if missing:
                        errors.append(f"Missing {field} in {len(missing)} records")

            is_valid = len(errors) == 0
            if is_valid:
                logger.info("All mappings validated successfully")
            else:
                logger.error(
                    "Validation failed with %d errors",
                    len(errors)
                )

            return is_valid, errors

        except KeyError as e:
            logger.error(
                "Configuration error in validate_mappings: missing key %s", e
            )
            return False, [f"Configuration error: {e}"]

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Data error in validate_mappings: %s", e
            )
            return False, [f"Data error: {e}"]

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in validate_mappings: %s",
                unexpected_error
            )
            return False, [f"Unexpected error: {unexpected_error}"]

def create_bp_mapper(engine: Optional[Engine] = None) -> BreakpointObjectMapper:
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
        logger.error(
            "Database error creating breakpoint mapper: %s",
            e
        )
        raise

    except (ValueError, TypeError, AttributeError) as e:
        logger.error(
            "Configuration error creating breakpoint mapper: %s",
            e
        )
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
