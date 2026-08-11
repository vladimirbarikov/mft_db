# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Breakpoint Object Mapping Module for Material Flow Table Database.

This module provides comprehensive functionality for mapping external object
identifiers (text values, composite keys) to database primary keys for the
Breakpoint (BP) pipeline. It serves as a bridge between transformed BP data
(which contains text references) and the database (which uses UUIDs), enabling
referential integrity during data loading.

Key Features:
    - Text-to-ID mapping for BP-related entities (breakpoints, parts, models, etc.)
    - Composite key handling for packaging entities (boxes, pallets)
    - Memory-efficient caching of ID mappings
    - Support for multiple input formats (dict, tuple, list)
    - Consistent packaging number generation matching database triggers
    - Independent from MFT mapper (no circular imports)

Configuration Source:
    This module uses constants from columns_config.py:
        - BP_JUNCTION_REQUIRED: Required columns for BP junction table
        - BP_JUNCTION_OPTIONAL: Optional columns for BP junction table

Architecture:
    The module follows a caching-first approach:
    1. On-demand loading of ID mappings from database
    2. In-memory caching for high-performance lookups
    3. Automatic cache management with manual clearing capability
    
    Composite key processing matches exactly what database.py event handlers
    do, ensuring consistent packaging number generation across the system.

ARCHITECTURE NOTES:

    BP Pipeline follows the ETL (Extract-Transform-Load) pattern:
    
    1. EXTRACT: Raw data from Excel files
    2. TRANSFORM: Data cleaning, validation, and preparation
    3. MAP: Text-to-ID conversion (this module)
    4. LOAD: Database insertion with business logic (bp_loader.py)
    
    Key Characteristics of BP Mapper:
    
    - READ-ONLY OPERATIONS: Only performs ID lookups, never modifies database
    - NO BUSINESS LOGIC: Action determination, versioning, and soft deletes are in bp_loader.py
    - NO RECORD CREATION: Does not create new parts, suppliers, or lines
    - SIMPLE MAPPING: Converts text values (part_number) to IDs (part_id)
    - OPTIONAL FIELDS HANDLING: Copies optional fields as-is from transformer
    
    Why BP Mapper Has No Business Logic:
    
    - Business logic (ADD/DELETE/UPDATE/REPLACE) belongs in loader
    - Version creation and soft deactivation are loader responsibilities
    - Mapper's sole responsibility is ID resolution
    - Clean separation of concerns enables easier testing and maintenance
    
    Example of BP Mapper's Simplicity:
    
        Input from transformer: {
            'part_no_before': 'ABC-123',                 # Text reference
            'part_no_after': 'ABC-124',                  # Text reference
            'bp_no': 'BP-2026-001',                      # Text reference
            'bom_product': 'jolion',                     # Text reference (model)
            'supplier_name_before': 'Acme Corp',
            'box_before': ('returnable', 400, 300, 200)  # Composite key
        }
        
        Output from mapper: {
            'old_part_id': '123e4567...',      # UUID from part_number lookup
            'new_part_id': '123e4567...',      # UUID from part_number lookup
            'breakpoint_id': '123e4567...',    # UUID from bp_no lookup
            'model_id': '123e4567...',         # UUID from model_code lookup
            'supplier_id_before': '123e4567...',
            'box_id_before': '123e4567...'     # UUID from composite lookup
        }
    
    This simplicity enables:
        - Easy testing (mock session, no complex logic)
        - High performance (pure ID lookups)
        - Clear separation of concerns
        - Predictable behavior

Dependencies:
    - SQLAlchemy 1.4.54+ for ORM and database abstraction
    - PostgreSQL 12+ as the source database for ID lookups

Performance Considerations:
    - Lazy loading: Mappings are loaded on first use
    - Memory caching: Reduces database queries for repeated lookups
    - Bulk loading: Pre-load methods for performance-critical operations
    - Cache statistics: Built-in logging for performance monitoring

Security Notes:
    - Read-only operations only (no data modification)
    - Input validation for composite keys
    - Safe handling of NULL/None values
    - No SQL injection risk (uses ORM with parameterized queries)

Usage Example:
    from dags.tasks.bp_mapper import create_bp_mapper

Create mapper (after core entity tables are loaded)
    - mapper = create_bp_mapper()

Standard text-to-ID mapping
    - breakpoint_id = mapper.get_breakpoint_id('BP-2026-001')
    - part_id = mapper.get_part_id_by_number('ABC-123')
    - model_id = mapper.get_model_id_by_code('jolion')

Composite key mapping for packaging
    - box_id = mapper.get_box_id_by_dimensions('returnable', 400, 300, 200)
    - pallet_id = mapper.get_pallet_id_by_dimensions('returnable', 1200, 800, 150)

Get mapping dictionaries for bulk operations
    - mapper.get_breakpoint_mapping()
    - mapper.get_part_mapping()
    - mapper.get_box_mapping()

Check mapping statistics
    - mapper.log_mapping_statistics()

Clear cache when done with bulk operations
    - mapper.clear_cache()

Module Structure:
    - BPObjectMapper: Main mapper class with comprehensive mapping capabilities
    - create_bp_mapper(): Factory function for mapper creation
    - Column-to-model mapping configuration (COLUMN_TO_MODEL)
    - Composite key handling utilities (_get_composite_id, _load_composite_mapping)
    - Caching system with configurable invalidation

Composite Key Handling:
    - Boxes: Maps (type, length, width, height) → box_id using composite mapping
    - Pallets: Maps (type, length, width, height) → pallet_id using composite mapping
    - Composite keys are processed as tuples: (type, length, width, height)

The mapping logic mirrors database structure:
    - Queries BoxData/PalletData directly using dimension columns
    - Creates tuple keys for in-memory lookup
    - No string formatting required (unlike database's Computed columns)

The generation logic matches database triggers exactly:
    - 'returnable' → 'R L-W-H'
    - 'non-returnable' → 'N L-W-H'

Error Handling:
    - Graceful handling of missing mappings (returns None with debug logging)
    - Input validation for composite keys (type, dimensions, null checks)
    - Comprehensive logging at DEBUG/INFO/WARNING levels
    - No exceptions raised for missing data (only warnings)
    - Exception catching with detailed error logging

Integration Notes:
    - Must be created AFTER core entity tables are loaded (depends on their IDs)
    - Used primarily by bp_loader.py for breakpoint junction processing
    - Cache should be cleared after bulk operations to free memory
    - Session is maintained for the lifetime of the mapper
    - Designed for read-heavy, write-light scenarios

Comparison with MFT Mapper:
    | Aspect              | MFT Mapper (ETL)            | BP Mapper (CDC)              |
    |---------------------|-----------------------------|------------------------------|
    | Primary Purpose     | ID lookup only              | ID lookup only               |
    | Creates new records | No (MFT loader does this)   | No (BP loader does this)     |
    | Modifies data       | No (MFT loader does this)   | No (BP loader does this)     |
    | Business logic      | None (MFT loader does this) | None (BP loader does this)   |
    | Optional fields     | Simple copy                 | Simple copy                  |
    | Time series support | No                          | No                           |
    | Complexity          | Low                         | Low (same as MFT)            |
    | Entity types        | MFT entities                | BP entities                  |

Version: 1.0.0
Compatibility: Python 3.14.4+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2026-08-11
Last Modified: 2026-08-11
License: MIT
Status: Development
"""
# Standard library imports
from pathlib import Path
import sys
from typing import Any, Optional, Union

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
    BP_JUNCTION_OPTIONAL
)
from dags.tasks.connector import initialize_database
from database.database import (
    SupplierData, PartData, BoxData, PalletData,
    ConfigurationData, ModelData, WorkshopData, LineData,
    BreakpointData
)

# Logger setup
logger = get_logger(__name__)


class BPObjectMapper:
    """
    Main mapper class for converting BP external identifiers to database primary keys.
    
    Provides unified interface for mapping text values and composite keys to
    database IDs with intelligent caching.
    
    Examples:
        >>> mapper = create_bp_mapper()
        >>> breakpoint_id = mapper.get_breakpoint_id('BP-2026-001')
        >>> part_id = mapper.get_part_id_by_number('ABC-123')
        >>> box_id = mapper.get_box_id_by_dimensions('returnable', 400, 300, 200)
    """
    # Configuration with table names
    COLUMN_TO_MODEL = {
        # Supplier mappings
        'supplier_name': (SupplierData, 'supplier_name', 'supplier_id'),
        'supplier_id': (SupplierData, 'supplier_id', 'supplier_id'),

        # Part mappings
        'part_number': (PartData, 'part_number', 'part_id'),
        'part_id': (PartData, 'part_id', 'part_id'),

        # Box mappings
        'box_number': (BoxData, 'box_number', 'box_id'),
        'box_id': (BoxData, 'box_id', 'box_id'),

        # Composite key mapping
        'box_composite': ('box_composite', 'box_type_length_width_height', 'box_id'),

        # Pallet mappings
        'pallet_number': (PalletData, 'pallet_number', 'pallet_id'),
        'pallet_id': (PalletData, 'pallet_id', 'pallet_id'),

        # Composite key mapping
        'pallet_composite': ('pallet_composite', 'pallet_type_length_width_height', 'pallet_id'),

        # Model mappings
        'model_code': (ModelData, 'model_code', 'model_id'),
        'model_id': (ModelData, 'model_id', 'model_id'),

        # Configuration mappings
        'configuration': (ConfigurationData, 'configuration', 'configuration_id'),
        'configuration_id': (ConfigurationData, 'configuration_id', 'configuration_id'),

        # Workshop mappings
        'workshop_code': (WorkshopData, 'workshop_code', 'workshop_id'),
        'workshop_id': (WorkshopData, 'workshop_id', 'workshop_id'),

        # Line mappings
        'line_code': (LineData, 'line_code', 'line_id'),
        'line_id': (LineData, 'line_id', 'line_id'),

        # Breakpoint mappings
        'breakpoint_number': (BreakpointData, 'breakpoint_number', 'breakpoint_id'),
        'breakpoint_id': (BreakpointData, 'breakpoint_id', 'breakpoint_id'),
    }

    # Composite key types supported by this mapper
    COMPOSITE_KEY_TYPES = {'box_composite', 'pallet_composite'}

    def __init__(
            self,
            session: Session
        ):
        """
        Initialize BP mapper with database session.
        
        Args:
            session: SQLAlchemy Session for database operations
        
        Example:
            >>> session_factory = sessionmaker(bind=engine)
            >>> mapper = BPObjectMapper(session_factory())
        """
        self.session = session
        self._cached_mappings = {}
        logger.debug("BPObjectMapper initialized with composite key support")

    def get_id(
            self,
            column_name: str,
            value: Any
        ) -> Optional[str]:
        """
        Get database ID for given column value.
        
        Handles both standard text values and composite keys.
        
        Args:
            column_name: Column to look up ('part_number', 'box_composite', etc.)
            value: Value to look up (string for standard, tuple/dict for composite)
                
        Returns:
            Database ID as string or None if not found
            
        Examples:
            >>> mapper.get_id('part_number', 'ABC-123')
            >>> mapper.get_id('box_composite', ('returnable', 400, 300, 200))
            >>> mapper.get_id('box_composite', {'type': 'returnable', 'length': 400, 
            ...                                 'width': 300, 'height': 200})
        """
        if not value:
            return None

        # Handle composite keys for packaging
        if column_name in self.COMPOSITE_KEY_TYPES:
            return self._get_composite_id(column_name, value)

        # Standard lookup for non-composite columns
        if column_name not in self.COLUMN_TO_MODEL:
            logger.warning(
                "Unknown column for mapping: %s",
                column_name
            )
            return None

        model_class, lookup_column, id_column = self.COLUMN_TO_MODEL[column_name]

        # Special case for composite entries in column_to_model
        if model_class in ['box_composite', 'pallet_composite']:
            logger.warning("Use 'box_composite' or 'pallet_composite' for composite lookups.")
            return None

        # Get or create mapping
        cache_key = f"{model_class.__name__}_{lookup_column}"
        if cache_key not in self._cached_mappings:
            self._load_mapping(model_class, lookup_column, id_column, cache_key)

        mapping = self._cached_mappings[cache_key]
        return mapping.get(str(value))

    def _get_composite_id(
            self,
            column_name: str,
            value: Union[tuple, list, dict]
        ) -> Optional[str]:
        """
        Get database ID for composite packaging key.
        
        Args:
            column_name: 'box_composite' or 'pallet_composite'
            value: (type, length, width, height) or dict with keys
                
        Returns:
            Database ID or None
        """
        try:
            # Parse composite value
            pack_type, length, width, height = None, None, None, None

            if isinstance(value, dict):
                pack_type = value.get('type')
                length = value.get('length')
                width = value.get('width')
                height = value.get('height')

            elif isinstance(value, (tuple, list)):
                if len(value) != 4:
                    logger.debug(
                        "Composite key must have 4 elements, got %d",
                        len(value)
                    )
                    return None

                pack_type, length, width, height = value

            else:
                logger.debug("Invalid composite value type: %s", type(value))
                return None

            # Validate all components are present
            if not all([pack_type, length, width, height]):
                logger.warning(
                    "Incomplete composite key: type=%s, length=%s, width=%s, height=%s",
                    pack_type, length, width, height
                )
                return None

            # Normalize type
            pack_type = str(pack_type).strip().lower()

            # Skip if type is 'null'
            if pack_type == 'null':
                logger.debug("Skipping 'null' type.")
                return None

            # Assert that the values are not None after verification
            assert length is not None and width is not None and height is not None

            # Convert dimensions to integers
            try:
                length_val = int(float(str(length))) if '.' in str(length) else int(length)
                width_val = int(float(str(width))) if '.' in str(width) else int(width)
                height_val = int(float(str(height))) if '.' in str(height) else int(height)

            except (ValueError, TypeError) as e:
                logger.debug(
                    "Cannot convert dimensions to int:\n"
                    "type=%s, length=%s, width=%s, height=%s, error=%s",
                    pack_type, length, width, height, e
                )
                return None

            # Validate packaging type
            if pack_type not in ['returnable', 'non-returnable']:
                logger.debug(
                    "Invalid packaging type: %s (must be 'returnable' or 'non-returnable').",
                    pack_type
                )
                return None

            # Create composite key as tuple for lookup
            composite_key = (pack_type, length_val, width_val, height_val)

            # Look up in appropriate mapping
            if column_name == 'box_composite':
                cache_key = "BoxData_composite"

                if cache_key not in self._cached_mappings:
                    self._load_composite_mapping(BoxData, cache_key)

                box_mapping = self._cached_mappings[cache_key]
                box_id = box_mapping.get(str(composite_key))

                if box_id:
                    logger.debug(
                        "Found box_id %s for composite %s",
                        box_id, composite_key
                    )
                else:
                    logger.debug(
                        "No box found for composite %s",
                        composite_key
                    )

                return box_id

            elif column_name == 'pallet_composite':
                cache_key = "PalletData_composite"

                if cache_key not in self._cached_mappings:
                    self._load_composite_mapping(PalletData, cache_key)

                pallet_mapping = self._cached_mappings[cache_key]
                pallet_id = pallet_mapping.get(str(composite_key))

                if pallet_id:
                    logger.debug(
                        "Found pallet_id %s for composite %s",
                        pallet_id, composite_key
                    )
                else:
                    logger.debug(
                        "No pallet found for composite %s",
                        composite_key
                    )

                return pallet_id

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Error processing composite key %s: %s",
                value, e
            )
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error processing composite key %s: %s",
                value, unexpected_error
            )
            return None

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
                    mapping[str(lookup_val)] = db_id

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

    def _load_composite_mapping(
            self,
            model_class,
            cache_key: str
        ):
        """
        Special method for loading composite mappings.
        
        REQUIRED because box_number and pallet_number are computed columns
        in database.py and cannot be queried directly.
        
        Args:
            model_class: BoxData or PalletData
            cache_key: Key to store the mapping in cache
        """
        logger.debug(
            "Loading composite mapping for %s",
            model_class.__name__
        )

        try:
            mapping = {}

            if model_class == BoxData:
                # Load box data using tuple (type, length, width, height)
                query = self.session.query(
                    BoxData.box_type,
                    BoxData.box_length_mm,
                    BoxData.box_width_mm,
                    BoxData.box_height_mm,
                    BoxData.box_id
                ).all()

                for box_type, length, width, height, box_id in query:
                    if all([box_type, length, width, height]):
                        # Create tuple key
                        key = (box_type, int(length), int(width), int(height))
                        mapping[str(key)] = box_id

            elif model_class == PalletData:
                # Load pallet data using tuple (type, length, width, height)
                query = self.session.query(
                    PalletData.pallet_type,
                    PalletData.pallet_length_mm,
                    PalletData.pallet_width_mm,
                    PalletData.pallet_height_mm,
                    PalletData.pallet_id
                ).all()

                for pallet_type, length, width, height, pallet_id in query:
                    if all([pallet_type, length, width, height]):
                        # Create tuple key
                        key = (pallet_type, int(length), int(width), int(height))
                        mapping[str(key)] = pallet_id

            self._cached_mappings[cache_key] = mapping

            total_count = len(mapping)
            logger.info(
                "Loaded composite mapping %s: %d entries",
                cache_key, total_count
            )

        except SQLAlchemyError as e:
            logger.error(
                "Database error loading composite mapping for %s: %s",
                cache_key, e
            )
            self._cached_mappings[cache_key] = {}
        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Data error loading composite mapping for %s: %s",
                cache_key, e
            )
            self._cached_mappings[cache_key] = {}
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error loading composite mapping for %s: %s",
                cache_key, unexpected_error
            )
            self._cached_mappings[cache_key] = {}

    # ========== SPECIALIZED GETTER METHODS FOR BP ==========
    def get_breakpoint_id(self, breakpoint_number: str) -> Optional[str]:
        """
        Get breakpoint_id from breakpoint_number.
        
        Args:
            breakpoint_number: Breakpoint number (e.g., 'BP-2026-001')
            
        Returns:
            breakpoint_id as string or None if not found
        """
        return self.get_id('breakpoint_number', breakpoint_number)

    def get_part_id_by_number(self, part_number: str) -> Optional[str]:
        """
        Get part_id from part_number.
        
        Args:
            part_number: Part number (e.g., 'ABC-123')
            
        Returns:
            part_id as string or None if not found
        """
        return self.get_id('part_number', part_number)

    def get_model_id_by_code(self, model_code: str) -> Optional[str]:
        """
        Get model_id from model_code.
        
        Args:
            model_code: Model code (e.g., 'jolion', 'h3', 'f7')
            
        Returns:
            model_id as string or None if not found
        """
        return self.get_id('model_code', model_code)

    def get_supplier_id_by_name(self, supplier_name: str) -> Optional[str]:
        """
        Get supplier_id from supplier_name.
        
        Args:
            supplier_name: Supplier name (e.g., 'Acme Corp')
            
        Returns:
            supplier_id as string or None if not found
        """
        return self.get_id('supplier_name', supplier_name)

    def get_workshop_id_by_code(self, workshop_code: str) -> Optional[str]:
        """
        Get workshop_id from workshop_code.
        
        Args:
            workshop_code: Workshop code (e.g., 'as', 'comp', 'paint')
            
        Returns:
            workshop_id as string or None if not found
        """
        return self.get_id('workshop_code', workshop_code)

    def get_line_id_by_code(self, line_code: str) -> Optional[str]:
        """
        Get line_id from line_code.
        
        Args:
            line_code: Line code (e.g., 'L001', 'L002')
            
        Returns:
            line_id as string or None if not found
        """
        return self.get_id('line_code', line_code)

    def get_configuration_id(self, configuration: str) -> Optional[str]:
        """
        Get configuration_id from configuration name.
        
        Args:
            configuration: Configuration name (e.g., 'comfort', 'elite')
            
        Returns:
            configuration_id as string or None if not found
        """
        return self.get_id('configuration', configuration)

    def get_box_id_by_dimensions(
            self,
            box_type: str,
            length_mm: int,
            width_mm: int,
            height_mm: int
        ) -> Optional[str]:
        """
        Get box_id from box dimensions.
        
        Args:
            box_type: 'returnable' or 'non-returnable'
            length_mm: Length in millimeters
            width_mm: Width in millimeters
            height_mm: Height in millimeters
            
        Returns:
            box_id as string or None if not found
        """
        composite_key = (box_type, length_mm, width_mm, height_mm)
        return self.get_id('box_composite', composite_key)

    def get_pallet_id_by_dimensions(
            self,
            pallet_type: str,
            length_mm: int,
            width_mm: int,
            height_mm: int
        ) -> Optional[str]:
        """
        Get pallet_id from pallet dimensions.
        
        Args:
            pallet_type: 'returnable' or 'non-returnable'
            length_mm: Length in millimeters
            width_mm: Width in millimeters
            height_mm: Height in millimeters
            
        Returns:
            pallet_id as string or None if not found
        """
        composite_key = (pallet_type, length_mm, width_mm, height_mm)
        return self.get_id('pallet_composite', composite_key)

    # ========== BULK MAPPING METHODS ==========
    def get_breakpoint_mapping(self) -> dict[str, Any]:
        """Get breakpoint_number → breakpoint_id mapping."""
        cache_key = "BreakpointData_breakpoint_number"
        if cache_key not in self._cached_mappings:
            self._load_mapping(BreakpointData, 'breakpoint_number', 'breakpoint_id', cache_key)
        return self._cached_mappings[cache_key]

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

    def get_box_mapping(self) -> dict[str, Any]:
        """Get box dimensions → box_id mapping (composite)."""
        cache_key = "BoxData_composite"
        if cache_key not in self._cached_mappings:
            self._load_composite_mapping(BoxData, cache_key)
        return self._cached_mappings[cache_key]

    def get_pallet_mapping(self) -> dict[str, Any]:
        """Get pallet dimensions → pallet_id mapping (composite)."""
        cache_key = "PalletData_composite"
        if cache_key not in self._cached_mappings:
            self._load_composite_mapping(PalletData, cache_key)
        return self._cached_mappings[cache_key]

    def get_model_mapping(self) -> dict[str, Any]:
        """Get model_code → model_id mapping."""
        cache_key = "ModelData_model_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(ModelData, 'model_code', 'model_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_configuration_mapping(self) -> dict[str, Any]:
        """Get configuration → configuration_id mapping."""
        cache_key = "ConfigurationData_configuration"
        if cache_key not in self._cached_mappings:
            self._load_mapping(ConfigurationData, 'configuration', 'configuration_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_workshop_mapping(self) -> dict[str, Any]:
        """Get workshop_code → workshop_id mapping."""
        cache_key = "WorkshopData_workshop_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(WorkshopData, 'workshop_code', 'workshop_id', cache_key)
        return self._cached_mappings[cache_key]

    def get_line_mapping(self) -> dict[str, Any]:
        """Get line_code → line_id mapping."""
        cache_key = "LineData_line_code"
        if cache_key not in self._cached_mappings:
            self._load_mapping(LineData, 'line_code', 'line_id', cache_key)
        return self._cached_mappings[cache_key]

    # ========== JUNCTION MAPPING METHODS ==========
    def map_breakpoint_records(
        self,
        bp_df,
        junction_type: str = 'part_to_breakpoint'
    ) -> list[dict[str, Any]]:
        """
        Map breakpoint junction records with composite packaging support.
        
        Args:
            bp_df: Polars DataFrame with breakpoint junction data
            junction_type: Type of junction table to map (default: 'part_to_breakpoint')
                
        Returns:
            List of dicts with database IDs ready for insertion
            
        Examples:
            >>> records = mapper.map_breakpoint_records(df)
        """
        logger.info(
            "Mapping breakpoint junction records for: %s",
            junction_type
        )

        if junction_type != 'part_to_breakpoint':
            logger.error(
                "Unknown junction type: %s. Only 'part_to_breakpoint' is supported.",
                junction_type
            )
            return []

        try:
            records = bp_df.to_dicts()
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

        for record in records:
            try:
                mapped_record = self._map_breakpoint_record(record)
                if mapped_record:
                    mapped_records.append(mapped_record)
                else:
                    skipped += 1
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error in handler for record %s: %s",
                    record, unexpected_error
                )
                skipped += 1

        # Logging statistics
        logger.info(
            "Breakpoint junction mapping completed: total=%d, mapped=%d, skipped=%d",
            len(records), len(mapped_records), skipped
        )

        # Debug information
        if mapped_records:
            sample_size = min(5, len(mapped_records))
            sample = mapped_records[:sample_size]
            logger.debug("First %d mapped records: %s", sample_size, sample)

        return mapped_records

    def _map_breakpoint_record(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map a single breakpoint junction record.
        
        Args:
            record: Dict with part_no_before, part_no_after, bp_no, bom_product,
                and optional fields (supplier_name_before, supplier_name_after, etc.)
        
        Returns:
            Dict with breakpoint_id, old_part_id, new_part_id, model_id,
            and optional IDs (supplier_id_before, supplier_id_after, etc.)
            or None if mapping fails
        """
        try:
            # Check for all required columns
            required_cols = BP_JUNCTION_REQUIRED['part_to_breakpoint']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in breakpoint record",
                        col
                    )
                    return None

            # Get breakpoint_id from bp_no
            breakpoint_id = self.get_breakpoint_id(record['bp_no'])
            if not breakpoint_id:
                logger.warning(
                    "No breakpoint_id found for bp_no: %s",
                    record['bp_no']
                )
                return None

            # Get model_id from bom_product (model_code)
            model_id = self.get_model_id_by_code(record['bom_product'])
            if not model_id:
                logger.warning(
                    "No model_id found for bom_product: %s",
                    record['bom_product']
                )
                return None

            # Get old_part_id from part_no_before (if present)
            old_part_id = None
            if record.get('part_no_before'):
                old_part_id = self.get_part_id_by_number(record['part_no_before'])
                if not old_part_id:
                    logger.warning(
                        "No part_id found for part_no_before: %s",
                        record['part_no_before']
                    )
                    # Don't return None - old_part can be NULL for ADD

            # Get new_part_id from part_no_after (if present)
            new_part_id = None
            if record.get('part_no_after'):
                new_part_id = self.get_part_id_by_number(record['part_no_after'])
                if not new_part_id:
                    logger.warning(
                        "No part_id found for part_no_after: %s",
                        record['part_no_after']
                    )
                    # Don't return None - new_part can be NULL for DELETE

            # Create result with required fields
            result = {
                'breakpoint_id': breakpoint_id,
                'model_id': model_id,
                'old_part_id': old_part_id,
                'new_part_id': new_part_id
            }

            # Map optional fields using configuration
            optional_cols = BP_JUNCTION_OPTIONAL['part_to_breakpoint']

            # Define mapping rules for optional fields
            # Format: (source_field, target_field, mapping_function)
            optional_mappings = [
                # Supplier mappings
                ('supplier_name_before', 'supplier_id_before', self.get_supplier_id_by_name),
                ('supplier_name_after', 'supplier_id_after', self.get_supplier_id_by_name),

                # Line mappings (workcenter)
                ('workcenter_no_before', 'line_id_before', self.get_line_id_by_code),
                ('workcenter_no_after', 'line_id_after', self.get_line_id_by_code),

                # Workcenter names (pass through)
                ('workcenter_name_before', 'workcenter_name_before', None),
                ('workcenter_name_after', 'workcenter_name_after', None),

                # Workshop mappings
                ('workshop_before', 'workshop_id_before', self.get_workshop_id_by_code),
                ('workshop_after', 'workshop_id_after', self.get_workshop_id_by_code),

                # Localization (store as-is for validation)
                ('localization_before', 'localization_before', None),
                ('localization_after', 'localization_after', None),

                # Box mappings (composite keys)
                ('box_before', 'box_id_before', self._get_box_id_from_record),
                ('box_after', 'box_id_after', self._get_box_id_from_record),

                # Pallet mappings (composite keys)
                ('pallet_before', 'pallet_id_before', self._get_pallet_id_from_record),
                ('pallet_after', 'pallet_id_after', self._get_pallet_id_from_record),

                # Additional pass-through fields
                ('disposal', 'disposal', None),
                ('interchangeable', 'interchangeable', None),
            ]

            # Process each optional mapping
            for source_field, target_field, mapping_func in optional_mappings:
                # Check if field exists in record and is not None/empty
                if source_field in optional_cols and record.get(source_field):
                    value = record[source_field]

                    if mapping_func:
                        # Apply mapping function to get ID
                        mapped_value = mapping_func(value)
                        if mapped_value:
                            result[target_field] = mapped_value
                        else:
                            logger.debug(
                                "No %s found for %s: %s",
                                target_field, source_field, value
                            )
                    else:
                        # Pass through value (for localization, disposal, interchangeable)
                        result[target_field] = value

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping breakpoint record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping breakpoint record: %s",
                unexpected_error
            )
            return None

    def _get_box_id_from_record(self, box_data: Any) -> Optional[str]:
        """
        Extract box_id from various box data formats.
        
        Args:
            box_data: Can be tuple (type, length, width, height),
                    dict with keys, or None
                
        Returns:
            box_id or None
        """
        if not box_data:
            return None

        try:
            if isinstance(box_data, (tuple, list)) and len(box_data) == 4:
                box_type, length, width, height = box_data
                # Ensure all values are not None and have correct types
                if all([box_type, length is not None, width is not None, height is not None]):
                    return self.get_box_id_by_dimensions(
                        str(box_type), 
                        int(length), 
                        int(width), 
                        int(height)
                    )

            elif isinstance(box_data, dict):
                box_type = box_data.get('type')
                length = box_data.get('length')
                width = box_data.get('width')
                height = box_data.get('height')
                
                # Explicitly check each value is not None
                if (box_type is not None and length is not None and 
                    width is not None and height is not None):
                    return self.get_box_id_by_dimensions(
                        str(box_type), 
                        int(length), 
                        int(width), 
                        int(height)
                    )

            else:
                logger.debug("Unsupported box_data format: %s", type(box_data))

            return None

        except (ValueError, TypeError, AttributeError) as e:
            logger.debug("Error extracting box_id from data: %s", e)
            return None


    def _get_pallet_id_from_record(self, pallet_data: Any) -> Optional[str]:
        """
        Extract pallet_id from various pallet data formats.
        
        Args:
            pallet_data: Can be tuple (type, length, width, height),
                        dict with keys, or None
                
        Returns:
            pallet_id or None
        """
        if not pallet_data:
            return None

        try:
            if isinstance(pallet_data, (tuple, list)) and len(pallet_data) == 4:
                pallet_type, length, width, height = pallet_data
                # Ensure all values are not None and have correct types
                if all([pallet_type, length is not None, width is not None, height is not None]):
                    return self.get_pallet_id_by_dimensions(
                        str(pallet_type), 
                        int(length), 
                        int(width), 
                        int(height)
                    )

            elif isinstance(pallet_data, dict):
                pallet_type = pallet_data.get('type')
                length = pallet_data.get('length')
                width = pallet_data.get('width')
                height = pallet_data.get('height')
                
                # Explicitly check each value is not None
                if (pallet_type is not None and length is not None and 
                    width is not None and height is not None):
                    return self.get_pallet_id_by_dimensions(
                        str(pallet_type), 
                        int(length), 
                        int(width), 
                        int(height)
                    )

            else:
                logger.debug("Unsupported pallet_data format: %s", type(pallet_data))

            return None

        except (ValueError, TypeError, AttributeError) as e:
            logger.debug("Error extracting pallet_id from data: %s", e)
            return None

    # ========== CACHE MANAGEMENT ==========
    def log_mapping_statistics(self) -> int:
        """
        Log statistics about all loaded mappings.
        
        Returns:
            int: Total number of cached mapping entries
        """
        total_entries = 0

        logger.info("=" * 60)
        logger.info("BP MAPPING STATISTICS (with composite key support)")
        logger.info("=" * 60)
        logger.info("Composite Key Format: (type, length, width, height) → id")
        logger.info("=" * 60)

        for cache_key, mapping in self._cached_mappings.items():
            count = len(mapping)
            total_entries += count
            logger.info("%s: %d entries", cache_key, count)

        logger.info("Total cached mappings: %d entries", total_entries)
        logger.info("=" * 60)

        return total_entries

    def clear_cache(self):
        """Clear cached mappings to free memory."""
        cache_size = sum(len(mapping) for mapping in self._cached_mappings.values())
        logger.info("Clearing cache with %d total entries.", cache_size)

        self._cached_mappings.clear()
        logger.debug("Cache cleared.")


def create_bp_mapper(engine=None) -> BPObjectMapper:
    """
    Factory function to create BPObjectMapper.
    
    Args:
        engine: Optional SQLAlchemy database engine (new one created if None)
                
    Returns:
        BPObjectMapper instance ready for use
        
    Raises:
        SQLAlchemyError: If database connection fails
        RuntimeError: If mapper cannot be created
        
    Critical Timing:
        Mapper MUST be created AFTER core entity tables are loaded.
        
    Example:
        >>> mapper = create_bp_mapper()
    """
    try:
        if engine is None:
            engine = initialize_database(create_tables=False)

        # Create session factory and session
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        mapper = BPObjectMapper(session)
        logger.info("BPObjectMapper created successfully.")

        return mapper

    except SQLAlchemyError as e:
        logger.error("Database error creating BP mapper: %s", e)
        raise

    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Configuration error creating BP mapper: %s", e)
        raise RuntimeError(
            f"Failed to create BP mapper: {e}"
        ) from e

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error creating BP mapper: %s",
            unexpected_error
        )
        raise RuntimeError(
            f"Unexpected error creating BP mapper: {unexpected_error}"
        ) from unexpected_error
