# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Object Mapping Module for Material Flow Table Database.

This module provides comprehensive functionality for mapping external object
identifiers (text values, composite keys) to database primary keys. It serves
as a bridge between transformed data (which contains text references) and the
database (which uses UUIDs), enabling referential integrity during data loading.

Key Features:
    - Text-to-ID mapping for all entity types (suppliers, parts, models, etc.)
    - Composite key handling for packaging entities (boxes, pallets)
    - Memory-efficient caching of ID mappings
    - Junction table record mapping with automatic dimension handling
    - Support for multiple input formats (dict, tuple, list)
    - Consistent packaging number generation matching database triggers

Configuration Source:
    This module uses constants from columns_config.py:
        - MFT_COMPOSITE_COLUMNS: Identifies composite key types
        - MFT_JUNCTION_REQUIRED: Required columns for each junction table
        - MFT_JUNCTION_OPTIONAL: Optional columns for each junction table

Architecture:
    The module follows a caching-first approach:
    1. On-demand loading of ID mappings from database
    2. In-memory caching for high-performance lookups
    3. Automatic cache management with manual clearing capability
    
    Composite key processing matches exactly what database.py event handlers
    do, ensuring consistent packaging number generation across the system.

Dependencies:
    - SQLAlchemy 1.4.54+ for ORM and database abstraction
    - Polars for DataFrame operations (junction table mapping)
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
    ```
    from dags.tasks.mft_mapper import create_mapper
    
    # Create mapper (after entity tables are loaded)
    mapper = create_mapper()
    
    # Standard text-to-ID mapping
    part_id = mapper.get_id('part_number', 'ABC-123')
    
    # Composite key mapping for packaging
    box_id = mapper.get_id('box_composite', 
                          ('returnable', 400, 300, 200))
    
    # Junction table mapping
    records = mapper.map_junction_records(df, 'part_to_box_composite')
    
    # Pre-load all mappings for bulk operations
    mapper.get_supplier_mapping()
    mapper.get_part_mapping()
    mapper.get_box_mapping()

    # Check mapping statistics
    mapper.log_mapping_statistics()

    # Clear cache when done with bulk operations
    mapper.clear_cache()
    ```

Module Structure:
    - MFTObjectMapper: Main mapper class with comprehensive mapping capabilities
    - create_mapper(): Factory function for mapper creation
    - Column-to-model mapping configuration (COLUMN_TO_MODEL)
    - Composite key handling utilities (_get_composite_id, _load_composite_mapping)
    - Junction-specific mapping methods (_map_part_to_box_composite, etc.)
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
    - Used primarily by loader.py for junction table processing
    - Cache should be cleared after bulk operations to free memory
    - Session is maintained for the lifetime of the mapper
    - Designed for read-heavy, write-light scenarios

Version: 1.0.0
Compatibility: Python 3.12.3+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2025-01-19
Last Modified: 2025-03-12
License: MIT
Status: Production
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
    MFT_COMPOSITE_COLUMNS,
    MFT_JUNCTION_REQUIRED,
    MFT_JUNCTION_OPTIONAL
)
from dags.tasks.connector import initialize_database
from database.database import (
    SupplierData, PartData, BoxData, PalletData,
    ConfigurationData, ModelData, WorkshopData, LineData
)

# Logger setup
logger = get_logger(__name__)


class MFTObjectMapper:
    """
    Main mapper class for converting external identifiers to database primary keys.
    
    Provides unified interface for mapping text values and composite keys to
    database IDs with intelligent caching.
    
    Examples:
        >>> mapper = create_mapper()
        >>> supplier_id = mapper.get_id('supplier_name', 'Acme Corp')
        >>> box_id = mapper.get_id('box_composite', ('returnable', 400, 300, 200))
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
    }

    def __init__(
            self,
            session: Session
        ):
        """
        Initialize mapper with database session.
        
        Args:
            session: SQLAlchemy Session for database operations
        
        Example:
            >>> session_factory = sessionmaker(bind=engine)
            >>> mapper = MFTObjectMapper(session_factory())
        """
        self.session = session
        self._cached_mappings = {}
        logger.debug("MFTObjectMapper initialized with composite key support")

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
        if column_name in MFT_COMPOSITE_COLUMNS:
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
            self, model_class,
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

    # ========== SPECIALIZED METHODS FOR JUNCTION TABLES ==========

    def _map_part_to_box_composite(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map part_to_box junction record with composite keys.
        
        Args:
            record: Dict with part_number, box_type, box_length_mm, 
                   box_width_mm, box_height_mm, part_per_box (optional)
        
        Returns:
            Dict with part_id, box_id, part_per_box or None
        """
        try:
            # Check for all required columns
            required_cols = MFT_JUNCTION_REQUIRED['part_to_box_composite']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in part_to_box record",
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

            # Mapping composite box → box_id
            box_composite = {
                'type': record['box_type'],
                'length': record['box_length_mm'],
                'width': record['box_width_mm'],
                'height': record['box_height_mm']
            }

            box_id = self.get_id('box_composite', box_composite)
            if not box_id:
                logger.warning(
                    "No box_id found for composite: %s",
                    box_composite
                )
                return None

            # Create result
            result = {
                'part_id': part_id,
                'box_id': box_id
            }

            # Add optional fields
            optional_cols = MFT_JUNCTION_OPTIONAL['part_to_box_composite']
            for col in optional_cols:
                if col in record and record[col] is not None:
                    result[col] = record[col]

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping part_to_box record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping part_to_box record: %s",
                unexpected_error
            )
            return None

    def _map_box_to_pallet_composite(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map box_to_pallet junction record with composite keys.
        
        Args:
            record: Dict with box_type, box_length_mm, box_width_mm, box_height_mm,
                   pallet_type, pallet_length_mm, pallet_width_mm, pallet_height_mm,
                   box_per_pallet (optional)
        
        Returns:
            Dict with box_id, pallet_id, box_per_pallet or None
        """
        try:
            # Check for all required columns
            required_cols = MFT_JUNCTION_REQUIRED['box_to_pallet_composite']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in box_to_pallet record",
                        col
                    )
                    return None

            # Mapping composite box → box_id
            box_composite = {
                'type': record['box_type'],
                'length': record['box_length_mm'],
                'width': record['box_width_mm'],
                'height': record['box_height_mm']
            }

            box_id = self.get_id('box_composite', box_composite)
            if not box_id:
                logger.debug(
                    "No box_id found for composite: %s",
                    box_composite
                )
                return None

            # Mapping composite pallet → pallet_id
            pallet_composite = {
                'type': record['pallet_type'],
                'length': record['pallet_length_mm'],
                'width': record['pallet_width_mm'],
                'height': record['pallet_height_mm']
            }

            pallet_id = self.get_id('pallet_composite', pallet_composite)
            if not pallet_id:
                logger.debug(
                    "No pallet_id found for composite: %s",
                    pallet_composite
                )
                return None

            # Create result
            result = {
                'box_id': box_id,
                'pallet_id': pallet_id
            }

            # Add optional fields
            optional_cols = MFT_JUNCTION_OPTIONAL['box_to_pallet_composite']
            for col in optional_cols:
                if col in record and record[col] is not None:
                    result[col] = record[col]

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping box_to_pallet record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping box_to_pallet record: %s",
                unexpected_error
            )
            return None

    def _map_part_to_model(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map part_to_model junction record with required configuration.
        
        Args:
            record: Dict with with required fields:
                    part_number, model_code, configuration
                    Optional: part_per_vehicle
        
        Returns:
            Dict with part_id, model_id, configuration_id[, part_per_vehicle]
            or None if mapping fails
        """
        try:
            # Check for all required columns
            required_cols = MFT_JUNCTION_REQUIRED['part_to_model']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in part_to_model record",
                        col
                    )
                    return None

            # Mapping part_number → part_id
            part_id = self.get_id('part_number', record['part_number'])
            if not part_id:
                logger.debug(
                    "No part_id found for part_number: %s",
                    record['part_number']
                )
                return None

            # Mapping model_code → model_id
            model_id = self.get_id('model_code', record['model_code'])
            if not model_id:
                logger.debug(
                    "No model_id found for model_code: %s",
                    record['model_code']
                )
                return None

            # Mapping configuration → configuration_id
            configuration_id = self.get_id('configuration', record['configuration'])
            if not configuration_id:
                logger.debug(
                    "No configuration_id found for configuration: %s",
                    record['configuration']
                )
                return None

            # Create result
            result = {
                'part_id': part_id,
                'model_id': model_id,
                'configuration_id': configuration_id
            }

            # Add optional fields
            optional_cols = MFT_JUNCTION_OPTIONAL['part_to_model']
            for col in optional_cols:
                if col in record and record[col] is not None:
                    result[col] = record[col]

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping part_to_model record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping part_to_model record: %s",
                unexpected_error
            )
            return None

    def _map_part_to_line(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map part_to_line junction record.
        
        Args:
            record: Dict with part_number, line_code
        
        Returns:
            Dict with part_id, line_id or None
        """
        try:
            # Check for all required columns
            required_cols = MFT_JUNCTION_REQUIRED['part_to_line']
            for col in required_cols:
                if col not in record:
                    logger.debug(
                        "Missing required column '%s' in part_to_line record",
                        col
                    )
                    return None

            # Mapping part_number → part_id
            part_id = self.get_id('part_number', record['part_number'])
            if not part_id:
                logger.debug(
                    "No part_id found for part_number: %s",
                    record['part_number']
                )
                return None

            # Mapping line_code → line_id
            line_id = self.get_id('line_code', record['line_code'])
            if not line_id:
                logger.debug(
                    "No line_id found for line_code: %s",
                    record['line_code']
                )
                return None

            # Create result
            result = {
                'part_id': part_id,
                'line_id': line_id
            }

            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.debug("Error mapping part_to_line record: %s", e)
            return None

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error mapping part_to_line record: %s",
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

    def get_box_mapping(self) -> dict[str, Any]:
        """Get box_number → box_id mapping."""
        cache_key = "BoxData_composite"
        if cache_key not in self._cached_mappings:
            self._load_composite_mapping(BoxData, cache_key)
        return self._cached_mappings[cache_key]

    def get_pallet_mapping(self) -> dict[str, Any]:
        """Get pallet_number → pallet_id mapping."""
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


    def map_junction_records(
        self,
        junction_df,
        junction_type: str
    ) -> list[dict[str, Any]]:
        """
        Map junction table records with composite packaging support.
        
        Args:
            junction_df: Polars DataFrame with junction table data
            junction_type: Type of junction table to map
                (part_to_box_composite, box_to_pallet_composite, part_to_model, part_to_line)
                
        Returns:
            List of dicts with database IDs ready for insertion
            
        Examples:
            >>> records = mapper.map_junction_records(df, 'part_to_box_composite')
        """
        logger.info(
            "Mapping junction records for: %s",
            junction_type
        )

        if junction_type not in MFT_JUNCTION_REQUIRED:
            logger.error(
                "Unknown junction type: %s. Available types: %s",
                junction_type, list(MFT_JUNCTION_REQUIRED.keys())
            )
            return []

        # Mapping junction types to corresponding handlers
        handler_map = {
            'part_to_box_composite': self._map_part_to_box_composite,
            'box_to_pallet_composite': self._map_box_to_pallet_composite,
            'part_to_model': self._map_part_to_model,
            'part_to_line': self._map_part_to_line,
        }

        if junction_type not in handler_map:
            logger.error(
                "Unknown junction type: %s",
                junction_type
            )
            return []

        handler = handler_map[junction_type]

        try:
            records = junction_df.to_dicts()
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
                mapped_record = handler(record)
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
            "Junction mapping completed for '%s': total=%d, mapped=%d, skipped=%d",
            junction_type, len(records), len(mapped_records), skipped
        )

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
        """
        total_entries = 0

        logger.info("=" * 60)
        logger.info("MAPPING STATISTICS (with composite key support)")
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


def create_mapper(engine=None) -> MFTObjectMapper:
    """
    Factory function to create MFTObjectMapper.
    
    Args:
        engine: Optional SQLAlchemy database engine (new one created if None)
                
    Returns:
        MFTObjectMapper instance ready for use
        
    Raises:
        SQLAlchemyError: If database connection fails
        RuntimeError: If mapper cannot be created
        
    Critical Timing:
        Mapper MUST be created AFTER core entity tables are loaded.
        
    Example:
        >>> mapper = create_mapper()
    """
    try:
        if engine is None:
            engine = initialize_database(create_tables=False)

        # Create session factory and session
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        mapper = MFTObjectMapper(session)
        logger.info("MFTObjectMapper created successfully.")

        return mapper

    except SQLAlchemyError as e:
        logger.error("Database error creating mapper: %s", e)
        raise

    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Configuration error creating mapper: %s", e)
        raise RuntimeError(
            f"Failed to create mapper: {e}"
        ) from e

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error creating mapper: %s",
            unexpected_error
        )
        raise RuntimeError(
            f"Unexpected error creating mapper: {unexpected_error}"
        ) from unexpected_error
