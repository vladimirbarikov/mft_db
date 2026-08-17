# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Breakpoint (BP) Database Loader Module.

This module provides a robust, production-grade data loading system for bulk inserting
transformed breakpoint data into a PostgreSQL database. It handles both core entity
tables (breakpoint_data) and junction tables (part_to_breakpoint) with comprehensive
error handling, versioning, soft deactivation, and transaction management.

The loader implements a two-phase loading strategy with per-breakpoint transactions:
    1. Core entity tables are loaded first (breakpoint_data) with ON CONFLICT handling
    2. Part changes are processed with business logic for each action type:
        - ADD: Create new part and activate for model
        - DELETE: Soft deactivate part for model
        - UPDATE: Create new version with updated attributes
        - REPLACE: Deactivate old part + activate new part (two records)

Key Features:
    - Per-breakpoint transaction isolation (one transaction per breakpoint)
    - Automatic action type detection (ADD, DELETE, UPDATE, REPLACE)
    - Versioning support for parts (original_part_id, version_number)
    - Soft deactivation via PartToModel.is_active = False
    - Automatic creation of missing entities (suppliers, lines, workshops, boxes, pallets)
    - Comprehensive error handling with rollback and logging
    - Integration with BPObjectMapper for ID resolution and entity creation
    - Transaction-safe loading with per-breakpoint commit points

Loading Architecture:
    Phase 1: Load breakpoint_data
        - Insert breakpoint record with ON CONFLICT (breakpoint_number) DO NOTHING
        - Returns existing or new breakpoint_id

    Phase 2: Process part changes for each breakpoint
        For each change record in the breakpoint:
            1. Determine action type based on presence of before/after data
            2. Execute business logic for the action
            3. Create part_to_breakpoint junction record
            4. Create/update related entities as needed

Action Type Logic:
    ADD (new_part_id NOT NULL, old_part_id NULL):
        - Create new part version (version = 1)
        - Create all required entities (supplier, workshop, line, box, pallet)
        - Create all relationships (PartToModel, PartToBox, BoxToPallet, PartToLine, PartToBreakpoint)
        - Activate PartToModel (is_active = True)
        - Check for duplicates before creating any entity

    DELETE (new_part_id NULL, old_part_id NOT NULL):
        - Find existing part
        - Deactivate PartToModel (is_active = False) for specific model only
        - Set deactivated_at and deactivated_by_breakpoint_id
        - Create part_to_breakpoint (new_part_id = NULL, old_part_id)
        - Relationships remain unchanged

    UPDATE (new_part_id = old_part_id, same part_number):
        - Create new version of part (copy attributes, version + 1)
        - Create all required entities for new version (supplier, workshop, line, box, pallet)
        - Create all relationships for new version (PartToModel, PartToBox, BoxToPallet, PartToLine)
        - Activate new PartToModel (is_active = True)
        - Deactivate old PartToModel (is_active = False) for specific model only
        - Create part_to_breakpoint (new_part_id, old_part_id)
        - Check for duplicates before creating any entity

    REPLACE (new_part_id NOT NULL, old_part_id NOT NULL, different part_number):
        - DELETE old part (deactivate PartToModel for specific model only)
        - ADD new part (create/activate PartToModel with all relationships)
        - Create TWO part_to_breakpoint records:
            * (new_part_id = NULL, old_part_id = old_part_id) - DELETE
            * (new_part_id = new_part_id, old_part_id = NULL) - ADD

Entity Creation:
    - Supplier: Created if not exists (by supplier_name)
    - Workshop: Created if not exists (by workshop_code)
    - Line: Created if not exists (by line_code), requires workshop
    - Box: Created if not exists (by type, length, width, height)
    - Pallet: Created if not exists (by type, length, width, height)

Relationships Created:
    - PartToModel: Links part to model with configuration
    - PartToBox: Links part to packaging box
    - BoxToPallet: Links box to pallet
    - PartToLine: Links part to production line
    - PartToBreakpoint: Links part versions through technical changes

Transaction Strategy:
    - One transaction per breakpoint
    - All changes for a breakpoint are committed together
    - On error: ROLLBACK entire breakpoint transaction
    - Error logged, processing continues with next breakpoint
    - No global rollback of entire pipeline

Versioning Strategy:
    - original_part_id: Points to the first version of the part (NULL for version 1)
    - version_number: Sequential version number (1, 2, 3, ...)
    - When updating: max(version_number) + 1 for the part group
    - PartToModel links specific version to model with active/inactive status

Error Handling:
    - Catches and logs specific exceptions per breakpoint
    - Rolls back transaction on any error
    - Continues processing with next breakpoint
    - Provides detailed error logs with breakpoint identification
    - Returns comprehensive statistics for monitoring

Dependencies:
    - SQLAlchemy 1.4.54+: Database ORM and connection management
    - Polars: DataFrame operations and data manipulation
    - PostgreSQL 12+: Target database with full constraint support
    - bp_mapper.py: ID resolution and entity creation service
    - config.columns_config: BP pipeline configuration constants

Usage Example:
    from dags.tasks.bp_loader import load_bp_pipeline
    from dags.tasks.connector import initialize_database

    # Initialize database connection
    engine = initialize_database(create_tables=False)

    # Prepare transformed data
    transformed_data = {
        'breakpoint_df': breakpoint_df,      # breakpoint_data
        'junction_df': junction_df           # part_to_breakpoint changes
    }

    # Load BP pipeline
    results = load_bp_pipeline(transformed_data, engine)

    # Check results
    print(f"Successful: {results['successful']}")
    print(f"Failed: {results['failed']}")
    for error in results['errors']:
        print(f"Error in breakpoint {error['breakpoint_number']}: {error['error']}")

Performance Considerations:
    - Per-breakpoint transactions minimize lock contention
    - Mapper caches ID mappings for repeated lookups
    - Bulk operations use batch inserts where possible
    - Entity creation uses ON CONFLICT to avoid duplicates
    - Relationship creation uses ON CONFLICT to avoid duplicates

Data Integrity Guarantees:
    - All foreign key references validated before insertion
    - Version numbers are sequential per part group
    - Active status is maintained consistently per model
    - Unique constraints enforced at database level
    - Full transaction rollback on any error
    - Deactivation only affects specified model (not all models)

Logging:
    - INFO level: Breakpoint processing progress and summary
    - DEBUG level: Detailed action processing and entity creation
    - WARNING level: Missing data, duplicate records, entity fallbacks
    - ERROR level: Failed breakpoints with full traceback

Maintainer: PLD Engineering Center
Version: 1.0.0
Compatibility: Python 3.14.4+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Created: 2026-08-11
Last Modified: 2026-08-13
License: MIT
Status: Development
"""
# Standard library imports
from pathlib import Path
import sys
from datetime import datetime
from typing import Any, Callable, Optional, Dict, List, Type, Tuple
import traceback

# Third-party imports
import polars as pl
from sqlalchemy import text, select
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from config.columns_config import (
    BP_TABLE_REQUIREMENTS,
    BP_ACTION_TYPES,
    BP_REQUIRED_FIELDS_BY_ACTION
)
from dags.tasks.connector import initialize_database
from dags.tasks.bp_mapper import BPObjectMapper, create_bp_mapper
from database.database import (
    # Entity tables
    SupplierData, PartData, BoxData, PalletData, ModelData,
    ConfigurationData, WorkshopData, LineData, BreakpointData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel, PartToLine, PartToBreakpoint,
)

# Logger setup
logger = get_logger(__name__)


# ============================================================================
# DATABASE HELPER FUNCTIONS
# ============================================================================

def _bulk_insert_with_returning(
    connection,
    model_class: Type,
    values_list: List[Dict[str, Any]],
    returning_col,
    conflict_cols: List[str]
) -> List[Any]:
    """
    Execute bulk INSERT with RETURNING.

    Args:
        connection: Database connection
        model_class: SQLAlchemy model class
        values_list: List of dicts with values to insert
        returning_col: Column to return after insert
        conflict_cols: Columns to use for ON CONFLICT DO NOTHING

    Returns:
        List of returned values (IDs)
    """
    try:
        stmt = pg_insert(model_class.__table__).values(values_list).returning(returning_col)

        if conflict_cols:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

        result = connection.execute(stmt)
        return result.scalars().all()

    except SQLAlchemyError as e:
        logger.error("Error in _bulk_insert_with_returning: %s", e)
        return []


def _insert_with_returning(
    connection,
    model_class: Type,
    values: Dict[str, Any],
    returning_col,
    conflict_cols: List[str]
) -> Optional[str]:
    """
    Execute INSERT with RETURNING and conflict handling.

    Args:
        connection: Database connection
        model_class: SQLAlchemy model class
        values: Dict with values to insert
        returning_col: Column to return after insert
        conflict_cols: Columns to use for ON CONFLICT DO NOTHING

    Returns:
        Returned value (ID) or None
    """
    try:
        stmt = pg_insert(model_class.__table__).values([values]).returning(returning_col)

        if conflict_cols:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

        result = connection.execute(stmt)
        return result.scalar_one_or_none()

    except SQLAlchemyError as e:
        logger.error("Error in _insert_with_returning: %s", e)
        return None


def _insert_without_returning(
    connection,
    model_class: Type,
    values: Dict[str, Any],
    conflict_cols: List[str]
) -> bool:
    """
    Execute INSERT without RETURNING (for junction tables).

    Args:
        connection: Database connection
        model_class: SQLAlchemy model class
        values: Dict with values to insert
        conflict_cols: Columns to use for ON CONFLICT DO NOTHING

    Returns:
        True if successful, False otherwise
    """
    try:
        stmt = pg_insert(model_class.__table__).values([values])

        if conflict_cols:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

        connection.execute(stmt)
        return True

    except SQLAlchemyError as e:
        logger.error("Error in _insert_without_returning: %s", e)
        return False


def _validate_breakpoint_columns(
    df: pl.DataFrame,
    table_name: str
) -> bool:
    """
    Validate required columns in DataFrame for breakpoint_data.

    Args:
        df: Polars DataFrame to validate
        table_name: Table name ('breakpoint_data')

    Returns:
        True if all required columns present, False otherwise
    """
    if table_name in BP_TABLE_REQUIREMENTS:
        required_cols = BP_TABLE_REQUIREMENTS[table_name]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(
                "Cannot load %s: missing required columns: %s. Available: %s.",
                table_name, missing_cols, list(df.columns)
            )
            return False

    return True


def _validate_action_fields(
    record: Dict[str, Any],
    action: str
) -> bool:
    """
    Validate required fields for the given action type.

    Args:
        record: Record with data
        action: Action type ('ADD', 'DELETE', 'UPDATE', 'REPLACE')

    Returns:
        True if all required fields present, False otherwise
    """
    if action not in BP_REQUIRED_FIELDS_BY_ACTION:
        logger.error("Unknown action type: %s", action)
        return False

    required_fields = BP_REQUIRED_FIELDS_BY_ACTION[action]
    missing_fields = [field for field in required_fields if field not in record or record.get(field) is None]

    if missing_fields:
        logger.error(
            "Action %s is missing required fields: %s. Available: %s",
            action, missing_fields, list(record.keys())
        )
        return False

    return True


def _parse_composite_key(composite_data: Any) -> Optional[Tuple[str, int, int, int]]:
    """
    Parse composite key for box or pallet from various formats.

    Args:
        composite_data: Tuple, dict, or string representing composite key

    Returns:
        Parsed tuple (type, length, width, height) or None
    """
    if not composite_data:
        return None

    try:
        # If it's a tuple or list
        if isinstance(composite_data, (tuple, list)) and len(composite_data) == 4:
            p_type, l, w, h = composite_data
            # Direct check of each element for type checker
            if p_type is not None and l is not None and w is not None and h is not None:
                return str(p_type), int(l), int(w), int(h)

            logger.debug("Composite key contains None values: %s", composite_data)
            return None

        # If it's a dict
        if isinstance(composite_data, dict):
            pack_type = composite_data.get('type')
            length = composite_data.get('length')
            width = composite_data.get('width')
            height = composite_data.get('height')

            # Explicit type narrowing without using all()
            if pack_type is not None and length is not None and width is not None and height is not None:
                return str(pack_type), int(length), int(width), int(height)

            logger.debug("Composite dict contains None values: %s", composite_data)
            return None

        # If it's a string in format "type length-width-height"
        if isinstance(composite_data, str):
            parts = composite_data.split()
            if len(parts) == 2:
                pack_type = parts[0]
                dims = parts[1].split('-')
                if len(dims) == 3 and all(dim.strip() for dim in dims):
                    return pack_type, int(dims[0]), int(dims[1]), int(dims[2])

        logger.debug("Unsupported composite key format: %s", composite_data)
        return None

    except (ValueError, TypeError) as e:
        logger.debug("Failed to parse composite key due to conversion error: %s", e)
        return None


# ============================================================================
# ENTITY SERVICE - centralized service for entity operations
# ============================================================================

class EntityService:
    """
    Service for creating/retrieving entities within a single transaction.

    Uses BPObjectMapper ONLY for READ operations (ID resolution).
    Entity creation and modification are the responsibility of this service.
    """

    def __init__(self, connection, mapper: BPObjectMapper):
        self.connection = connection
        self.mapper = mapper

        # Cache of created entities within transaction
        self._supplier_cache: Dict[str, str] = {}
        self._workshop_cache: Dict[str, str] = {}
        self._line_cache: Dict[str, str] = {}
        self._box_cache: Dict[tuple, str] = {}
        self._pallet_cache: Dict[tuple, str] = {}
        self._part_cache: Dict[str, Dict[str, Any]] = {}
        self._breakpoint_cache: Dict[str, str] = {}
        self._model_cache: Dict[str, str] = {}
        self._configuration_cache: Dict[str, str] = {}

    # READ METHODS (ID retrieval via mapper, no creation)
    def get_part_id(self, part_number: str) -> Optional[str]:
        """
        Get part_id by part number via mapper (READ-ONLY).

        Args:
            part_number: Part number to look up

        Returns:
            part_id or None
        """
        if not part_number:
            return None
        return self.mapper.get_part_id_by_number(part_number)

    # ENSURE METHODS (check via mapper + direct query if needed)
    def ensure_model(self, model_code: str) -> Optional[str]:
        """
        Check model existence and return model_id.

        First checks via mapper (cache), if not found - performs direct DB query.
        Models must be pre-loaded via MFT pipeline.

        Args:
            model_code: Model code to look up

        Returns:
            model_id or None
        """
        if not model_code:
            return None

        if model_code in self._model_cache:
            return self._model_cache[model_code]

        # First check via mapper (cache)
        model_id = self.mapper.get_model_id_by_code(model_code)
        if model_id:
            self._model_cache[model_code] = model_id
            logger.debug("Model found via mapper: %s -> %s", model_code, model_id)
            return model_id

        # If not found in mapper cache - perform direct query
        try:
            stmt = select(ModelData.model_id).where(
                ModelData.model_code == model_code
            )
            result = self.connection.execute(stmt).scalar_one_or_none()
            if result:
                self._model_cache[model_code] = result
                logger.debug("Model found via direct query: %s -> %s", model_code, result)
                return result
            else:
                logger.error("Model not found: %s", model_code)
                return None
        except SQLAlchemyError as e:
            logger.error("Error checking model %s: %s", model_code, e)
            return None

    def ensure_configuration(self, configuration: str) -> Optional[str]:
        """
        Create or retrieve configuration_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            configuration: Configuration name

        Returns:
            configuration_id or None
        """
        if not configuration:
            return None

        if configuration in self._configuration_cache:
            return self._configuration_cache[configuration]

        # First check via mapper (cache)
        config_id = self.mapper.get_configuration_id(configuration)
        if config_id:
            self._configuration_cache[configuration] = config_id
            logger.debug("Configuration found via mapper: %s -> %s", configuration, config_id)
            return config_id

        # If not found in mapper cache - perform direct query
        try:
            stmt = select(ConfigurationData.configuration_id).where(
                ConfigurationData.configuration == configuration
            )
            result = self.connection.execute(stmt).scalar_one_or_none()
            if result:
                self._configuration_cache[configuration] = result
                logger.debug("Configuration found via direct query: %s -> %s", configuration, result)
                return result
            else:
                logger.error("Configuration not found: %s", configuration)
                return None
        except SQLAlchemyError as e:
            logger.error("Error checking configuration %s: %s", configuration, e)
            return None

    def ensure_supplier(self, name: str, localization: str = 'no data') -> Optional[str]:
        """
        Create or retrieve supplier_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            name: Supplier name
            localization: Supplier localization (default: 'no data')

        Returns:
            supplier_id or None
        """
        if not name:
            return None

        cache_key = f"{name}|{localization}"
        if cache_key in self._supplier_cache:
            return self._supplier_cache[cache_key]

        try:
            # First check via mapper (READ-ONLY)
            supplier_id = self.mapper.get_supplier_id_by_name(name)
            if supplier_id:
                self._supplier_cache[cache_key] = supplier_id
                return supplier_id

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {
                'supplier_name': name,
                'localization': localization
            }

            result = _insert_with_returning(
                self.connection,
                SupplierData,
                values,
                SupplierData.supplier_id,
                ['supplier_name']
            )

            if result:
                self._supplier_cache[cache_key] = result
                logger.debug("Created supplier: %s -> %s", name, result)
                return result

            logger.warning("Failed to create/find supplier: %s", name)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring supplier %s: %s", name, e)
            return None

    def ensure_workshop(self, code: str) -> Optional[str]:
        """
        Create or retrieve workshop_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            code: Workshop code

        Returns:
            workshop_id or None
        """
        if not code:
            return None

        if code in self._workshop_cache:
            return self._workshop_cache[code]

        try:
            # First check via mapper (READ-ONLY)
            workshop_id = self.mapper.get_workshop_id_by_code(code)
            if workshop_id:
                self._workshop_cache[code] = workshop_id
                return workshop_id

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {'workshop_code': code}

            result = _insert_with_returning(
                self.connection,
                WorkshopData,
                values,
                WorkshopData.workshop_id,
                ['workshop_code']
            )

            if result:
                self._workshop_cache[code] = result
                logger.debug("Created workshop: %s -> %s", code, result)
                return result

            logger.warning("Failed to create/find workshop: %s", code)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring workshop %s: %s", code, e)
            return None

    def ensure_line(self, code: str, name: Optional[str], workshop_code: str) -> Optional[str]:
        """
        Create or retrieve line_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            code: Line code
            name: Line name (optional)
            workshop_code: Workshop code

        Returns:
            line_id or None
        """
        if not code or not workshop_code:
            return None

        cache_key = f"{code}|{workshop_code}"
        if cache_key in self._line_cache:
            return self._line_cache[cache_key]

        try:
            # First check via mapper (READ-ONLY)
            line_id = self.mapper.get_line_id_by_code(code)
            if line_id:
                self._line_cache[cache_key] = line_id
                return line_id

            # If not found - create new (WRITE)
            workshop_id = self.ensure_workshop(workshop_code)
            if not workshop_id:
                logger.error("Cannot create line: workshop %s not found", workshop_code)
                return None

            values: Dict[str, Any] = {'line_code': code, 'workshop_id': workshop_id}
            if name:
                values['line_name'] = name

            result = _insert_with_returning(
                self.connection,
                LineData,
                values,
                LineData.line_id,
                ['line_code']
            )

            if result:
                self._line_cache[cache_key] = result
                logger.debug("Created line: %s -> %s", code, result)
                return result

            logger.warning("Failed to create/find line: %s", code)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring line %s: %s", code, e)
            return None

    def ensure_box(
        self,
        box_type: str,
        length_mm: int,
        width_mm: int,
        height_mm: int,
        weight_kg: Optional[float] = None,
        stacking: Optional[int] = None,
    ) -> Optional[str]:
        """
        Create or retrieve box_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            box_type: Box type ('returnable' or 'non-returnable')
            length_mm: Length in millimeters
            width_mm: Width in millimeters
            height_mm: Height in millimeters
            weight_kg: Box weight in kg (optional)
            stacking: Stacking limit (optional)

        Returns:
            box_id or None
        """
        if not box_type or not all([length_mm, width_mm, height_mm]):
            return None

        cache_key = (box_type, length_mm, width_mm, height_mm)
        if cache_key in self._box_cache:
            return self._box_cache[cache_key]

        try:
            # First check via mapper (READ-ONLY)
            box_id = self.mapper.get_box_id_by_dimensions(
                box_type, length_mm, width_mm, height_mm
            )
            if box_id:
                self._box_cache[cache_key] = box_id
                return box_id

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {
                'box_type': box_type,
                'box_length_mm': length_mm,
                'box_width_mm': width_mm,
                'box_height_mm': height_mm,
            }
            if weight_kg is not None:
                values['box_weight_kg'] = weight_kg
            if stacking is not None:
                values['box_stacking'] = stacking

            result = _insert_with_returning(
                self.connection,
                BoxData,
                values,
                BoxData.box_id,
                ['box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm']
            )

            if result:
                self._box_cache[cache_key] = result
                logger.debug("Created box: %s %dx%dx%d -> %s",
                           box_type, length_mm, width_mm, height_mm, result)
                return result

            logger.warning("Failed to create/find box: %s %dx%dx%d",
                          box_type, length_mm, width_mm, height_mm)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring box: %s", e)
            return None

    def ensure_pallet(
        self,
        pallet_type: str,
        length_mm: int,
        width_mm: int,
        height_mm: int,
        weight_kg: Optional[float] = None,
        stacking: Optional[int] = None,
    ) -> Optional[str]:
        """
        Create or retrieve pallet_id.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            pallet_type: Pallet type ('returnable' or 'non-returnable')
            length_mm: Length in millimeters
            width_mm: Width in millimeters
            height_mm: Height in millimeters
            weight_kg: Pallet weight in kg (optional)
            stacking: Stacking limit (optional)

        Returns:
            pallet_id or None
        """
        if not pallet_type or not all([length_mm, width_mm, height_mm]):
            return None

        cache_key = (pallet_type, length_mm, width_mm, height_mm)
        if cache_key in self._pallet_cache:
            return self._pallet_cache[cache_key]

        try:
            # First check via mapper (READ-ONLY)
            pallet_id = self.mapper.get_pallet_id_by_dimensions(
                pallet_type, length_mm, width_mm, height_mm
            )
            if pallet_id:
                self._pallet_cache[cache_key] = pallet_id
                return pallet_id

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {
                'pallet_type': pallet_type,
                'pallet_length_mm': length_mm,
                'pallet_width_mm': width_mm,
                'pallet_height_mm': height_mm,
            }
            if weight_kg is not None:
                values['pallet_weight_kg'] = weight_kg
            if stacking is not None:
                values['pallet_stacking'] = stacking

            result = _insert_with_returning(
                self.connection,
                PalletData,
                values,
                PalletData.pallet_id,
                ['pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm']
            )

            if result:
                self._pallet_cache[cache_key] = result
                logger.debug("Created pallet: %s %dx%dx%d -> %s",
                           pallet_type, length_mm, width_mm, height_mm, result)
                return result

            logger.warning("Failed to create/find pallet: %s %dx%dx%d",
                          pallet_type, length_mm, width_mm, height_mm)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring pallet: %s", e)
            return None

    def ensure_breakpoint_with_data(
        self,
        breakpoint_number: str,
        status: str = 'no data',
        change_date: Optional[datetime] = None,
        batch_plan: Optional[str] = None,
        batch_fact: Optional[str] = None,
        description: Optional[str] = None,
        solution: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create or retrieve breakpoint with data.

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            breakpoint_number: Breakpoint number
            status: Breakpoint status
            change_date: Change date
            batch_plan: Batch plan
            batch_fact: Batch fact
            description: Description
            solution: Solution

        Returns:
            breakpoint_id or None
        """
        if not breakpoint_number:
            return None

        if breakpoint_number in self._breakpoint_cache:
            return self._breakpoint_cache[breakpoint_number]

        try:
            # First check via mapper (READ-ONLY)
            breakpoint_id = self.mapper.get_breakpoint_id(breakpoint_number)
            if breakpoint_id:
                self._breakpoint_cache[breakpoint_number] = breakpoint_id
                logger.debug("Breakpoint already exists: %s -> %s", breakpoint_number, breakpoint_id)
                return breakpoint_id

            # Prepare change_date
            if change_date:
                if isinstance(change_date, str):
                    try:
                        change_date = datetime.fromisoformat(change_date)
                    except ValueError:
                        logger.warning("Invalid change_date format: %s, using current time", change_date)
                        change_date = datetime.now()
            else:
                change_date = datetime.now()

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {
                'breakpoint_number': breakpoint_number,
                'breakpoint_status': status,
                'breakpoint_date': change_date,
            }

            if batch_plan and batch_plan.strip():
                values['batch_plan'] = batch_plan.strip()
            if batch_fact and batch_fact.strip():
                values['batch_fact'] = batch_fact.strip()
            if description and description.strip():
                values['description'] = description.strip()
            if solution and solution.strip():
                values['solution'] = solution.strip()

            results = _bulk_insert_with_returning(
                self.connection,
                BreakpointData,
                [values],
                BreakpointData.breakpoint_id,
                ['breakpoint_number']
            )

            if results:
                breakpoint_id = results[0]
                self._breakpoint_cache[breakpoint_number] = breakpoint_id
                logger.info("Created breakpoint %s with ID %s", breakpoint_number, breakpoint_id)
                return breakpoint_id

            logger.error("No breakpoint_id returned when creating breakpoint %s", breakpoint_number)
            return None

        except SQLAlchemyError as e:
            logger.error("Error ensuring breakpoint %s: %s", breakpoint_number, e)
            return None

    # ========================================================================
    # PART METHODS
    # ========================================================================

    def get_or_create_part(
        self,
        part_number: str,
        part_name: Optional[str] = None,
        supplier_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get existing part or create new one (version 1).

        First checks via mapper (READ-ONLY), if not found - creates new.

        Args:
            part_number: Part number
            part_name: Part name (optional)
            supplier_id: Supplier ID (optional)

        Returns:
            Part info dict with part_id, original_part_id, version_number, is_new
        """
        if not part_number:
            return None

        if part_number in self._part_cache:
            return self._part_cache[part_number]

        try:
            # First check via mapper (READ-ONLY)
            part_id = self.mapper.get_part_id_by_number(part_number)
            if part_id:
                # Get additional info
                stmt = select(
                    PartData.original_part_id,
                    PartData.version_number,
                ).where(PartData.part_id == part_id)
                result = self.connection.execute(stmt).first()

                if result:
                    part_info = {
                        'part_id': part_id,
                        'original_part_id': result[0] or part_id,
                        'version_number': result[1],
                        'is_new': False,
                    }
                    self._part_cache[part_number] = part_info
                    return part_info

            # If not found - create new (WRITE)
            values: Dict[str, Any] = {
                'part_number': part_number,
                'version_number': 1,
            }
            if part_name:
                values['part_name'] = part_name
            if supplier_id:
                values['supplier_id'] = supplier_id

            results = _bulk_insert_with_returning(
                self.connection,
                PartData,
                [values],
                PartData.part_id,
                []  # No conflicts, we already checked existence
            )

            if not results:
                logger.error("No result when creating new part %s", part_number)
                return None

            part_id = results[0]

            # Update original_part_id to self
            update_stmt = text("""
                UPDATE part_data
                SET original_part_id = :part_id
                WHERE part_id = :part_id
            """)
            self.connection.execute(update_stmt, {'part_id': part_id})

            part_info = {
                'part_id': part_id,
                'original_part_id': part_id,
                'version_number': 1,
                'is_new': True,
            }

            self._part_cache[part_number] = part_info
            logger.debug("Created new part: %s -> %s", part_number, part_id)
            return part_info

        except SQLAlchemyError as e:
            logger.error("Error getting/creating part %s: %s", part_number, e)
            return None

    def create_new_part_version(
        self,
        part_number: str,
        original_part_id: str,
        part_name: Optional[str] = None,
        supplier_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a new version of an existing part with locking.

        Args:
            part_number: New part number
            original_part_id: Original part ID
            part_name: Part name (optional)
            supplier_id: Supplier ID (optional)

        Returns:
            New part_id or None
        """
        if not part_number or not original_part_id:
            return None

        try:
            lock_stmt = text("""
                SELECT COALESCE(MAX(version_number), 0) + 1
                FROM part_data
                WHERE original_part_id = :original_part_id
                FOR UPDATE
            """)

            result = self.connection.execute(lock_stmt, {'original_part_id': original_part_id})
            new_version = result.scalar_one()

            if not new_version:
                new_version = 1

            values: Dict[str, Any] = {
                'part_number': part_number,
                'original_part_id': original_part_id,
                'version_number': new_version,
            }
            if part_name:
                values['part_name'] = part_name
            if supplier_id:
                values['supplier_id'] = supplier_id

            results = _bulk_insert_with_returning(
                self.connection,
                PartData,
                [values],
                PartData.part_id,
                []  # No conflicts, this is a new version
            )

            if results:
                part_id = results[0]
                logger.debug("Created new version %d of part %s -> %s",
                           new_version, part_number, part_id)
                return part_id

            logger.error("Failed to create new version of part %s", part_number)
            return None

        except SQLAlchemyError as e:
            logger.error("Error creating new version of part %s: %s", part_number, e)
            return None

    # ========================================================================
    # JUNCTION TABLE METHODS (WRITE operations)
    # ========================================================================

    def activate_part_for_model(
        self,
        part_id: str,
        model_id: str,
        configuration_id: str,
        part_per_vehicle: Optional[int],
        breakpoint_id: str,
    ) -> bool:
        """
        Activate part for model (PartToModel).

        Args:
            part_id: Part ID
            model_id: Model ID
            configuration_id: Configuration ID
            part_per_vehicle: Quantity per vehicle
            breakpoint_id: Breakpoint ID that triggered activation (for logging)

        Returns:
            True if successful, False otherwise
        """
        try:
            values: Dict[str, Any] = {
                'part_id': part_id,
                'model_id': model_id,
                'configuration_id': configuration_id,
                'is_active': True,
            }
            if part_per_vehicle is not None:
                values['part_per_vehicle'] = part_per_vehicle

            logger.debug(
                "Activating part %s for model %s due to breakpoint %s",
                part_id, model_id, breakpoint_id
            )

            return _insert_without_returning(
                self.connection,
                PartToModel,
                values,
                ['part_id', 'model_id', 'configuration_id']
            )

        except SQLAlchemyError as e:
            logger.error("Error activating part %s for model: %s", part_id, e)
            return False

    def deactivate_part_for_model(
        self,
        part_id: str,
        model_id: str,
        breakpoint_id: str,
    ) -> bool:
        """
        Deactivate part for model.

        Args:
            part_id: Part ID
            model_id: Model ID
            breakpoint_id: Breakpoint ID that triggered deactivation

        Returns:
            True if successful, False otherwise
        """
        try:
            update_stmt = text("""
                UPDATE part_to_model
                SET is_active = False,
                    deactivated_at = NOW(),
                    deactivated_by_breakpoint_id = :breakpoint_id
                WHERE part_id = :part_id
                  AND model_id = :model_id
                  AND is_active = True
            """)

            result = self.connection.execute(update_stmt, {
                'part_id': part_id,
                'model_id': model_id,
                'breakpoint_id': breakpoint_id,
            })

            if result.rowcount > 0:
                logger.debug(
                    "Deactivated part %s for model %s due to breakpoint %s",
                    part_id, model_id, breakpoint_id
                )
                return True
            else:
                logger.warning(
                    "No active PartToModel to deactivate for part %s, model %s (breakpoint %s)",
                    part_id, model_id, breakpoint_id
                )
                return False

        except SQLAlchemyError as e:
            logger.error("Error deactivating part %s for model: %s", part_id, e)
            return False

    def ensure_part_to_box(
        self,
        part_id: str,
        box_id: str,
        part_per_box: Optional[int] = None,
    ) -> bool:
        """
        Create PartToBox relationship (if not exists).

        Args:
            part_id: Part ID
            box_id: Box ID
            part_per_box: Parts per box

        Returns:
            True if successful, False otherwise
        """
        try:
            values: Dict[str, Any] = {
                'part_id': part_id,
                'box_id': box_id,
            }
            if part_per_box is not None:
                values['part_per_box'] = part_per_box

            return _insert_without_returning(
                self.connection,
                PartToBox,
                values,
                ['part_id', 'box_id']
            )

        except SQLAlchemyError as e:
            logger.error("Error creating PartToBox for part %s: %s", part_id, e)
            return False


    def ensure_box_to_pallet(
        self,
        part_id: str,
        box_id: str,
        pallet_id: str,
        box_per_pallet: Optional[int] = None,
    ) -> bool:
        """
        Create BoxToPallet relationship (if not exists).

        Args:
            part_id: Part ID
            box_id: Box ID
            pallet_id: Pallet ID
            box_per_pallet: Boxes per pallet

        Returns:
            True if successful, False otherwise
        """
        try:
            values: Dict[str, Any] = {
                'part_id': part_id,
                'box_id': box_id,
                'pallet_id': pallet_id,
            }
            if box_per_pallet is not None:
                values['box_per_pallet'] = box_per_pallet

            return _insert_without_returning(
                self.connection,
                BoxToPallet,
                values,
                ['part_id', 'box_id', 'pallet_id']
            )

        except SQLAlchemyError as e:
            logger.error("Error creating BoxToPallet for part %s: %s", part_id, e)
            return False

    def ensure_part_to_line(
        self,
        part_id: str,
        line_id: str,
    ) -> bool:
        """
        Create PartToLine relationship (if not exists).

        Args:
            part_id: Part ID
            line_id: Line ID

        Returns:
            True if successful, False otherwise
        """
        try:
            values: Dict[str, Any] = {
                'part_id': part_id,
                'line_id': line_id,
            }

            return _insert_without_returning(
                self.connection,
                PartToLine,
                values,
                ['part_id', 'line_id']
            )

        except SQLAlchemyError as e:
            logger.error("Error creating PartToLine for part %s: %s", part_id, e)
            return False

    def create_part_to_breakpoint(
        self,
        new_part_id: Optional[str],
        old_part_id: Optional[str],
        breakpoint_id: str,
        model_id: str,
    ) -> bool:
        """
        Create record in part_to_breakpoint.

        Args:
            new_part_id: New part ID (can be None)
            old_part_id: Old part ID (can be None)
            breakpoint_id: Breakpoint ID
            model_id: Model ID

        Returns:
            True if successful, False otherwise
        """
        try:
            values: Dict[str, Any] = {
                'new_part_id': new_part_id,
                'old_part_id': old_part_id,
                'breakpoint_id': breakpoint_id,
                'model_id': model_id,
            }

            return _insert_without_returning(
                self.connection,
                PartToBreakpoint,
                values,
                ['new_part_id', 'old_part_id', 'breakpoint_id', 'model_id']
            )

        except SQLAlchemyError as e:
            logger.error("Error creating part_to_breakpoint: %s", e)
            return False


# ============================================================================
# MAIN BREAKPOINT PROCESSING LOGIC
# ============================================================================

def _determine_action_type(record: Dict[str, Any]) -> str:
    """
    Determine action type based on presence of before/after data.

    Returns:
        One of BP_ACTION_TYPES constants: 'ADD', 'DELETE', 'UPDATE', 'REPLACE'
    """
    has_before = bool(record.get('part_no_before'))
    has_after = bool(record.get('part_no_after'))

    if not has_before and has_after:
        return BP_ACTION_TYPES['ADD']
    elif has_before and not has_after:
        return BP_ACTION_TYPES['DELETE']
    elif has_before and has_after:
        if record['part_no_before'] == record['part_no_after']:
            return BP_ACTION_TYPES['UPDATE']
        else:
            return BP_ACTION_TYPES['REPLACE']
    else:
        return 'UNKNOWN'


def _get_record_value(record: Dict[str, Any], field: str, default: Any = None) -> Any:
    """Safely get value from record."""
    return record.get(field, default)


def _create_part_relations(
    service: EntityService,
    part_id: str,
    record: Dict[str, Any],
    prefix: str
) -> bool:
    """
    Create all relationships for a part (PartToBox, BoxToPallet, PartToLine).
    
    Args:
        service: EntityService
        part_id: Part ID
        record: Record with data
        prefix: 'before' or 'after'
    
    Returns:
        True if all relationships created successfully
    """
    try:
        # Get parameters from record
        workshop_code = _get_record_value(record, f'workshop_{prefix}')
        workcenter_no = _get_record_value(record, f'workcenter_no_{prefix}')
        workcenter_name = _get_record_value(record, f'workcenter_name_{prefix}')
        box_data = _get_record_value(record, f'box_{prefix}')
        pallet_data = _get_record_value(record, f'pallet_{prefix}')
        quantity_per_box = _get_record_value(record, f'quantity_per_box_{prefix}')
        box_per_pallet = _get_record_value(record, f'box_per_pallet_{prefix}')

        # Create/check Workshop
        workshop_id = None
        if workshop_code:
            workshop_id = service.ensure_workshop(workshop_code)
            if not workshop_id:
                logger.error("Failed to ensure workshop: %s", workshop_code)
                return False

        # Create/check Line
        line_id = None
        if workcenter_no and workshop_code:
            line_id = service.ensure_line(workcenter_no, workcenter_name, workshop_code)
            if not line_id:
                logger.error("Failed to ensure line: %s", workcenter_no)

        # Create/check Box
        box_id = None
        if box_data:
            parsed_box = _parse_composite_key(box_data)
            if parsed_box:
                box_type, length, width, height = parsed_box
                box_id = service.ensure_box(box_type, length, width, height)
                if not box_id:
                    logger.error("Failed to ensure box: %s", box_data)

        # Create/check Pallet
        pallet_id = None
        if pallet_data:
            parsed_pallet = _parse_composite_key(pallet_data)
            if parsed_pallet:
                pallet_type, length, width, height = parsed_pallet
                pallet_id = service.ensure_pallet(pallet_type, length, width, height)
                if not pallet_id:
                    logger.error("Failed to ensure pallet: %s", pallet_data)

        # Create relationships
        if box_id:
            if not service.ensure_part_to_box(part_id, box_id, quantity_per_box):
                logger.error("Failed to create PartToBox for part %s", part_id)
                return False

            if pallet_id:
                if not service.ensure_box_to_pallet(part_id, box_id, pallet_id, box_per_pallet):
                    logger.error("Failed to create BoxToPallet for part %s", part_id)
                    return False

        if line_id:
            if not service.ensure_part_to_line(part_id, line_id):
                logger.error("Failed to create PartToLine for part %s", part_id)
                return False

        return True

    except KeyError as e:
        logger.error("Missing expected key in record for part %s: %s", part_id, e)
        logger.debug(traceback.format_exc())
        return False
    except ValueError as e:
        logger.error("Invalid value in record for part %s: %s", part_id, e)
        logger.debug(traceback.format_exc())
        return False
    except TypeError as e:
        logger.error("Wrong data type in record for part %s: %s", part_id, e)
        logger.debug(traceback.format_exc())
        return False
    except AttributeError as e:
        logger.error("Missing attribute for part %s: %s", part_id, e)
        logger.debug(traceback.format_exc())
        return False
    except SQLAlchemyError as e:
        logger.error("Database error creating relations for part %s: %s", part_id, e)
        logger.debug(traceback.format_exc())
        return False
    except Exception as unexpected_error:
        logger.error("Unexpected error creating part relations for part %s: %s", part_id, unexpected_error)
        logger.debug(traceback.format_exc())
        return False


def _process_add_action(
    record: Dict[str, Any],
    service: EntityService,
    breakpoint_id: str,
    model_id: str,
) -> bool:
    """
    Process ADD action.
    
    ADD: part_no_before NULL, part_no_after NOT NULL.
    Creates a new part with all relationships.
    """
    part_number_after = _get_record_value(record, 'part_no_after')
    part_name_after = _get_record_value(record, 'part_name_after')
    configuration = _get_record_value(record, 'configuration')
    part_per_vehicle = _get_record_value(record, 'quantity_per_vehicle_after')
    supplier_name_after = _get_record_value(record, 'supplier_name_after')
    localization_after = _get_record_value(record, 'localization_after', 'no data')

    try:
        # Get supplier_id via service (creates if not exists)
        supplier_id = None
        if supplier_name_after:
            supplier_id = service.ensure_supplier(supplier_name_after, localization_after)
            if not supplier_id:
                logger.warning("Could not create/find supplier %s", supplier_name_after)

        # Get or create part
        part_info = service.get_or_create_part(
            part_number_after,
            part_name_after,
            supplier_id,
        )
        if not part_info:
            logger.error("Failed to create/get part %s", part_number_after)
            return False

        part_id = part_info['part_id']

        # Get configuration_id via service
        configuration_id = service.ensure_configuration(configuration)
        if not configuration_id:
            logger.error("Configuration not found: %s", configuration)
            return False

        # Create all relationships for new part (PartToBox, BoxToPallet, PartToLine)
        if not _create_part_relations(service, part_id, record, 'after'):
            logger.error("Failed to create relations for part %s", part_id)
            return False

        # Activate part for model
        if not service.activate_part_for_model(
            part_id, model_id, configuration_id, part_per_vehicle, breakpoint_id
        ):
            logger.error("Failed to activate part %s for model", part_id)
            return False

        # Create record in part_to_breakpoint
        if not service.create_part_to_breakpoint(part_id, None, breakpoint_id, model_id):
            logger.error("Failed to create part_to_breakpoint for ADD")
            return False

        logger.info("ADD processed successfully: part %s", part_number_after)
        return True

    except KeyError as e:
        logger.error("Missing key in record for ADD action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except ValueError as e:
        logger.error("Invalid value in record for ADD action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except TypeError as e:
        logger.error("Wrong data type in record for ADD action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except AttributeError as e:
        logger.error("Missing attribute for ADD action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except SQLAlchemyError as e:
        logger.error("Database error in ADD action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except Exception as unexpected_error:
        logger.error("Unexpected error processing ADD action: %s", unexpected_error)
        logger.debug(traceback.format_exc())
        return False


def _process_delete_action(
    record: Dict[str, Any],
    service: EntityService,
    breakpoint_id: str,
    model_id: str,
) -> bool:
    """
    Process DELETE action.
    
    DELETE: part_no_before NOT NULL, part_no_after NULL.
    Part is deactivated for the specified model. Relationships remain unchanged.
    """
    part_number_before = _get_record_value(record, 'part_no_before')

    try:
        # Get part_id via mapper
        part_id = service.get_part_id(part_number_before)
        if not part_id:
            logger.error("Part not found: %s", part_number_before)
            return False

        # Deactivate part for model (only for specified model)
        if not service.deactivate_part_for_model(part_id, model_id, breakpoint_id):
            logger.warning("Failed to deactivate part %s for model (may already be inactive)",
                         part_id)

        # Create record in part_to_breakpoint
        if not service.create_part_to_breakpoint(None, part_id, breakpoint_id, model_id):
            logger.error("Failed to create part_to_breakpoint for DELETE")
            return False

        logger.info("DELETE processed successfully: part %s", part_number_before)
        return True

    except KeyError as e:
        logger.error("Missing key in record for DELETE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except ValueError as e:
        logger.error("Invalid value in record for DELETE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except TypeError as e:
        logger.error("Wrong data type in record for DELETE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except AttributeError as e:
        logger.error("Missing attribute for DELETE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except SQLAlchemyError as e:
        logger.error("Database error in DELETE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except Exception as unexpected_error:
        logger.error("Unexpected error processing DELETE action: %s", unexpected_error)
        logger.debug(traceback.format_exc())
        return False


def _process_update_action(
    record: Dict[str, Any],
    service: EntityService,
    breakpoint_id: str,
    model_id: str,
) -> bool:
    """
    Process UPDATE action.
    
    UPDATE: part_no_before NOT NULL, part_no_after NOT NULL, same part_number.
    Creates a new version of the part with all relationships.
    Old version is deactivated for the specified model.
    """
    part_number_before = _get_record_value(record, 'part_no_before')
    part_number_after = _get_record_value(record, 'part_no_after')
    part_name_after = _get_record_value(record, 'part_name_after')
    configuration = _get_record_value(record, 'configuration')
    part_per_vehicle = _get_record_value(record, 'quantity_per_vehicle_after')
    supplier_name_after = _get_record_value(record, 'supplier_name_after')
    localization_after = _get_record_value(record, 'localization_after', 'no data')

    try:
        # Get old_part_id via mapper
        old_part_id = service.get_part_id(part_number_before)
        if not old_part_id:
            logger.error("Old part not found: %s", part_number_before)
            return False

        # Get original_part_id from old part
        stmt = select(PartData.original_part_id).where(PartData.part_id == old_part_id)
        original_part_id = service.connection.execute(stmt).scalar_one_or_none()
        if not original_part_id:
            original_part_id = old_part_id

        # Get supplier_id via service (creates if not exists)
        supplier_id = None
        if supplier_name_after:
            supplier_id = service.ensure_supplier(supplier_name_after, localization_after)
            if not supplier_id:
                logger.warning("Could not create/find supplier %s", supplier_name_after)

        # Create new version of part
        new_part_id = service.create_new_part_version(
            part_number_after,
            original_part_id,
            part_name_after,
            supplier_id,
        )
        if not new_part_id:
            logger.error("Failed to create new version of part %s", part_number_after)
            return False

        # Get configuration_id via service
        configuration_id = service.ensure_configuration(configuration)
        if not configuration_id:
            logger.error("Configuration not found: %s", configuration)
            return False

        # Create all relationships for new version of part (PartToBox, BoxToPallet, PartToLine)
        if not _create_part_relations(service, new_part_id, record, 'after'):
            logger.error("Failed to create relations for new version of part %s", new_part_id)
            return False

        # Activate new version for model
        if not service.activate_part_for_model(
            new_part_id, model_id, configuration_id, part_per_vehicle, breakpoint_id
        ):
            logger.error("Failed to activate new part %s for model", new_part_id)
            return False

        # Deactivate old version for model (only for specified model)
        if not service.deactivate_part_for_model(old_part_id, model_id, breakpoint_id):
            logger.warning("Failed to deactivate old part %s for model", old_part_id)

        # Create record in part_to_breakpoint
        if not service.create_part_to_breakpoint(new_part_id, old_part_id, breakpoint_id, model_id):
            logger.error("Failed to create part_to_breakpoint for UPDATE")
            return False

        logger.info("UPDATE processed successfully: %s -> %s (version)",
                   part_number_before, part_number_after)
        return True

    except KeyError as e:
        logger.error("Missing key in record for UPDATE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except ValueError as e:
        logger.error("Invalid value in record for UPDATE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except TypeError as e:
        logger.error("Wrong data type in record for UPDATE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except AttributeError as e:
        logger.error("Missing attribute for UPDATE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except SQLAlchemyError as e:
        logger.error("Database error in UPDATE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except Exception as unexpected_error:
        logger.error("Unexpected error processing UPDATE action: %s", unexpected_error)
        logger.debug(traceback.format_exc())
        return False


def _process_replace_action(
    record: Dict[str, Any],
    service: EntityService,
    breakpoint_id: str,
    model_id: str,
) -> bool:
    """
    Process REPLACE action.
    
    REPLACE: part_no_before NOT NULL, part_no_after NOT NULL, different part_number.
    First DELETE (deactivate old part), then ADD (create new part).
    """
    part_number_before = _get_record_value(record, 'part_no_before')
    part_number_after = _get_record_value(record, 'part_no_after')

    try:
        # STEP 1: DELETE - deactivate old part
        old_part_id = service.get_part_id(part_number_before)
        if not old_part_id:
            logger.error("Old part not found: %s", part_number_before)
            return False

        # Deactivate old part for model
        if not service.deactivate_part_for_model(old_part_id, model_id, breakpoint_id):
            logger.warning("Failed to deactivate old part %s for model", old_part_id)

        # Create record in part_to_breakpoint for DELETE (new_part_id = NULL)
        if not service.create_part_to_breakpoint(None, old_part_id, breakpoint_id, model_id):
            logger.error("Failed to create part_to_breakpoint for DELETE (REPLACE)")
            return False

        # STEP 2: ADD - create new part
        # Use _process_add_action to create new part with all relationships
        add_success = _process_add_action(record, service, breakpoint_id, model_id)
        if not add_success:
            logger.error("Failed to create new part in REPLACE action")
            return False

        logger.info("REPLACE processed successfully: %s -> %s",
                   part_number_before, part_number_after)
        return True

    except KeyError as e:
        logger.error("Missing key in record for REPLACE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except ValueError as e:
        logger.error("Invalid value in record for REPLACE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except TypeError as e:
        logger.error("Wrong data type in record for REPLACE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except AttributeError as e:
        logger.error("Missing attribute for REPLACE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except SQLAlchemyError as e:
        logger.error("Database error in REPLACE action: %s", e)
        logger.debug(traceback.format_exc())
        return False
    except Exception as unexpected_error:
        logger.error("Unexpected error processing REPLACE action: %s", unexpected_error)
        logger.debug(traceback.format_exc())
        return False


def _process_single_breakpoint(
    breakpoint_number: str,
    breakpoint_record: Dict[str, Any],
    change_records: List[Dict[str, Any]],
    engine: Engine,
    mapper: BPObjectMapper,
) -> Dict[str, Any]:
    """Process a single breakpoint in one transaction."""
    result = {
        'success': False,
        'breakpoint_id': None,
        'records_processed': 0,
        'actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
        'error': None,
    }

    try:
        with engine.begin() as connection:
            logger.info("Processing breakpoint: %s (%d changes)",
                       breakpoint_number, len(change_records))

            if breakpoint_record:
                bp_df = pl.DataFrame([breakpoint_record])
                if not _validate_breakpoint_columns(bp_df, 'breakpoint_data'):
                    raise ValueError(f"Breakpoint {breakpoint_number} has missing required columns")

            service = EntityService(connection, mapper)

            breakpoint_id = service.ensure_breakpoint_with_data(
                breakpoint_number=breakpoint_number,
                status=breakpoint_record.get('status', 'no data'),
                change_date=breakpoint_record.get('change_date'),
                batch_plan=breakpoint_record.get('batch_plan'),
                batch_fact=breakpoint_record.get('batch_fact'),
                description=breakpoint_record.get('description'),
                solution=breakpoint_record.get('solution'),
            )

            if not breakpoint_id:
                raise RuntimeError(f"Failed to ensure breakpoint {breakpoint_number}")

            result['breakpoint_id'] = breakpoint_id

            model_code = None
            for rec in change_records:
                if rec.get('bom_product'):
                    model_code = rec['bom_product']
                    break

            if not model_code:
                raise ValueError(f"No bom_product found for breakpoint {breakpoint_number}")

            model_id = service.ensure_model(model_code)
            if not model_id:
                raise RuntimeError(f"Model not found: {model_code}")

            for record in change_records:
                action = _determine_action_type(record)

                if action == 'UNKNOWN':
                    logger.warning("Unknown action type for record: %s", record)
                    continue

                if not _validate_action_fields(record, action):
                    raise ValueError(f"Action {action} has missing required fields for record: {record}")

                success = False
                if action == BP_ACTION_TYPES['ADD']:
                    success = _process_add_action(record, service, breakpoint_id, model_id)
                elif action == BP_ACTION_TYPES['DELETE']:
                    success = _process_delete_action(record, service, breakpoint_id, model_id)
                elif action == BP_ACTION_TYPES['UPDATE']:
                    success = _process_update_action(record, service, breakpoint_id, model_id)
                elif action == BP_ACTION_TYPES['REPLACE']:
                    success = _process_replace_action(record, service, breakpoint_id, model_id)

                if success:
                    result['records_processed'] += 1
                    result['actions'][action] += 1
                else:
                    raise RuntimeError(f"Failed to process {action} action for record: {record}")

            result['success'] = True
            logger.info("Breakpoint %s processed successfully: %d changes",
                       breakpoint_number, result['records_processed'])

            return result

    except ValueError as e:
        error_msg = str(e)
        result['error'] = error_msg
        logger.error("Validation error for breakpoint %s: %s", breakpoint_number, error_msg)
        logger.debug(traceback.format_exc())
        return result
    except RuntimeError as e:
        error_msg = str(e)
        result['error'] = error_msg
        logger.error("Runtime error for breakpoint %s: %s", breakpoint_number, error_msg)
        logger.debug(traceback.format_exc())
        return result
    except SQLAlchemyError as e:
        error_msg = str(e)
        result['error'] = error_msg
        logger.error("Database error for breakpoint %s: %s", breakpoint_number, error_msg)
        logger.debug(traceback.format_exc())
        return result
    except Exception as unexpected_error:
        error_msg = str(unexpected_error)
        result['error'] = error_msg
        logger.error("Unexpected error for breakpoint %s: %s", breakpoint_number, error_msg)
        logger.debug(traceback.format_exc())
        return result


def _disable_foreign_keys(engine: Engine) -> None:
    """Disable foreign key constraints for performance."""
    try:
        with engine.begin() as connection:
            connection.execute(text('SET session_replication_role = replica;'))
            logger.info("Foreign key constraints disabled.")
    except SQLAlchemyError as e:
        logger.warning("Database error disabling foreign keys: %s", e)
    except Exception as unexpected_error:
        logger.warning("Unexpected error disabling foreign keys: %s", unexpected_error)


def _enable_foreign_keys(engine: Engine) -> None:
    """Enable foreign key constraints back."""
    try:
        with engine.begin() as connection:
            connection.execute(text('SET session_replication_role = DEFAULT;'))
            logger.info("Foreign key constraints enabled.")
    except SQLAlchemyError as e:
        logger.error("Database error enabling foreign keys: %s", e)
        raise RuntimeError(f"Failed to enable foreign keys: {e}") from e
    except Exception as unexpected_error:
        logger.error("Unexpected error enabling foreign keys: %s", unexpected_error)
        raise RuntimeError(f"Unexpected error enabling foreign keys: {unexpected_error}") from unexpected_error


# ============================================================================
# MAIN LOADING FUNCTION
# ============================================================================

def load_bp_pipeline(
    transformed_data: Dict[str, pl.DataFrame],
    engine: Optional[Engine] = None,
    preserve_mapper_cache: bool = False,
) -> Dict[str, Any]:
    """
    Main function to load BP Pipeline data.

    Args:
        transformed_data: Dictionary with 'breakpoint_df' and 'junction_df'
        engine: Optional SQLAlchemy engine
        preserve_mapper_cache: If True, keep mapper cache after loading

    Returns:
        Dict with loading statistics and results
    """
    logger.info("Starting BP Pipeline loading...")

    if engine is None:
        try:
            engine = initialize_database(create_tables=False)
            if not engine:
                logger.error("Failed to initialize database!")
                return {
                    'total_breakpoints': 0,
                    'successful': 0,
                    'failed': 0,
                    'errors': [],
                    'breakpoint_results': {},
                    'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
                }
        except SQLAlchemyError as e:
            logger.error("Database error initializing database: %s", e)
            return {
                'total_breakpoints': 0,
                'successful': 0,
                'failed': 0,
                'errors': [],
                'breakpoint_results': {},
                'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
            }
        except Exception as unexpected_error:
            logger.error("Unexpected error initializing database: %s", unexpected_error)
            return {
                'total_breakpoints': 0,
                'successful': 0,
                'failed': 0,
                'errors': [],
                'breakpoint_results': {},
                'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
            }

    if 'breakpoint_df' not in transformed_data or 'junction_df' not in transformed_data:
        logger.error("Missing required DataFrames: 'breakpoint_df' and 'junction_df'")
        return {
            'total_breakpoints': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
            'breakpoint_results': {},
            'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
        }

    breakpoint_df = transformed_data['breakpoint_df']
    junction_df = transformed_data['junction_df']

    if breakpoint_df.is_empty() or junction_df.is_empty():
        logger.info("No data to load (empty DataFrames)")
        return {
            'total_breakpoints': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
            'breakpoint_results': {},
            'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
        }

    try:
        mapper = create_bp_mapper(engine)
        if mapper is None:
            logger.error("Failed to create BPObjectMapper!")
            return {
                'total_breakpoints': 0,
                'successful': 0,
                'failed': 0,
                'errors': [],
                'breakpoint_results': {},
                'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
            }
    except SQLAlchemyError as e:
        logger.error("Database error creating mapper: %s", e)
        return {
            'total_breakpoints': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
            'breakpoint_results': {},
            'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
        }
    except Exception as unexpected_error:
        logger.error("Unexpected error creating mapper: %s", unexpected_error)
        return {
            'total_breakpoints': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
            'breakpoint_results': {},
            'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
        }

    # Pre-load mapper caches (same as in mft_loader.py)
    try:
        logger.info("Pre-loading ID mappings...")
        mapper.get_breakpoint_mapping()
        mapper.get_part_mapping()
        mapper.get_model_mapping()
        mapper.get_supplier_mapping()
        mapper.get_workshop_mapping()
        mapper.get_line_mapping()
        mapper.get_box_mapping()
        mapper.get_pallet_mapping()
        mapper.get_configuration_mapping()
        mapper.log_mapping_statistics()
    except SQLAlchemyError as e:
        logger.warning("Database error pre-loading mappings: %s", e)
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning("Data error pre-loading mappings: %s", e)
    except Exception as unexpected_error:
        logger.warning("Unexpected error pre-loading mappings: %s", unexpected_error)

    results = {
        'total_breakpoints': 0,
        'successful': 0,
        'failed': 0,
        'errors': [],
        'breakpoint_results': {},
        'total_actions': {'ADD': 0, 'DELETE': 0, 'UPDATE': 0, 'REPLACE': 0},
    }

    try:
        _disable_foreign_keys(engine)

        junction_records = junction_df.to_dicts()
        junction_by_breakpoint = {}

        breakpoint_records = breakpoint_df.to_dicts()
        breakpoint_map = {}
        for bp in breakpoint_records:
            bp_no = bp.get('bp_no')
            if bp_no:
                breakpoint_map[bp_no] = bp

        for record in junction_records:
            bp_no = record.get('bp_no')
            if not bp_no:
                logger.warning("Record missing bp_no: %s", record)
                continue

            if bp_no not in junction_by_breakpoint:
                junction_by_breakpoint[bp_no] = []
            junction_by_breakpoint[bp_no].append(record)

        results['total_breakpoints'] = len(junction_by_breakpoint)

        for bp_no, changes in junction_by_breakpoint.items():
            logger.info("=" * 60)
            logger.info("Processing breakpoint: %s", bp_no)

            bp_record = breakpoint_map.get(bp_no, {})
            if not bp_record:
                logger.warning("No breakpoint data found for %s, using minimal data", bp_no)
                bp_record = {'bp_no': bp_no}

            bp_result = _process_single_breakpoint(
                bp_no, bp_record, changes, engine, mapper
            )

            results['breakpoint_results'][bp_no] = bp_result

            if bp_result['success']:
                results['successful'] += 1
                for action, count in bp_result['actions'].items():
                    results['total_actions'][action] += count
            else:
                results['failed'] += 1
                results['errors'].append({
                    'breakpoint_number': bp_no,
                    'error': bp_result.get('error', 'Unknown error'),
                    'breakpoint_id': bp_result.get('breakpoint_id'),
                })

            logger.info("Breakpoint %s: %s (processed: %d changes)",
                       bp_no, "SUCCESS" if bp_result['success'] else "FAILED",
                       bp_result['records_processed'])

    except SQLAlchemyError as e:
        logger.error("Database error in BP Pipeline: %s", e)
        logger.debug(traceback.format_exc())
    except ValueError as e:
        logger.error("Validation error in BP Pipeline: %s", e)
        logger.debug(traceback.format_exc())
    except RuntimeError as e:
        logger.error("Runtime error in BP Pipeline: %s", e)
        logger.debug(traceback.format_exc())
    except Exception as unexpected_error:
        logger.error("Unexpected error in BP Pipeline: %s", unexpected_error)
        logger.debug(traceback.format_exc())

    finally:
        try:
            _enable_foreign_keys(engine)
        except (SQLAlchemyError, RuntimeError) as e:
            logger.error("Error re-enabling foreign keys: %s", e)
        except Exception as unexpected_error:
            logger.error("Unexpected error re-enabling foreign keys: %s", unexpected_error)

        if not preserve_mapper_cache:
            try:
                mapper.clear_cache()
                logger.debug("Cleared mapper cache")
            except Exception as unexpected_error:
                logger.warning("Error clearing mapper cache: %s", unexpected_error)

    logger.info("=" * 60)
    logger.info("BP PIPELINE LOADING COMPLETED")
    logger.info("=" * 60)
    logger.info("Total breakpoints: %d", results['total_breakpoints'])
    logger.info("Successful: %d", results['successful'])
    logger.info("Failed: %d", results['failed'])
    logger.info("Total actions:")
    for action, count in results['total_actions'].items():
        logger.info("  %s: %d", action, count)

    if results['errors']:
        logger.warning("Errors occurred in %d breakpoints:", len(results['errors']))
        for error in results['errors']:
            logger.warning("  - Breakpoint %s: %s",
                          error['breakpoint_number'], error['error'])

    logger.info("=" * 60)

    return results


def create_bp_loader(
    engine: Optional[Engine] = None
) -> Callable[[Dict[str, pl.DataFrame]], Dict[str, Any]]:
    """
    Factory to create BP loading function with pre-configured engine.

    Args:
        engine: Optional SQLAlchemy engine

    Returns:
        BP loader function

    Raises:
        RuntimeError: If database initialization fails
    """
    if engine is None:
        engine = initialize_database(create_tables=False)
        if not engine:
            raise RuntimeError("Failed to initialize database!")

    def loader(transformed_data: Dict[str, pl.DataFrame]) -> Dict[str, Any]:
        return load_bp_pipeline(transformed_data, engine)

    logger.info("BP Loader created successfully")
    return loader
