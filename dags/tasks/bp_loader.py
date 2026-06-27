"""
Breakpoint Loader Module for Material Flow Table Database.

This module handles the actual database loading of breakpoint data.
It receives prepared data from mapper and performs the actual commits
with proper transaction management and soft delete support.

Key Features:
    - Two-phase loading: breakpoint_data first, then part_to_breakpoint
    - Foreign key management for bulk operations
    - Soft delete support for parts (is_active = False)
    - Part-to-line relationship management
    - Comprehensive error handling and rollback
    - Statistics tracking for all operations

Maintainer: PLD Engineering Center
Version: 1.0.0
Compatibility: Python 3.12.3+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Created: 2026-01-12
Last Modified: 2025-03-13
License: MIT
Status: Production
"""

import logging
from typing import Dict, List, Tuple, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, text
from sqlalchemy.engine import Engine

from database.database import (
    BreakpointData, PartData, LineData, SupplierData,
    PartToBreakpoint, PartToLine
)
from dags.tasks.connector import initialize_database

logger = logging.getLogger(__name__)


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
            logger.info("Foreign key constraints disabled for breakpoint loading.")

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
            logger.info("Foreign key constraints enabled for breakpoint loading.")

    except SQLAlchemyError as e:
        logger.error("Could not enable foreign keys due to SQLAlchemy error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error while enabling foreign keys: %s", e)
        raise


class BreakpointLoader:
    """
    Loader class for breakpoint data.
    Handles actual database operations with proper transaction management.
    """
    
    def __init__(self, db_session: Session, engine: Optional[Engine] = None):
        """
        Initialize loader with database session.
        
        Args:
            db_session: SQLAlchemy Session for database operations
            engine: SQLAlchemy Engine for foreign key management (optional)
        """
        self.session = db_session
        self.engine = engine
        self.stats = {
            'inserted_breakpoints': 0,
            'inserted_part_to_breakpoint': 0,
            'soft_deleted_parts': 0,
            'updated_parts': 0,
            'created_part_to_line': 0,
            'removed_part_to_line': 0,
            'errors': 0,
            'warnings': 0
        }
        logger.debug("BreakpointLoader initialized")
    
    def _get_engine(self) -> Optional[Engine]:
        """
        Get engine from session or stored engine.
        
        Returns:
            SQLAlchemy Engine or None
        """
        if self.engine:
            return self.engine
        try:
            return self.session.get_bind()
        except AttributeError:
            return None
    
    def load_breakpoint_data(
        self,
        breakpoint_records: List[Dict[str, Any]]
    ) -> Tuple[bool, List[str]]:
        """
        Load breakpoint data to database.
        
        Args:
            breakpoint_records: List of dictionaries with mapped breakpoint data
            
        Returns:
            Tuple of (success, list_of_errors)
        """
        errors = []
        
        try:
            # Filter only new breakpoints
            new_breakpoints = [
                r for r in breakpoint_records 
                if not r.get('_exists_in_db', False)
            ]
            
            if not new_breakpoints:
                logger.info("No new breakpoints to insert")
                return True, errors
            
            # Bulk insert new breakpoints
            for record in new_breakpoints:
                breakpoint = BreakpointData(
                    breakpoint_number=record['breakpoint_number'],
                    breakpoint_date=record['breakpoint_date'],
                    description=record.get('description'),
                    batch=record.get('batch')
                )
                self.session.add(breakpoint)
                self.stats['inserted_breakpoints'] += 1
            
            logger.info(
                "Prepared %d breakpoints for insertion",
                self.stats['inserted_breakpoints']
            )
            
        except (KeyError, ValueError, TypeError) as e:
            errors.append(f"Data error preparing breakpoint data: {e}")
            self.stats['errors'] += 1
            return False, errors
        except Exception as e:
            errors.append(f"Failed to prepare breakpoint data: {e}")
            self.stats['errors'] += 1
            return False, errors
        
        return True, errors
    
    def load_part_to_breakpoint_data(
        self,
        ptb_records: List[Dict[str, Any]],
        temp_records: Dict
    ) -> Tuple[bool, List[str]]:
        """
        Load part_to_breakpoint data and handle related updates.
        
        Args:
            ptb_records: List of dictionaries with mapped part_to_breakpoint data
            temp_records: Dictionary of temporary records created by mapper
            
        Returns:
            Tuple of (success, list_of_errors)
        """
        errors = []
        engine = self._get_engine()
        
        try:
            # Disable foreign keys for bulk operations if engine available
            if engine:
                disable_foreign_keys(engine)
            
            for record in ptb_records:
                action = record.get('action')
                
                # Handle different actions
                if action == 'replace':
                    self._handle_replace_load(record)
                elif action == 'delete':
                    self._handle_delete_load(record)
                elif action == 'add':
                    self._handle_add_load(record)
                elif action == 'update':
                    self._handle_update_load(record)
                elif action == 'no data':
                    logger.warning(
                        "Skipping manual review record for part: %s",
                        record.get('part_number_before_change')
                    )
                    self.stats['warnings'] += 1
                    continue
                else:
                    logger.warning(
                        "Unknown action type: %s, skipping",
                        action
                    )
                    self.stats['warnings'] += 1
                    continue
                
                # Always create part_to_breakpoint record
                ptb = PartToBreakpoint(
                    part_id=record['part_id'],
                    breakpoint_id=record['breakpoint_id'],
                    model_id=record['model_id'],
                    supplier_id=record.get('supplier_id'),
                    line_id=record.get('line_id'),
                    action=record['action'],
                    part_number_before_change=record.get('part_number_before_change'),
                    supplier_name_before_change=record.get('supplier_name_before_change'),
                    localization_before_change=record.get('localization_before_change'),
                    line_name_before_change=record.get('line_name_before_change')
                )
                self.session.add(ptb)
                self.stats['inserted_part_to_breakpoint'] += 1
            
            # Commit all changes
            self.session.commit()
            logger.info(
                "Successfully loaded %d part_to_breakpoint records",
                self.stats['inserted_part_to_breakpoint']
            )
            
        except IntegrityError as e:
            self.session.rollback()
            errors.append(f"Integrity error: {e}")
            self.stats['errors'] += 1
            logger.error("Integrity error details: %s", e)
        except SQLAlchemyError as e:
            self.session.rollback()
            errors.append(f"Database error: {e}")
            self.stats['errors'] += 1
            logger.error("Database error details: %s", e)
        except Exception as e:
            self.session.rollback()
            errors.append(f"Unexpected error: {e}")
            self.stats['errors'] += 1
            logger.error("Unexpected error details: %s", e, exc_info=True)
        finally:
            # Always re-enable foreign keys
            if engine:
                try:
                    enable_foreign_keys(engine)
                except Exception as fk_error:
                    errors.append(f"Failed to re-enable foreign keys: {fk_error}")
                    logger.error("Failed to re-enable foreign keys: %s", fk_error)
        
        return len(errors) == 0, errors
    
    def _handle_replace_load(self, row: Dict):
        """
        Handle database operations for REPLACE action.
        
        Args:
            row: Mapped record with metadata for REPLACE action
        """
        # New part already created by mapper, ensure relationships
        if row.get('_new_part_id') and row.get('line_id'):
            # Check if part-to-line relationship already exists
            existing_ptl = self.session.query(PartToLine).filter(
                PartToLine.part_id == row['_new_part_id'],
                PartToLine.line_id == row['line_id']
            ).first()
            
            if not existing_ptl:
                ptl = PartToLine(
                    part_id=row['_new_part_id'],
                    line_id=row['line_id']
                )
                self.session.add(ptl)
                self.stats['created_part_to_line'] += 1
                logger.debug(
                    "Created PartToLine relation for new part %s and line %s",
                    row['_new_part_id'],
                    row['line_id']
                )
        
        # Update old part supplier if needed (soft update)
        if row.get('_update_part_supplier') and row.get('part_id') and row.get('supplier_id'):
            part = self.session.query(PartData).get(row['part_id'])
            if part and part.supplier_id != row['supplier_id']:
                part.supplier_id = row['supplier_id']
                self.stats['updated_parts'] += 1
                logger.debug(
                    "Updated supplier for part %s to %s",
                    row['part_id'],
                    row['supplier_id']
                )
    
    def _handle_delete_load(self, row: Dict):
        """
        Handle database operations for DELETE action with soft delete.
        
        Args:
            row: Mapped record with metadata for DELETE action
        """
        # Soft delete the part
        if row.get('_part_to_deactivate'):
            part = self.session.query(PartData).get(row['_part_to_deactivate'])
            if part and part.is_active:
                part.is_active = False
                part.deactivated_at = func.now()
                part.deactivated_by_breakpoint_id = row.get('_deactivation_breakpoint_id')
                self.stats['soft_deleted_parts'] += 1
                logger.info(
                    "Part %s (ID: %s) soft deleted by breakpoint %s",
                    part.part_number,
                    part.part_id,
                    row.get('_deactivation_breakpoint_id')
                )
            elif part and not part.is_active:
                logger.debug(
                    "Part %s already inactive, skipping soft delete",
                    part.part_number
                )
        
        # Remove PartToLine relations
        if row.get('_remove_part_line_relations') and row.get('part_id'):
            part_to_lines = self.session.query(PartToLine).filter(
                PartToLine.part_id == row['part_id']
            ).all()
            
            removed_count = 0
            for ptl in part_to_lines:
                self.session.delete(ptl)
                removed_count += 1
            
            if removed_count > 0:
                self.stats['removed_part_to_line'] += removed_count
                logger.debug(
                    "Removed %d PartToLine relations for part %s",
                    removed_count,
                    row['part_id']
                )
    
    def _handle_add_load(self, row: Dict):
        """
        Handle database operations for ADD action.
        
        Args:
            row: Mapped record with metadata for ADD action
        """
        # Create part-to-line relationship for new part
        if row.get('_create_part_line_relation') and row.get('line_id') and row.get('_new_part_id'):
            # Check if relationship already exists
            existing_ptl = self.session.query(PartToLine).filter(
                PartToLine.part_id == row['_new_part_id'],
                PartToLine.line_id == row['line_id']
            ).first()
            
            if not existing_ptl:
                ptl = PartToLine(
                    part_id=row['_new_part_id'],
                    line_id=row['line_id']
                )
                self.session.add(ptl)
                self.stats['created_part_to_line'] += 1
                logger.debug(
                    "Created PartToLine relation for new part %s and line %s",
                    row['_new_part_id'],
                    row['line_id']
                )
    
    def _handle_update_load(self, row: Dict):
        """
        Handle database operations for UPDATE action.
        
        Args:
            row: Mapped record with metadata for UPDATE action
        """
        # Update supplier if changed
        if row.get('_update_supplier') and row.get('part_id') and row.get('supplier_id'):
            part = self.session.query(PartData).get(row['part_id'])
            if part and part.supplier_id != row['supplier_id']:
                part.supplier_id = row['supplier_id']
                self.stats['updated_parts'] += 1
                logger.debug(
                    "Updated supplier for part %s to %s",
                    row['part_id'],
                    row['supplier_id']
                )
        
        # Update line relationship if changed
        if row.get('_update_line') and row.get('part_id') and row.get('line_id'):
            # Check if relationship already exists
            existing = self.session.query(PartToLine).filter(
                PartToLine.part_id == row['part_id'],
                PartToLine.line_id == row['line_id']
            ).first()
            
            if not existing:
                ptl = PartToLine(
                    part_id=row['part_id'],
                    line_id=row['line_id']
                )
                self.session.add(ptl)
                self.stats['created_part_to_line'] += 1
                logger.debug(
                    "Created PartToLine relation for part %s and line %s",
                    row['part_id'],
                    row['line_id']
                )
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get loading statistics.
        
        Returns:
            Dictionary with loading statistics
        """
        return self.stats.copy()
    
    def log_statistics(self) -> None:
        """
        Log current loading statistics.
        """
        logger.info("=" * 60)
        logger.info("BREAKPOINT LOADING STATISTICS")
        logger.info("=" * 60)
        logger.info("Inserted breakpoints: %d", self.stats['inserted_breakpoints'])
        logger.info("Inserted part_to_breakpoint: %d", self.stats['inserted_part_to_breakpoint'])
        logger.info("Soft deleted parts: %d", self.stats['soft_deleted_parts'])
        logger.info("Updated parts: %d", self.stats['updated_parts'])
        logger.info("Created PartToLine relations: %d", self.stats['created_part_to_line'])
        logger.info("Removed PartToLine relations: %d", self.stats['removed_part_to_line'])
        logger.info("Errors: %d", self.stats['errors'])
        logger.info("Warnings: %d", self.stats['warnings'])
        logger.info("=" * 60)
    
    def reset_stats(self) -> None:
        """
        Reset loading statistics.
        """
        self.stats = {
            'inserted_breakpoints': 0,
            'inserted_part_to_breakpoint': 0,
            'soft_deleted_parts': 0,
            'updated_parts': 0,
            'created_part_to_line': 0,
            'removed_part_to_line': 0,
            'errors': 0,
            'warnings': 0
        }
        logger.debug("BreakpointLoader statistics reset")


def create_breakpoint_loader(engine=None) -> BreakpointLoader:
    """
    Factory function to create BreakpointLoader.
    
    Args:
        engine: Optional SQLAlchemy database engine (new one created if None)
        
    Returns:
        BreakpointLoader instance ready for use
        
    Raises:
        SQLAlchemyError: If database connection fails
        RuntimeError: If loader cannot be created
    """
    try:
        from sqlalchemy.orm import sessionmaker
        
        if engine is None:
            engine = initialize_database(create_tables=False)
        
        session_factory = sessionmaker(bind=engine)
        session = session_factory()
        
        loader = BreakpointLoader(session, engine)
        logger.info("BreakpointLoader created successfully")
        
        return loader
        
    except SQLAlchemyError as e:
        logger.error("Database error creating breakpoint loader: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error creating breakpoint loader: %s", e)
        raise RuntimeError(f"Failed to create breakpoint loader: {e}") from e