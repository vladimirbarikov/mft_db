# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
MFT Manual Modification API Module.

PURPOSE:
    Provides REST API endpoints for making manual changes to part data with
    full audit trail through virtual breakpoints. This closes the critical
    architectural gap where manual changes were not tracked in the history.

KEY FEATURES:
    1. Virtual Breakpoints (MAN-YYYYMMDD-XXXX):
        - Each manual change automatically creates a virtual breakpoint
        - Full integration with existing breakpoint architecture
        - Easy to distinguish from automatic changes (MAN-* vs BP-*)

    2. Complete History Tracking:
        - Every change (automatic OR manual) has a breakpoint_id
        - Single unified history view via part_to_breakpoint table
        - Source tracking: 'manual' vs 'automatic' change_type

    3. Version Rollback:
        - Rollback to ANY historical version
        - Creates new virtual breakpoint for the rollback
        - Preserves full audit trail of the rollback itself

    4. Smart Change Detection:
        - No new version created if attributes didn't actually change
        - Prevents "noise" versions from bloating the database
        - Saves storage and keeps history clean

    5. Atomic Group Changes:
        - All changes in one request = ONE transaction
        - ONE breakpoint_id for the entire group
        - Atomic rollback of the entire group

    6. Security & Access Control:
        - JWT authentication required for all endpoints
        - Role-based access control (admin, editor, viewer)
        - Rate limiting to prevent abuse
        - Full audit log: who, when, why

    7. Universal API Endpoint:
        - One endpoint for ALL attribute changes
        - Unified interface for all modification types
        - No need to understand internal database structure

    8. Zero Schema Changes:
        - Uses existing tables: breakpoint_data, part_data,
          part_to_breakpoint, part_to_model
        - Complete compatibility with BP pipeline
        - No database migrations required

BUSINESS VALUE:
    - Before: Manual changes were lost in history (no breakpoint_id)
    - After: ALL changes (auto + manual) have full audit trail
    - Full traceability of every modification
    - Ability to rollback ANY change at ANY time
    - Single source of truth for all changes

ENDPOINTS OVERVIEW:
    POST   /api/v1/parts/{part_number}/modify
        - Main endpoint for ALL manual changes
        - Creates virtual breakpoint automatically
        - Validates fields via Marshmallow schema

    GET    /api/v1/parts/{part_number}/history
        - Full history with source tracking
        - Shows both manual and automatic changes
        - Filters by model_code

    POST   /api/v1/parts/{part_number}/versions/{version}/rollback
        - Rollback to any historical version
        - Creates new virtual breakpoint
        - Preserves full audit trail

    GET    /api/v1/parts/{part_number}/versions
        - Simplified list of versions
        - Shows active/inactive status

    GET    /api/v1/health
        - Health check endpoint (no auth required)

    POST   /api/v1/auth/token
        - JWT token generation (for testing only)

DEPENDENCIES:
    Flask 3.0.3+         - Web framework
    Flask-Limiter 3.12+  - Rate limiting
    flask-cors 6.0.2+    - CORS support
    marshmallow 3.20.0+  - Request validation
    pyjwt 2.8.0+         - JWT authentication
    SQLAlchemy 1.4.54+   - Database ORM

Version: 1.0.0
Compatibility: Python 3.14.4+, Flask 3.0.3+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-19
License: MIT
Status: Development
"""
# Standard library imports
from pathlib import Path
import sys
import os
from datetime import datetime
from functools import wraps
from typing import Dict, Any, Optional, cast
import zoneinfo

# Third-party imports
from dotenv import load_dotenv
from flask import Flask, Blueprint, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from marshmallow import Schema, fields, validate, ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    PROJECT_ROOT = Path("/opt/airflow")

# Add project root to path if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

# Local imports
from config import get_logger
from dags.tasks.connector import initialize_database
from dags.tasks.change_classifier import ChangeClassifier
from endpoints.auth import (
    jwt_required,
    role_required,
    get_current_email,
    get_current_username,
    ROLES,
    generate_token,
)
from database.database import (
    # Entity tables
    SupplierData, PartData, BoxData, PalletData, ModelData,
    ConfigurationData, WorkshopData, LineData, BreakpointData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel,
    PartToLine, PartToBreakpoint,
)
from database.views import PartHistoryView, ActivePartsFullView


# Logger setup
logger = get_logger(__name__)

# ========== TIMEZONE ==========
MOSCOW_TZ = zoneinfo.ZoneInfo("Europe/Moscow")

# ========== CONFIGURATION ==========
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
if not FLASK_SECRET_KEY:
    raise RuntimeError("FLASK_SECRET_KEY must be set in .env file")

FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('MFT_MODIFY_API_PORT', '5004'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
IS_PRODUCTION = FLASK_ENV == 'production'

ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')
RATE_LIMIT = os.getenv('RATE_LIMIT', '10 per minute')
RATE_LIMIT_STORAGE_URL = os.getenv('RATE_LIMIT_STORAGE_URL', 'memory://')

# ========== CREATING BLUEPRINT ==========
modify_bp = Blueprint('modify', __name__, url_prefix='/api/v1')

# ========== RATE LIMITING SETUP ==========
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=RATE_LIMIT_STORAGE_URL,
    default_limits=["200 per day", "50 per hour"],
    strategy="fixed-window"
)


def rate_limit(limit_string: Optional[str] = None):
    """Decorator factory for applying rate limits to endpoints."""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return limiter.limit(limit_string or RATE_LIMIT)(f)(*args, **kwargs)
        return wrapped
    return decorator


# ============================================================================
# MARSHMALLOW SCHEMAS
# ============================================================================

class PartChangeSchema(Schema):
    """Request schema for part modification."""
    model_code = fields.Str(
        required=True,
        validate=validate.OneOf(
            ['jolion', 'h3', 'f7', 'f7x', 'dargo', 'h7',
             'a01', 'a08', 'b02', 'b04', 'b06', 'b16'],
            error="Invalid model_code"
        )
    )
    changes = fields.Dict(
        required=True,
        validate=validate.Length(min=1, error="At least one change required")
    )
    change_reason = fields.Str(
        required=True,
        validate=validate.Length(min=5, max=500)
    )
    ticket_number = fields.Str(
        required=False,
        allow_none=True,
        validate=validate.Length(max=50)
    )

    @staticmethod
    def _get_type_name(type_spec):
        """Get readable type name from type specification."""
        if isinstance(type_spec, tuple):
            return ' or '.join(t.__name__ for t in type_spec)
        return type_spec.__name__

    @staticmethod
    def validate_changes(data):
        """
        Validate changes contain only allowed fields and correct data types.
        
        Типы данных соответствуют моделям из database.py:
        - String поля: str
        - Numeric поля: float (int или float)
        - Integer поля: int
        - ENUM поля: str (проверка на допустимые значения)
        """
        allowed_fields = {
            # PartData (String)
            'part_name': {'type': str, 'max_length': 100},

            # PartData (Numeric)
            'part_weight_kg': {'type': (int, float), 'min': 0},

            # SupplierData (String + ENUM)
            'supplier_name': {'type': str, 'max_length': 200},
            'localization': {'type': str, 'enum': ['yes', 'no', 'no data']},

            # BoxData (ENUM + Integer + Numeric)
            'box_type': {'type': str, 'enum': ['returnable', 'non-returnable', 'no data']},
            'box_length_mm': {'type': int, 'min': 1},
            'box_width_mm': {'type': int, 'min': 1},
            'box_height_mm': {'type': int, 'min': 1},
            'box_weight_kg': {'type': (int, float), 'min': 0},
            'box_stacking': {'type': int, 'min': 0},

            # PalletData (ENUM + Integer + Numeric)
            'pallet_type': {'type': str, 'enum': ['returnable', 'non-returnable', 'no data']},
            'pallet_length_mm': {'type': int, 'min': 1},
            'pallet_width_mm': {'type': int, 'min': 1},
            'pallet_height_mm': {'type': int, 'min': 1},
            'pallet_weight_kg': {'type': (int, float), 'min': 0},
            'pallet_stacking': {'type': int, 'min': 0},

            # LineData (String)
            'line_code': {'type': str, 'max_length': 10},
            'line_name': {'type': str, 'max_length': 50},

            # WorkshopData (ENUM)
            'workshop_code': {
                'type': str,
                'enum': ['as', 'comp', 'paint', 'weld', 'stamp', 'engine', 'no data']
            },

            # ConfigurationData (ENUM + String)
            'configuration': {
                'type': str,
                'enum': ['comfort', 'elite', 'tech-plus', 'premium', 'no data']
            },
            'transmission': {'type': str, 'max_length': 100},

            # Junction tables (Integer > 0)
            'part_per_vehicle': {'type': int, 'min': 1},
            'part_per_box': {'type': int, 'min': 1},
            'box_per_pallet': {'type': int, 'min': 1},
        }

        changes = data.get('changes', {})

        # 1. Check for invalid fields
        invalid_fields = set(changes.keys()) - set(allowed_fields.keys())
        if invalid_fields:
            raise ValidationError(
                f"Invalid fields: {', '.join(invalid_fields)}. "
                f"Allowed fields: {', '.join(sorted(allowed_fields.keys()))}"
            )

        # 2. Checking the types and values for each field
        errors = []
        for field, value in changes.items():
            rule = allowed_fields[field]

            # Check for None
            if value is None:
                errors.append(f"Field '{field}' cannot be None")
                continue

            # Type verification
            expected_type = rule['type']
            if not isinstance(value, expected_type):
                # For float allow int
                if expected_type == (int, float) and isinstance(value, (int, float)):
                    pass
                else:
                    errors.append(
                        f"Field '{field}' must be of type {PartChangeSchema._get_type_name(expected_type)}, "
                        f"got {type(value).__name__}"
                    )
                    continue

            # Length check for strings
            if 'max_length' in rule and isinstance(value, str):
                if len(value) > rule['max_length']:
                    errors.append(
                        f"Field '{field}' exceeds maximum length {rule['max_length']} "
                        f"(got {len(value)} characters)"
                    )

            # Checking ENUM values
            if 'enum' in rule and isinstance(value, str):
                if value not in rule['enum']:
                    errors.append(
                        f"Field '{field}' must be one of: {', '.join(rule['enum'])}, "
                        f"got '{value}'"
                    )

            # Checking the minimum value
            if 'min' in rule:
                try:
                    if value < rule['min']:
                        errors.append(
                            f"Field '{field}' must be >= {rule['min']}, got {value}"
                        )
                except TypeError:
                    errors.append(
                        f"Field '{field}' cannot be compared with {rule['min']} "
                        f"(type: {type(value).__name__})"
                    )

        if errors:
            raise ValidationError("; ".join(errors))

        return data


class RollbackSchema(Schema):
    """Request schema for rollback."""
    model_code = fields.Str(
        required=True,
        validate=validate.OneOf(
            ['jolion', 'h3', 'f7', 'f7x', 'dargo', 'h7',
             'a01', 'a08', 'b02', 'b04', 'b06', 'b16']
        )
    )
    reason = fields.Str(required=False, allow_none=True, validate=validate.Length(max=500))
    ticket_number = fields.Str(required=False, allow_none=True, validate=validate.Length(max=50))


# ============================================================================
# CORE SERVICE
# ============================================================================

class ManualChangeService:
    """Service for handling manual part changes with virtual breakpoints."""

    def __init__(self, session: Session):
        self.session = session

    def create_virtual_breakpoint(
        self,
        part_number: str,
        model_code: str,
        change_reason: str,
        ticket_number: Optional[str],
        changed_by: str,
        changes: Dict[str, Any],
        change_domain: str,
        change_nature: str
    ) -> BreakpointData:
        """Create a virtual breakpoint for manual changes."""
        today = datetime.now(MOSCOW_TZ).strftime('%Y%m%d')

        count_query = text("""
            SELECT COUNT(*)
            FROM breakpoint_data
            WHERE breakpoint_number LIKE :pattern
        """)
        pattern = f"MAN-{today}-%"
        count = self.session.execute(count_query, {'pattern': pattern}).scalar() or 0
        seq = str(count + 1).zfill(4)
        breakpoint_number = f"MAN-{today}-{seq}"

        changes_summary = ', '.join([f"{k}={v}" for k, v in changes.items()])
        description = f"""MANUAL CHANGE
            Part: {part_number}
            Model: {model_code}
            Domain: {change_domain}
            Nature: {change_nature}
            Changes: {changes_summary}
            Reason: {change_reason}
            Ticket: {ticket_number or 'N/A'}
            Changed by: {changed_by}"""

        # Create a BreakpointData object by assigning attributes
        bp = BreakpointData()
        bp.breakpoint_number = breakpoint_number
        bp.breakpoint_status = 'closed'
        bp.breakpoint_date = datetime.now(MOSCOW_TZ)
        bp.description = description.strip()
        bp.solution = f"Manual change by {changed_by}"
        bp.change_domain = change_domain
        bp.change_nature = change_nature

        self.session.add(bp)
        self.session.flush()

        logger.info(
            "Created virtual breakpoint %s (domain=%s, nature=%s)",
            breakpoint_number, change_domain, change_nature
        )

        return bp

    def get_active_version(self, part_number: str, model_code: str) -> Optional[Dict[str, Any]]:
        """
        Get currently active version of part for model using ActivePartsFullView.
        
        Использует представление v_active_parts_full, которое уже содержит
        все необходимые данные и фильтрует только активные записи.
        """
        part = self.session.query(ActivePartsFullView).where(
            ActivePartsFullView.part_number == part_number,
            ActivePartsFullView.model_code == model_code,
            ActivePartsFullView.is_active.is_(True)
        ).first()

        if part:
            return {
                'part_id': part.part_id,
                'part_number': part.part_number,
                'version_number': part.version_number,
                'part_name': part.part_name,
                'part_weight_kg': float(part.part_weight_kg) if part.part_weight_kg else None,
                'supplier_id': part.supplier_id,
                'original_part_id': part.original_part_id,
                'created_at': part.created_at,
                'supplier_name': part.supplier_name,
                'configuration_id': part.configuration_id,
                'part_per_vehicle': part.part_per_vehicle,
            }
        return None

    def get_part_attributes(self, part_id: str) -> Dict[str, Any]:
        """
        Get all attributes of a part version using ActivePartsFullView.
        
        Использует представление v_active_parts_full для получения всех
        атрибутов детали в денормализованном виде.
        """
        part = self.session.query(ActivePartsFullView).where(
            ActivePartsFullView.part_id == part_id
        ).first()

        if not part:
            return {}

        return {
            'part_number': part.part_number,
            'part_name': part.part_name,
            'part_weight_kg': float(part.part_weight_kg) if part.part_weight_kg else None,
            'supplier_name': part.supplier_name,
            'localization': part.localization,
            'box_type': part.box_type,
            'box_length_mm': part.box_length_mm,
            'box_width_mm': part.box_width_mm,
            'box_height_mm': part.box_height_mm,
            'box_weight_kg': float(part.box_weight_kg) if part.box_weight_kg else None,
            'box_stacking': part.box_stacking,
            'pallet_type': part.pallet_type,
            'pallet_length_mm': part.pallet_length_mm,
            'pallet_width_mm': part.pallet_width_mm,
            'pallet_height_mm': part.pallet_height_mm,
            'pallet_weight_kg': float(part.pallet_weight_kg) if part.pallet_weight_kg else None,
            'pallet_stacking': part.pallet_stacking,
            'line_code': part.line_code,
            'line_name': part.line_name,
            'workshop_code': part.workshop_code,
            'configuration': part.configuration,
            'transmission': part.transmission,
            'part_per_vehicle': part.part_per_vehicle,
            'part_per_box': part.part_per_box,
            'box_per_pallet': part.box_per_pallet,
            'box_id': part.box_id,
            'pallet_id': part.pallet_id,
            'configuration_id': part.configuration_id,
        }

    def apply_changes(
        self,
        part_number: str,
        model_code: str,
        changes: Dict[str, Any],
        bp: BreakpointData,
        changed_by: str
    ) -> Dict[str, Any]:
        """Apply changes to part, creating new version if needed."""
        # 1. Get current active version
        current = self.get_active_version(part_number, model_code)
        if not current:
            raise ValueError(f"Part {part_number} not found for model {model_code}")

        old_part_id = current['part_id']
        old_version = current['version_number']

        # 2. Get current attributes
        current_attrs = self.get_part_attributes(old_part_id)

        # 3. Detect actual changes
        actual_changes = {}
        for field, new_value in changes.items():
            current_value = current_attrs.get(field)

            if current_value is None and new_value is None:
                continue
            if current_value is None or new_value is None:
                actual_changes[field] = new_value
                continue

            if str(current_value) != str(new_value):
                actual_changes[field] = new_value

        if not actual_changes:
            logger.info("No actual changes detected for part %s", part_number)
            return {
                'old_part_id': old_part_id,
                'new_part_id': old_part_id,
                'old_version': old_version,
                'new_version': old_version,
                'changes_applied': {},
                'is_new_version': False
            }

        # 4. Create new version
        new_part = self._create_new_version(
            old_part_id,
            part_number,
            actual_changes,
            current_attrs
        )

        new_part_id = new_part.part_id
        new_version = new_part.version_number

        # 5. Get model and config IDs
        model_id = self._get_model_id(model_code)
        config_id = self._get_configuration_id(actual_changes.get('configuration', current_attrs.get('configuration')))

        # 6. Deactivate old version
        self._deactivate_part_for_model(old_part_id, model_id, bp.breakpoint_id)

        # 7. Activate new version
        self._activate_part_for_model(
            new_part_id,
            model_id,
            config_id,
            actual_changes.get('part_per_vehicle', current_attrs.get('part_per_vehicle'))
        )

        # 8. Create transition record
        self._create_transition(new_part_id, old_part_id, bp.breakpoint_id, model_id)

        logger.info(
            "User %s applied changes to part %s: v%d → v%d (breakpoint: %s)",
            changed_by, part_number, old_version, new_version, bp.breakpoint_number
        )

        return {
            'old_part_id': old_part_id,
            'new_part_id': new_part_id,
            'old_version': old_version,
            'new_version': new_version,
            'changes_applied': actual_changes,
            'is_new_version': True,
            'changed_by': changed_by
        }

    def _create_new_version(
        self,
        old_part_id: str,
        part_number: str,
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> PartData:
        """Create a new version of the part."""
        old_part = self.session.get(PartData, old_part_id)
        if old_part is None:
            raise ValueError(f"Part version {old_part_id} not found")

        old_part = cast(PartData, old_part)

        original_part_id = old_part.original_part_id or old_part_id

        # Get next version number with lock
        version_query = text("""
            SELECT COALESCE(MAX(version_number), 0) + 1
            FROM part_data
            WHERE original_part_id = :original_part_id
            FOR UPDATE
        """)
        new_version = self.session.execute(
            version_query,
            {'original_part_id': original_part_id}
        ).scalar() or 1

        # Build new part data
        new_part_data = {
            'part_number': part_number,
            'original_part_id': original_part_id,
            'version_number': new_version,
            'part_name': changes.get('part_name', old_part.part_name),
            'part_weight_kg': changes.get('part_weight_kg', old_part.part_weight_kg),
        }

        # Handle supplier
        if 'supplier_name' in changes or 'localization' in changes:
            supplier_name = changes.get('supplier_name')
            if supplier_name is None:
                supplier_name = current_attrs.get('supplier_name')

            if not supplier_name:
                raise ValueError("supplier_name is required when changing supplier")

            localization = changes.get('localization')
            if localization is None:
                localization = current_attrs.get('localization', 'no data')

            new_part_data['supplier_id'] = self._ensure_supplier(str(supplier_name), str(localization))
        else:
            new_part_data['supplier_id'] = old_part.supplier_id

        new_part = PartData(**new_part_data)
        self.session.add(new_part)
        self.session.flush()

        # Handle box
        if any(k.startswith('box_') for k in changes.keys()) or 'box_type' in changes:
            box_id = self._ensure_box(changes)
            if box_id:
                part_per_box = changes.get('part_per_box', current_attrs.get('part_per_box'))
                self._ensure_part_to_box(new_part.part_id, box_id, part_per_box)

        # Handle pallet
        if any(k.startswith('pallet_') for k in changes.keys()) or 'pallet_type' in changes:
            pallet_id = self._ensure_pallet(changes)
            if pallet_id:
                # Get box_id
                if 'box_type' in changes:
                    box_id = self._ensure_box(changes)
                else:
                    box_id = current_attrs.get('box_id')

                if box_id:
                    box_per_pallet = changes.get('box_per_pallet', current_attrs.get('box_per_pallet'))
                    self._ensure_box_to_pallet(new_part.part_id, box_id, pallet_id, box_per_pallet)

        # Handle line
        if 'line_code' in changes or 'workshop_code' in changes:
            line_code = changes.get('line_code', current_attrs.get('line_code'))
            if line_code:
                workshop_code = changes.get('workshop_code', current_attrs.get('workshop_code'))
                line_id = self._ensure_line(line_code, changes.get('line_name'), workshop_code)
                if line_id:
                    self._ensure_part_to_line(new_part.part_id, line_id)

        return new_part

    def _ensure_supplier(self, supplier_name: str, localization: str = 'no data') -> str:
        """Create or find supplier."""
        if not supplier_name:
            raise ValueError("Supplier name is required")

        supplier = self.session.query(SupplierData).where(
            SupplierData.supplier_name == supplier_name
        ).first()

        if supplier:
            return supplier.supplier_id

        # Create a SupplierData object by assigning attributes.
        new_supplier = SupplierData()
        new_supplier.supplier_name = supplier_name
        new_supplier.localization = localization

        self.session.add(new_supplier)
        self.session.flush()
        return new_supplier.supplier_id

    def _ensure_box(self, changes: Dict[str, Any]) -> Optional[str]:
        """Create or find box."""
        box_type = changes.get('box_type')
        length_raw = changes.get('box_length_mm')
        width_raw = changes.get('box_width_mm')
        height_raw = changes.get('box_height_mm')

        # Check if box_type exists
        if box_type is None:
            return None

        # Convert to int with validation (if values exist)
        try:
            length = int(length_raw) if length_raw is not None else None
            width = int(width_raw) if width_raw is not None else None
            height = int(height_raw) if height_raw is not None else None
        except (ValueError, TypeError):
            return None

        # Check: if the value is not None, then it must be > 0
        if length is not None and length <= 0:
            logger.warning("Invalid box length: %s (must be > 0 or None)", length)
            return None
        if width is not None and width <= 0:
            logger.warning("Invalid box width: %s (must be > 0 or None)", width)
            return None
        if height is not None and height <= 0:
            logger.warning("Invalid box height: %s (must be > 0 or None)", height)
            return None

        # If ALL dimensions are None, then you cannot create a box.
        if length is None or width is None or height is None:
            logger.warning("All box dimensions cannot be None")
            return None

        # Search for an existing box
        box = self.session.query(BoxData).where(
            BoxData.box_type == box_type,
            BoxData.box_length_mm == length,
            BoxData.box_width_mm == width,
            BoxData.box_height_mm == height
        ).first()

        if box:
            return box.box_id

        # Creating a new box
        new_box = BoxData()
        new_box.box_type = box_type
        new_box.box_length_mm = length
        new_box.box_width_mm = width
        new_box.box_height_mm = height

        # The weight can be None or >= 0
        box_weight = changes.get('box_weight_kg')
        if box_weight is not None:
            if box_weight >= 0:
                new_box.box_weight_kg = box_weight
            else:
                logger.warning("Invalid box weight: %s (must be >= 0)", box_weight)
                return None

        # Stacking can be None or >= 0
        box_stacking = changes.get('box_stacking')
        if box_stacking is not None:
            if box_stacking >= 0:
                new_box.box_stacking = box_stacking
            else:
                logger.warning("Invalid box stacking: %s (must be >= 0)", box_stacking)
                return None

        self.session.add(new_box)
        self.session.flush()
        return new_box.box_id

    def _ensure_pallet(self, changes: Dict[str, Any]) -> Optional[str]:
        """Create or find pallet."""
        pallet_type = changes.get('pallet_type')
        length_raw = changes.get('pallet_length_mm')
        width_raw = changes.get('pallet_width_mm')
        height_raw = changes.get('pallet_height_mm')

        # Check if all values exist and not None
        if pallet_type is None or length_raw is None or width_raw is None or height_raw is None:
            return None

        # Convert to int with validation
        try:
            length = int(length_raw)
            width = int(width_raw)
            height = int(height_raw)
        except (ValueError, TypeError):
            return None

        # Check that all values are > 0
        if length <= 0 or width <= 0 or height <= 0:
            logger.warning(
                "Invalid pallet dimensions: length=%s, width=%s, height=%s (must be > 0)",
                length, width, height
            )
            return None

        pallet = self.session.query(PalletData).where(
            PalletData.pallet_type == pallet_type,
            PalletData.pallet_length_mm == length,
            PalletData.pallet_width_mm == width,
            PalletData.pallet_height_mm == height
        ).first()

        if pallet:
            return pallet.pallet_id

        # Create a PalletData object by assigning attributes.
        new_pallet = PalletData()
        new_pallet.pallet_type = pallet_type
        new_pallet.pallet_length_mm = length
        new_pallet.pallet_width_mm = width
        new_pallet.pallet_height_mm = height

        # The weight can be None or >= 0
        pallet_weight = changes.get('pallet_weight_kg')
        if pallet_weight is not None:
            if pallet_weight >= 0:
                new_pallet.pallet_weight_kg = pallet_weight
            else:
                logger.warning("Invalid pallet weight: %s (must be >= 0)", pallet_weight)
                return None

        # Stacking can be None or >= 0
        pallet_stacking = changes.get('pallet_stacking')
        if pallet_stacking is not None:
            if pallet_stacking >= 0:
                new_pallet.pallet_stacking = pallet_stacking
            else:
                logger.warning("Invalid pallet stacking: %s (must be >= 0)", pallet_stacking)
                return None

        self.session.add(new_pallet)
        self.session.flush()
        return new_pallet.pallet_id

    def _ensure_line(
            self,
            line_code: str,
            line_name: Optional[str],
            workshop_code: Optional[str]
        ) -> Optional[str]:
        """Create or find line."""
        if not line_code:
            return None

        line = self.session.query(LineData).where(LineData.line_code == line_code).first()
        if line:
            return line.line_id

        if not workshop_code:
            raise ValueError(f"workshop_code required for new line {line_code}")

        workshop = self.session.query(WorkshopData).where(
            WorkshopData.workshop_code == workshop_code
        ).first()

        if not workshop:
            # Create a WorkshopData object by assigning attributes.
            workshop = WorkshopData()
            workshop.workshop_code = workshop_code
            self.session.add(workshop)
            self.session.flush()

        # Create a LineData object by assigning attributes.
        new_line = LineData()
        new_line.line_code = line_code
        new_line.workshop_id = workshop.workshop_id
        if line_name:
            new_line.line_name = line_name

        self.session.add(new_line)
        self.session.flush()
        return new_line.line_id

    def _get_model_id(self, model_code: str) -> str:
        """Get model ID by code."""
        model = self.session.query(ModelData).where(
            ModelData.model_code == model_code
        ).first()
        if not model:
            raise ValueError(f"Model {model_code} not found")
        return model.model_id

    def _get_configuration_id(self, configuration: Optional[str]) -> str:
        """Get configuration ID by name."""
        if not configuration:
            # Use default 'no data'
            config = self.session.query(ConfigurationData).where(
                ConfigurationData.configuration == 'no data'
            ).first()
            if config:
                return config.configuration_id
            # Create ConfigurationData object if it doesn't exist
            new_config = ConfigurationData()
            new_config.configuration = 'no data'
            self.session.add(new_config)
            self.session.flush()
            return new_config.configuration_id

        config = self.session.query(ConfigurationData).where(
            ConfigurationData.configuration == configuration
        ).first()

        if config:
            return config.configuration_id

        new_config = ConfigurationData()
        new_config.configuration = configuration
        self.session.add(new_config)
        self.session.flush()
        return new_config.configuration_id

    def _deactivate_part_for_model(self, part_id: str, model_id: str, breakpoint_id: str):
        """Deactivate part for model."""
        self.session.execute(
            text("""
                UPDATE part_to_model
                SET is_active = false,
                    deactivated_by_breakpoint_id = :breakpoint_id
                WHERE part_id = :part_id
                  AND model_id = :model_id
                  AND is_active = true
            """),
            {'part_id': part_id, 'model_id': model_id, 'breakpoint_id': breakpoint_id}
        )

    def _activate_part_for_model(
        self,
        part_id: str,
        model_id: str,
        config_id: str,
        part_per_vehicle: Optional[int]
    ):
        """Activate part for model."""
        existing = self.session.query(PartToModel).where(
            PartToModel.part_id == part_id,
            PartToModel.model_id == model_id,
            PartToModel.configuration_id == config_id
        ).first()

        if existing:
            existing.is_active = True
            existing.deactivated_by_breakpoint_id = None
            if part_per_vehicle:
                existing.part_per_vehicle = part_per_vehicle
        else:
            # Create a PartToModel object by assigning attributes.
            ptm = PartToModel()
            ptm.part_id = part_id
            ptm.model_id = model_id
            ptm.configuration_id = config_id
            ptm.part_per_vehicle = part_per_vehicle
            ptm.is_active = True
            self.session.add(ptm)

    def _create_transition(
        self,
        new_part_id: str,
        old_part_id: str,
        breakpoint_id: str,
        model_id: str
    ):
        """Create PartToBreakpoint record."""
        existing = self.session.query(PartToBreakpoint).where(
            PartToBreakpoint.new_part_id == new_part_id,
            PartToBreakpoint.old_part_id == old_part_id,
            PartToBreakpoint.breakpoint_id == breakpoint_id,
            PartToBreakpoint.model_id == model_id
        ).first()

        if not existing:
            # Create a PartToBreakpoint object by assigning attributes.
            ptb = PartToBreakpoint()
            ptb.new_part_id = new_part_id
            ptb.old_part_id = old_part_id
            ptb.breakpoint_id = breakpoint_id
            ptb.model_id = model_id
            self.session.add(ptb)

    def _ensure_part_to_box(self, part_id: str, box_id: str, part_per_box: Optional[int] = None):
        """Ensure PartToBox relationship."""
        existing = self.session.query(PartToBox).where(
            PartToBox.part_id == part_id,
            PartToBox.box_id == box_id
        ).first()
        if not existing:
            # Create a PartToBox object by assigning attributes.
            ptb = PartToBox()
            ptb.part_id = part_id
            ptb.box_id = box_id
            if part_per_box is not None:
                ptb.part_per_box = part_per_box
            self.session.add(ptb)

    def _ensure_box_to_pallet(
        self,
        part_id: str,
        box_id: str,
        pallet_id: str,
        box_per_pallet: Optional[int] = None
    ):
        """Ensure BoxToPallet relationship."""
        existing = self.session.query(BoxToPallet).where(
            BoxToPallet.part_id == part_id,
            BoxToPallet.box_id == box_id,
            BoxToPallet.pallet_id == pallet_id
        ).first()
        if not existing:
            # Create a BoxToPallet object by assigning attributes.
            btp = BoxToPallet()
            btp.part_id = part_id
            btp.box_id = box_id
            btp.pallet_id = pallet_id
            if box_per_pallet is not None:
                btp.box_per_pallet = box_per_pallet
            self.session.add(btp)

    def _ensure_part_to_line(self, part_id: str, line_id: str):
        """Ensure PartToLine relationship."""
        existing = self.session.query(PartToLine).where(
            PartToLine.part_id == part_id,
            PartToLine.line_id == line_id
        ).first()
        if not existing:
            # Create a BoxToPallet object by assigning attributes.
            ptl = PartToLine()
            ptl.part_id = part_id
            ptl.line_id = line_id
            self.session.add(ptl)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_db_session() -> Session:
    """Get database session."""
    engine = initialize_database(create_tables=False)
    if not engine:
        raise RuntimeError("Failed to initialize database")
    return Session(engine)


def handle_api_response(f):
    """Decorator to handle API responses and errors."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            if isinstance(result, tuple):
                return result
            if isinstance(result, dict):
                if result.get('error'):
                    status_code = result.get('status_code', 500)
                    return jsonify(result), status_code
                return jsonify(result)
            return result

        except ValidationError as e:
            return jsonify({
                'success': False,
                'error': 'Validation error',
                'details': e.messages
            }), 400

        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400

        except RuntimeError as e:
            logger.error("Runtime error: %s", e)
            return jsonify({
                'success': False,
                'error': 'Service error'
            }), 503

        except Exception as e:
            logger.error("Unexpected error: %s", e, exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500

    return wrapper


# ============================================================================
# FLASK ENDPOINTS
# ============================================================================

@modify_bp.route('/parts/<string:part_number>/modify', methods=['POST'])
@rate_limit()
@jwt_required
@role_required([ROLES['ADMIN'], ROLES['EDITOR']])
@handle_api_response
def modify_part(part_number):
    """Modify a part with full audit trail."""
    schema = PartChangeSchema()
    data = schema.load(request.json)

    if not isinstance(data, dict):
        return {
            'success': False,
            'error': 'Invalid request data format',
            'status_code': 400
        }

    # Checking that model_code is present
    model_code = data.get('model_code')
    if not model_code:
        return {
            'success': False,
            'error': 'model_code is required',
            'status_code': 400
        }

    # Check that part_number is not empty
    if not part_number or not part_number.strip():
        return {
            'success': False,
            'error': 'part_number is required',
            'status_code': 400
        }

    changed_by = get_current_email() or get_current_username() or 'unknown'

    session = get_db_session()
    try:
        service = ManualChangeService(session)

        # Get current attributes for classification
        current = service.get_active_version(part_number, model_code)

        if not current:
            return {
                'success': False,
                'error': f"Part {part_number} not found for model {model_code}",
                'status_code': 404
            }

        current_attrs = service.get_part_attributes(current['part_id'])

        if not current_attrs:
            return {
                'success': False,
                'error': f"Part attributes not found for part {part_number}",
                'status_code': 404
            }

        # Classify changes
        domain, nature = ChangeClassifier.classify(data['changes'], current_attrs)
        logger.debug("Classification: domain=%s, nature=%s", domain, nature)

        # Create breakpoint
        bp = service.create_virtual_breakpoint(
            part_number=part_number,
            model_code=model_code,
            change_reason=data['change_reason'],
            ticket_number=data.get('ticket_number'),
            changed_by=changed_by,
            changes=data['changes'],
            change_domain=domain,
            change_nature=nature
        )

        # Apply changes
        result = service.apply_changes(
            part_number=part_number,
            model_code=model_code,
            changes=data['changes'],
            bp=bp,
            changed_by=changed_by
        )

        session.commit()

        return {
            'success': True,
            'message': 'Part modified successfully',
            'breakpoint_id': bp.breakpoint_id,
            'breakpoint_number': bp.breakpoint_number,
            'old_part_id': result['old_part_id'],
            'new_part_id': result['new_part_id'],
            'old_version': result['old_version'],
            'new_version': result['new_version'],
            'changes_applied': result['changes_applied'],
            'change_domain': domain,
            'change_nature': nature,
            'changed_by': changed_by
        }

    except Exception as e:
        session.rollback()
        logger.error("Error in modify_part: %s", e, exc_info=True)
        return {'success': False, 'error': str(e)}, 500

    finally:
        session.close()


@modify_bp.route('/parts/<string:part_number>/history', methods=['GET'])
@rate_limit()
@jwt_required
@handle_api_response
def get_part_history(part_number):
    """Get full history of a part."""
    model_code = request.args.get('model_code')

    try:
        limit = request.args.get('limit', 100, type=int)
        if limit is None or limit < 1:
            limit = 100
        limit = min(limit, 500)
    except (ValueError, TypeError):
        limit = 100

    session = get_db_session()
    try:
        query = session.query(PartHistoryView).where(
            PartHistoryView.part_number == part_number
        )

        if model_code:
            query = query.where(PartHistoryView.model_code == model_code)

        history = query.order_by(
            PartHistoryView.version_number.desc()
        ).limit(limit).all()

        if not history:
            return {
                'success': False,
                'error': f'Part {part_number} not found',
                'status_code': 404
            }

        result = []
        for h in history:
            entry = {
                'version': h.version_number,
                'part_name': h.part_name,
                'part_weight_kg': float(h.part_weight_kg) if h.part_weight_kg else None,
                'supplier_name': h.supplier_name,
                'configuration': h.configuration,
                'is_active': h.is_active,
                'breakpoint_number': h.breakpoint_number,
                'breakpoint_date': h.breakpoint_date.isoformat() if h.breakpoint_date else None,
                'change_source': h.change_source,
                'change_domain': h.change_domain,
                'change_nature': h.change_nature,
                'change_action_type': h.change_action_type,
                'created_at': h.created_at.isoformat() if h.created_at else None,
            }
            result.append(entry)

        return {
            'success': True,
            'part_number': part_number,
            'total_versions': len(result),
            'history': result
        }

    except SQLAlchemyError as e:
        session.rollback()
        logger.error("Database error in get_part_history for %s: %s", part_number, str(e))
        return {
            'success': False,
            'error': f"Database error: {str(e)}",
            'status_code': 500
        }
    except Exception as e:
        session.rollback()
        logger.error("Unexpected error in get_part_history for %s: %s", part_number, str(e), exc_info=True)
        return {
            'success': False,
            'error': f"Internal server error: {str(e)}",
            'status_code': 500
        }
    finally:
        session.close()


@modify_bp.route('/parts/<string:part_number>/versions/<int:version>/rollback', methods=['POST'])
@rate_limit()
@jwt_required
@role_required([ROLES['ADMIN'], ROLES['EDITOR']])
@handle_api_response
def rollback_part_version(part_number, version):
    """Rollback part to a specific version."""
    schema = RollbackSchema()

    # 1. Load the data with error handling.
    try:
        data = schema.load(request.json or {})
    except ValidationError as e:
        return {
            'success': False,
            'error': 'Validation error',
            'details': e.messages,
            'status_code': 400
        }

    # 2. Check that data is a dictionary.
    if not isinstance(data, dict):
        return {
            'success': False,
            'error': 'Invalid request data format',
            'status_code': 400
        }

    # 3. Check for the presence of model_code
    model_code = data.get('model_code')
    if not model_code:
        return {
            'success': False,
            'error': 'model_code is required',
            'status_code': 400
        }

    # 4. Check the part_number
    if not part_number or not part_number.strip():
        return {
            'success': False,
            'error': 'part_number is required',
            'status_code': 400
        }

    # 5. Check the version
    if version is None or version <= 0:
        return {
            'success': False,
            'error': 'version must be a positive integer',
            'status_code': 400
        }

    reason = data.get('reason', f'Rollback to version {version}')
    changed_by = get_current_email() or get_current_username() or 'system'

    session = get_db_session()
    try:
        service = ManualChangeService(session)

        # Get target version
        target = session.query(PartData).where(
            PartData.part_number == part_number,
            PartData.version_number == version
        ).first()

        if not target:
            return {
                'success': False,
                'error': f'Version {version} not found for part {part_number}',
                'status_code': 404
            }

        # Get current version
        current = service.get_active_version(part_number, model_code)
        if not current:
            return {
                'success': False,
                'error': f'No active version for part {part_number} on model {model_code}',
                'status_code': 404
            }

        # Get attributes for comparison
        target_attrs = service.get_part_attributes(target.part_id)
        current_attrs = service.get_part_attributes(current['part_id'])

        # Build changes to rollback
        changes = {}
        for key, value in target_attrs.items():
            if key in current_attrs and str(current_attrs.get(key)) != str(value):
                changes[key] = value

        if not changes:
            return {
                'success': True,
                'message': f'Part {part_number} is already at version {version}',
                'old_version': current['version_number'],
                'new_version': current['version_number']
            }

        # Classify changes
        domain, nature = ChangeClassifier.classify(changes, current_attrs)

        # Create breakpoint
        bp = service.create_virtual_breakpoint(
            part_number=part_number,
            model_code=model_code,
            change_reason=f'Rollback to version {version}: {reason}',
            ticket_number=data.get('ticket_number'),
            changed_by=changed_by,
            changes=changes,
            change_domain=domain,
            change_nature=nature
        )

        # Apply changes
        result = service.apply_changes(
            part_number=part_number,
            model_code=model_code,
            changes=changes,
            bp=bp,
            changed_by=changed_by
        )

        session.commit()

        return {
            'success': True,
            'message': f'Successfully rolled back part {part_number} to version {version}',
            'breakpoint_id': bp.breakpoint_id,
            'old_version': result['old_version'],
            'new_version': result['new_version'],
            'changes_applied': result['changes_applied'],
            'change_domain': domain,
            'change_nature': nature,
            'changed_by': changed_by
        }

    except KeyError as e:
        logger.error("Missing key in request data: %s", e)
        return {
            'success': False,
            'error': f"Missing required field: {str(e)}",
            'status_code': 400
        }
    except ValueError as e:
        logger.error("Value error: %s", e)
        return {
            'success': False,
            'error': str(e),
            'status_code': 400
        }
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
        return {
            'success': False,
            'error': f"Internal server error: {str(e)}",
            'status_code': 500
        }
    finally:
        session.close()


@modify_bp.route('/parts/<string:part_number>/versions', methods=['GET'])
@rate_limit()
@jwt_required
@handle_api_response
def get_part_versions(part_number):
    """Get all versions of a part."""
    model_code = request.args.get('model_code')

    session = get_db_session()
    try:
        query = text("""
            SELECT
                p.part_id,
                p.version_number,
                p.part_name,
                p.part_weight_kg,
                p.created_at,
                s.supplier_name,
                ptm.is_active,
                ptm.deactivated_at
            FROM part_data p
            LEFT JOIN supplier_data s ON p.supplier_id = s.supplier_id
            LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
            WHERE p.part_number = :part_number
            AND (:model_code IS NULL OR ptm.model_id = (
                SELECT model_id FROM model_data WHERE model_code = :model_code
            ))
            ORDER BY p.version_number DESC
        """)

        results = session.execute(query, {
            'part_number': part_number,
            'model_code': model_code
        }).all()

        versions = []
        for row in results:
            row_dict = row._asdict()
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
            versions.append(row_dict)

        return {
            'success': True,
            'part_number': part_number,
            'total_versions': len(versions),
            'versions': versions
        }

    except SQLAlchemyError as e:
        session.rollback()
        logger.error("Database error in get_part_versions for %s: %s", part_number, str(e))
        return {
            'success': False,
            'error': f"Database error: {str(e)}",
            'status_code': 500
        }
    except Exception as e:
        session.rollback()
        logger.error("Unexpected error in get_part_versions for %s: %s", part_number, str(e), exc_info=True)
        return {
            'success': False,
            'error': f"Internal server error: {str(e)}",
            'status_code': 500
        }
    finally:
        session.close()


@modify_bp.route('/auth/token', methods=['POST'])
@rate_limit()
def generate_auth_token():
    """Generate JWT token for testing."""
    try:
        data = request.json or {}
        username = data.get('username', 'test_user')
        email = data.get('email', 'test@company.com')
        roles = data.get('roles', ['viewer'])

        token = generate_token(username, email, roles)

        return {
            'success': True,
            'token': token,
            'expires_in': 8 * 3600,
            'user': {
                'username': username,
                'email': email,
                'roles': roles
            }
        }

    except Exception as e:
        logger.error("Error generating token: %s", e)
        return {
            'success': False,
            'error': str(e)
        }, 500


@modify_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    engine = None
    try:
        engine = initialize_database(create_tables=False)
        db_status = 'healthy' if engine else 'unhealthy'

        return {
            'status': 'healthy' if db_status == 'healthy' else 'degraded',
            'service': 'MFT Modify API',
            'version': '1.0.0',
            'timestamp': datetime.now(MOSCOW_TZ).isoformat(),
            'environment': FLASK_ENV,
            'database': db_status
        }

    except Exception as e:
        return {
            'status': 'unhealthy',
            'service': 'MFT Modify API',
            'error': str(e),
            'timestamp': datetime.now(MOSCOW_TZ).isoformat()
        }, 503
    finally:
        if engine:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning("Error disposing database engine: %s", e)


@modify_bp.route('/', methods=['GET'])
def api_documentation():
    """API documentation."""
    return {
        'name': 'MFT Modify API',
        'version': '1.0.0',
        'description': 'Manual part modification with virtual breakpoints',
        'endpoints': {
            '/api/v1/parts/{part_number}/modify': {
                'methods': ['POST'],
                'auth': ['admin', 'editor'],
                'description': 'Modify part attributes'
            },
            '/api/v1/parts/{part_number}/history': {
                'methods': ['GET'],
                'auth': ['viewer', 'editor', 'admin'],
                'description': 'Get part history'
            },
            '/api/v1/parts/{part_number}/versions/{version}/rollback': {
                'methods': ['POST'],
                'auth': ['admin', 'editor'],
                'description': 'Rollback to version'
            },
            '/api/v1/parts/{part_number}/versions': {
                'methods': ['GET'],
                'auth': ['viewer', 'editor', 'admin'],
                'description': 'Get all versions'
            },
            '/api/v1/health': {
                'methods': ['GET'],
                'auth': 'none',
                'description': 'Health check'
            }
        }
    }


# ============================================================================
# FLASK APP SETUP
# ============================================================================

def create_app():
    """Create and configure the Flask application instance."""
    app = Flask(__name__)
    app.secret_key = FLASK_SECRET_KEY

    # CORS
    if ALLOWED_ORIGINS == "*":
        CORS(app)
    else:
        origins = [origin.strip() for origin in ALLOWED_ORIGINS.split(',')]
        CORS(app, origins=origins, supports_credentials=True)

    # Security headers
    @app.after_request
    def add_security_headers(response):
        if IS_PRODUCTION:
            response.headers.add('X-Content-Type-Options', 'nosniff')
            response.headers.add('X-Frame-Options', 'DENY')
            response.headers.add('X-XSS-Protection', '1; mode=block')
        return response

    # Register blueprint
    app.register_blueprint(modify_bp)

    # Rate limiting
    limiter.init_app(app)

    # Error handlers
    @app.errorhandler(404)
    def not_found(_):
        return jsonify({'success': False, 'error': 'Resource not found'}), 404

    @app.errorhandler(429)
    def ratelimit_handler(_):
        return jsonify({
            'success': False,
            'error': 'Rate limit exceeded. Please try again later.'
        }), 429

    return app


flask_app = create_app()

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Starting MFT Modify API on %s:%s", FLASK_HOST, FLASK_PORT)
    logger.info("Environment: %s", FLASK_ENV)
    logger.info("=" * 60)

    try:
        flask_app.run(
            host=FLASK_HOST,
            port=FLASK_PORT,
            debug=FLASK_DEBUG,
            threaded=True
        )
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error("Failed to start application: %s", e, exc_info=True)
        sys.exit(1)
