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
        - Role-based access control (admin, engineer, planner)
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

TECHNICAL DEBT PAID:
    - Solves "silent updates" problem
    - Eliminates need for separate audit tables
    - Maintains data integrity across all change sources
    - Provides unified change management interface

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

CRITICAL CONCEPTS:
    - Virtual Breakpoint: A breakpoint created for manual changes
      (MAN-YYYYMMDD-XXXX format)
    - Change Source: 'manual' for API changes, 'automatic' for BP pipeline
    - Atomic Group: All changes in one request share one breakpoint_id
    - Smart Versioning: New version created only when attributes actually change

TODO (Before Production):
    1. Test all endpoints thoroughly
    2. Adjust import paths for project structure
    3. Validate database connection handling in production
    4. Review and adjust rate limits based on load testing
    5. Add proper error handling for edge cases
    6. Implement comprehensive logging strategy
    7. Replace /auth/token with proper OAuth2/LDAP integration
    8. Add request validation for all edge cases
    9. Write integration tests
    10. Document all endpoints with examples

Maintainer: PLD Engineering Center
Version: 1.0.0
Created: 2026-08-18
Last Modified: 2026-08-18
License: MIT
Status: Development
"""
# Standard library imports
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import logging
import os
from functools import wraps

# Third-party imports
from flask import Flask, request, jsonify, Blueprint, current_app
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
from marshmallow import Schema, fields, validate, ValidationError
from sqlalchemy.orm import Session
from sqlalchemy import text, select
import jwt

# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    # If __file__ is not defined (in exec() or interactive mode)
    PROJECT_ROOT = Path("/opt/airflow")

# Add project root to path if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from dags.tasks.connector import initialize_database
from dags.tasks.bp_mapper import create_bp_mapper
from dags.tasks.change_classifier import ChangeClassifier
from database.database import (
    # Entity tables
    SupplierData, PartData, BoxData, PalletData, ModelData,
    ConfigurationData, WorkshopData, LineData, BreakpointData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel, PartToLine, PartToBreakpoint
)

# Logger setup
logger = get_logger(__name__)

# ============================================================================
# FLASK APP SETUP
# ============================================================================

app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['JWT_ALGORITHM'] = os.getenv('JWT_ALGORITHM', 'HS256')
app.config['CORS_ORIGINS'] = os.getenv('CORS_ORIGINS', '*').split(',')

# Enable CORS
CORS(app, origins=app.config['CORS_ORIGINS'])

# Rate limiter setup
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

# Create blueprint for API routes
api_bp = Blueprint('api', __name__, url_prefix='/api/v1')


# ============================================================================
# JWT AUTHENTICATION DECORATOR
# ============================================================================

def jwt_required(f):
    """Decorator to protect endpoints with JWT authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')

        if not auth_header:
            return jsonify({
                'success': False,
                'error': 'Missing Authorization header'
            }), 401

        try:
            # Extract token from "Bearer <token>"
            parts = auth_header.split()
            if len(parts) != 2 or parts[0].lower() != 'bearer':
                return jsonify({
                    'success': False,
                    'error': 'Invalid Authorization header format. Use: Bearer <token>'
                }), 401

            token = parts[1]

            # Decode token
            payload = jwt.decode(
                token,
                current_app.config['SECRET_KEY'],
                algorithms=[current_app.config['JWT_ALGORITHM']]
            )

            # Add user info to request context
            request.user = payload.get('sub')
            request.user_roles = payload.get('roles', [])
            request.user_email = payload.get('email')

            logger.debug("JWT authenticated: %s", request.user)

        except jwt.ExpiredSignatureError:
            return jsonify({
                'success': False,
                'error': 'Token has expired'
            }), 401
        except jwt.InvalidTokenError as e:
            return jsonify({
                'success': False,
                'error': f'Invalid token: {str(e)}'
            }), 401
        except Exception as e:
            logger.error("JWT validation error: %s", str(e))
            return jsonify({
                'success': False,
                'error': 'Authentication error'
            }), 401

        return f(*args, **kwargs)

    return decorated_function


def role_required(required_roles):
    """Decorator to check user roles."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not hasattr(request, 'user_roles'):
                return jsonify({
                    'success': False,
                    'error': 'Authentication required'
                }), 401

            user_roles = request.user_roles
            if not any(role in user_roles for role in required_roles):
                return jsonify({
                    'success': False,
                    'error': f'Insufficient permissions. Required roles: {required_roles}'
                }), 403

            return f(*args, **kwargs)
        return decorated_function
    return decorator


# ============================================================================
# MARSHMALLOW SCHEMAS (Request/Response Validation)
# ============================================================================

class PartChangeSchema(Schema):
    """Request schema for part modification."""

    model_code = fields.Str(
        required=True,
        validate=validate.OneOf(
            ['jolion', 'h3', 'f7', 'f7x', 'dargo', 'h7',
             'a01', 'a08', 'b02', 'b04', 'b06', 'b16'],
            error="Invalid model_code. Must be one of: jolion, h3, f7, f7x, dargo, h7, a01, a08, b02, b04, b06, b16"
        ),
        description="Model code (e.g., 'jolion', 'h3', 'f7')"
    )

    changes = fields.Dict(
        required=True,
        validate=validate.Length(min=1, error="At least one change must be provided"),
        description="Dictionary of fields to change and their new values"
    )

    change_reason = fields.Str(
        required=True,
        validate=validate.Length(min=10, max=500),
        description="Reason for the change (mandatory)"
    )

    ticket_number = fields.Str(
        required=False,
        allow_none=True,
        validate=validate.Length(max=50),
        description="Ticket number in JIRA/YouTrack"
    )

    force_create_new_version = fields.Bool(
        required=False,
        missing=False,
        description="Force creation of new version even if attributes didn't change"
    )

    # Custom validation for changes fields
    @staticmethod
    def validate_changes(data, **kwargs):
        """Validate that changes contain only allowed fields."""
        allowed_fields = {
            'part_name', 'part_weight_kg', 'supplier_name', 'localization',
            'box_type', 'box_length_mm', 'box_width_mm', 'box_height_mm',
            'pallet_type', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm',
            'line_code', 'line_name', 'workshop_code',
            'configuration', 'part_per_vehicle', 'transmission',
            'box_weight_kg', 'box_stacking',
            'pallet_weight_kg', 'pallet_stacking',
            'part_per_box', 'box_per_pallet'
        }

        changes = data.get('changes', {})
        invalid_fields = set(changes.keys()) - allowed_fields

        if invalid_fields:
            raise ValidationError(
                f"Invalid fields: {', '.join(invalid_fields)}. "
                f"Allowed fields: {', '.join(allowed_fields)}"
            )

        return data


class PartChangeResponseSchema(Schema):
    """Response schema for part modification."""

    success = fields.Bool(required=True)
    message = fields.Str(required=True)
    breakpoint_id = fields.Str(required=False, allow_none=True)
    breakpoint_number = fields.Str(required=False, allow_none=True)
    old_part_id = fields.Str(required=False, allow_none=True)
    new_part_id = fields.Str(required=False, allow_none=True)
    old_version = fields.Int(required=False, allow_none=True)
    new_version = fields.Int(required=False, allow_none=True)
    changes_applied = fields.Dict(required=False, default={})
    created_at = fields.DateTime(required=False)


class PartHistorySchema(Schema):
    """Response schema for part history."""

    success = fields.Bool(required=True)
    part_number = fields.Str(required=True)
    total_versions = fields.Int(required=True)
    history = fields.List(fields.Dict, required=True)


class RollbackSchema(Schema):
    """Request schema for rollback."""

    model_code = fields.Str(
        required=True,
        validate=validate.OneOf(
            ['jolion', 'h3', 'f7', 'f7x', 'dargo', 'h7',
             'a01', 'a08', 'b02', 'b04', 'b06', 'b16']
        ),
        description="Model code"
    )
    reason = fields.Str(
        required=False,
        allow_none=True,
        validate=validate.Length(max=500),
        description="Reason for rollback"
    )
    ticket_number = fields.Str(
        required=False,
        allow_none=True,
        validate=validate.Length(max=50),
        description="Ticket number"
    )


# ============================================================================
# CORE SERVICE LOGIC
# ============================================================================

class ManualChangeService:
    """Service for handling manual part changes with virtual breakpoints."""

    def __init__(self, db_session: Session):
        self.session = db_session
        self.mapper = create_bp_mapper(db_session.bind)

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
    ) -> str:
        """Create a virtual breakpoint for manual changes."""
        # Generate breakpoint number: MAN-YYYYMMDD-XXXX
        today = datetime.now().strftime('%Y%m%d')

        # Count existing manual breakpoints for today
        count_query = text("""
            SELECT COUNT(*) 
            FROM breakpoint_data 
            WHERE breakpoint_number LIKE :pattern
        """)
        pattern = f"MAN-{today}-%"
        count = self.session.execute(count_query, {'pattern': pattern}).scalar() or 0
        seq = str(count + 1).zfill(4)

        breakpoint_number = f"MAN-{today}-{seq}"

        # Prepare description
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

        # Create breakpoint with classification
        new_breakpoint = BreakpointData(
            breakpoint_number=breakpoint_number,
            breakpoint_status='closed',
            breakpoint_date=datetime.now(),
            description=description.strip(),
            solution=f"Manual change by {changed_by}",
            change_domain=change_domain,
            change_nature=change_nature,
        )

        self.session.add(new_breakpoint)
        self.session.flush()

        logger.info(
            "Created virtual breakpoint %s (%s) with classification: domain=%s, nature=%s",
            breakpoint_number, new_breakpoint.breakpoint_id, change_domain, change_nature
        )

        return new_breakpoint.breakpoint_id

    def apply_changes_to_part(
        self,
        part_number: str,
        model_code: str,
        changes: Dict[str, Any],
        breakpoint_id: str,
        changed_by: str,
        change_domain: str,
        change_nature: str
    ) -> Dict[str, Any]:
        """Apply changes to part, creating new version if needed."""
        # 1. Get current active version for model
        current_version = self._get_active_version(part_number, model_code)
        if not current_version:
            raise ValueError(f"Part {part_number} not found for model {model_code}")

        old_part_id = current_version['part_id']
        old_version_number = current_version['version_number']

        # 2. Check if any changes are actually different
        current_attrs = self._get_part_attributes(old_part_id)
        actual_changes = {}

        for field, new_value in changes.items():
            current_value = current_attrs.get(field)
            # Compare values (convert to string for safe comparison)
            if str(current_value) != str(new_value):
                actual_changes[field] = new_value

        if not actual_changes:
            logger.info("No actual changes detected for part %s", part_number)
            return {
                'old_part_id': old_part_id,
                'new_part_id': old_part_id,
                'old_version': old_version_number,
                'new_version': old_version_number,
                'changes_applied': {},
                'is_new_version': False
            }

        # 3. Create new version of part
        new_part = self._create_new_version(
            old_part_id,
            part_number,
            actual_changes,
            current_attrs
        )

        new_part_id = new_part.part_id
        new_version_number = new_part.version_number

        # 4. Get model_id
        model_id = self.mapper.get_model_id_by_code(model_code)
        if not model_id:
            raise ValueError(f"Model {model_code} not found")

        # 5. Deactivate old version for this model
        self._deactivate_part_for_model(old_part_id, model_id, breakpoint_id)

        # 6. Activate new version for this model
        self._activate_part_for_model(new_part_id, model_id, breakpoint_id, current_attrs)

        # 7. Create PartToBreakpoint record
        self._create_transition_record(
            new_part_id,
            old_part_id,
            breakpoint_id,
            model_id
        )

        # 8. Commit all changes
        self.session.commit()

        logger.info(
            "Successfully applied changes to part %s: v%d → v%d (domain=%s, nature=%s)",
            part_number, old_version_number, new_version_number,
            change_domain, change_nature
        )

        return {
            'old_part_id': old_part_id,
            'new_part_id': new_part_id,
            'old_version': old_version_number,
            'new_version': new_version_number,
            'changes_applied': actual_changes,
            'is_new_version': True
        }

    def _get_active_version(self, part_number: str, model_code: str) -> Optional[Dict[str, Any]]:
        """Get the currently active version of a part for a model."""
        query = text("""
            SELECT 
                p.part_id,
                p.part_number,
                p.version_number,
                p.part_name,
                p.part_weight_kg,
                p.supplier_id,
                p.created_at
            FROM part_data p
            JOIN part_to_model ptm ON p.part_id = ptm.part_id
            JOIN model_data m ON ptm.model_id = m.model_id
            WHERE p.part_number = :part_number
              AND m.model_code = :model_code
              AND ptm.is_active = true
            ORDER BY p.version_number DESC
            LIMIT 1
        """)

        result = self.session.execute(query, {
            'part_number': part_number,
            'model_code': model_code
        }).first()

        if result:
            return dict(result._mapping)
        return None

    def _get_part_attributes(self, part_id: str) -> Dict[str, Any]:
        """Get all attributes of a part version."""
        query = text("""
            SELECT 
                p.part_number,
                p.part_name,
                p.part_weight_kg,
                s.supplier_name,
                s.localization,
                b.box_type,
                b.box_length_mm,
                b.box_width_mm,
                b.box_height_mm,
                b.box_weight_kg,
                b.box_stacking,
                pl.pallet_type,
                pl.pallet_length_mm,
                pl.pallet_width_mm,
                pl.pallet_height_mm,
                pl.pallet_weight_kg,
                pl.pallet_stacking,
                l.line_code,
                l.line_name,
                w.workshop_code,
                c.configuration,
                c.transmission,
                ptm.part_per_vehicle,
                ptb.part_per_box,
                btp.box_per_pallet,
                b.box_id,
                pl.pallet_id
            FROM part_data p
            LEFT JOIN supplier_data s ON p.supplier_id = s.supplier_id
            LEFT JOIN part_to_box ptb ON p.part_id = ptb.part_id
            LEFT JOIN box_data b ON ptb.box_id = b.box_id
            LEFT JOIN box_to_pallet btp ON p.part_id = btp.part_id
            LEFT JOIN pallet_data pl ON btp.pallet_id = pl.pallet_id
            LEFT JOIN part_to_line ptl ON p.part_id = ptl.part_id
            LEFT JOIN line_data l ON ptl.line_id = l.line_id
            LEFT JOIN workshop_data w ON l.workshop_id = w.workshop_id
            LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
            LEFT JOIN configuration_data c ON ptm.configuration_id = c.configuration_id
            WHERE p.part_id = :part_id
            LIMIT 1
        """)

        result = self.session.execute(query, {'part_id': part_id}).first()
        if result:
            return dict(result._mapping)
        return {}

    def _create_new_version(
        self,
        old_part_id: str,
        part_number: str,
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> PartData:
        """Create a new version of the part with changes applied."""
        # Get old version info
        old_part = self.session.get(PartData, old_part_id)
        if not old_part:
            raise ValueError(f"Part version {old_part_id} not found")

        # Determine original_part_id
        original_part_id = old_part.original_part_id or old_part_id

        # Calculate new version number with locking
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

        # Create new part with all attributes (copy from old + apply changes)
        new_part_data = {
            'part_number': part_number,
            'original_part_id': original_part_id,
            'version_number': new_version,
            'part_name': old_part.part_name,
            'part_weight_kg': old_part.part_weight_kg,
            'supplier_id': old_part.supplier_id,
            'created_at': datetime.now()
        }

        # Apply changes to the new part
        for field, value in changes.items():
            if field == 'part_name':
                new_part_data['part_name'] = value
            elif field == 'part_weight_kg':
                new_part_data['part_weight_kg'] = value
            elif field == 'supplier_name':
                # Need to create/find supplier
                localization = changes.get('localization', current_attrs.get('localization', 'no data'))
                supplier_id = self._ensure_supplier(value, localization)
                new_part_data['supplier_id'] = supplier_id
            elif field == 'localization':
                # Will be handled with supplier
                pass

        # Create new part record
        new_part = PartData(**new_part_data)
        self.session.add(new_part)
        self.session.flush()

        # Handle box changes
        box_changed = any(k.startswith('box_') for k in changes.keys())
        if box_changed or 'box_type' in changes:
            # Get box ID from current or create new
            if 'box_type' in changes:
                # New box specified
                box_id = self._ensure_box(changes)
            else:
                # Keep current box
                box_id = current_attrs.get('box_id')

            if box_id:
                part_per_box = changes.get('part_per_box', current_attrs.get('part_per_box'))
                self._create_part_to_box(new_part.part_id, box_id, part_per_box)

        # Handle pallet changes
        pallet_changed = any(k.startswith('pallet_') for k in changes.keys())
        if pallet_changed or 'pallet_type' in changes:
            if 'pallet_type' in changes:
                pallet_id = self._ensure_pallet(changes)
            else:
                pallet_id = current_attrs.get('pallet_id')

            if pallet_id:
                # Need to get box_id - either from changes or current
                box_id = None
                if 'box_type' in changes:
                    box_id = self._ensure_box(changes)
                else:
                    box_id = current_attrs.get('box_id')

                if box_id:
                    box_per_pallet = changes.get('box_per_pallet', current_attrs.get('box_per_pallet'))
                    self._create_box_to_pallet(new_part.part_id, box_id, pallet_id, box_per_pallet)

        # Handle line changes
        if 'line_code' in changes or 'workshop_code' in changes:
            line_code = changes.get('line_code', current_attrs.get('line_code'))
            line_name = changes.get('line_name', current_attrs.get('line_name'))
            workshop_code = changes.get('workshop_code', current_attrs.get('workshop_code'))

            if line_code:
                line_id = self._ensure_line(line_code, line_name, workshop_code)
                if line_id:
                    self._create_part_to_line(new_part.part_id, line_id)

        # Store configuration for later use in _activate_part_for_model
        if 'configuration' in changes:
            config_id = self._ensure_configuration(changes['configuration'])
            if config_id:
                new_part._temp_config_id = config_id

        return new_part

    def _ensure_supplier(self, supplier_name: str, localization: str = 'no data') -> str:
        """Create or find supplier."""
        if not supplier_name:
            raise ValueError("Supplier name is required")

        query = select(SupplierData).where(
            SupplierData.supplier_name == supplier_name
        )
        supplier = self.session.execute(query).scalar_one_or_none()

        if supplier:
            return supplier.supplier_id

        # Create new supplier
        new_supplier = SupplierData(
            supplier_name=supplier_name,
            localization=localization
        )
        self.session.add(new_supplier)
        self.session.flush()
        return new_supplier.supplier_id

    def _ensure_box(self, changes: Dict[str, Any]) -> Optional[str]:
        """Create or find box."""
        box_type = changes.get('box_type')
        length = changes.get('box_length_mm')
        width = changes.get('box_width_mm')
        height = changes.get('box_height_mm')

        if not all([box_type, length, width, height]):
            return None

        # Convert to int if needed
        try:
            length = int(length)
            width = int(width)
            height = int(height)
        except (ValueError, TypeError):
            logger.warning("Invalid box dimensions: %s, %s, %s", length, width, height)
            return None

        query = select(BoxData).where(
            BoxData.box_type == box_type,
            BoxData.box_length_mm == length,
            BoxData.box_width_mm == width,
            BoxData.box_height_mm == height
        )
        box = self.session.execute(query).scalar_one_or_none()

        if box:
            return box.box_id

        # Create new box
        new_box = BoxData(
            box_type=box_type,
            box_length_mm=length,
            box_width_mm=width,
            box_height_mm=height,
            box_weight_kg=changes.get('box_weight_kg'),
            box_stacking=changes.get('box_stacking')
        )
        self.session.add(new_box)
        self.session.flush()
        return new_box.box_id

    def _ensure_pallet(self, changes: Dict[str, Any]) -> Optional[str]:
        """Create or find pallet."""
        pallet_type = changes.get('pallet_type')
        length = changes.get('pallet_length_mm')
        width = changes.get('pallet_width_mm')
        height = changes.get('pallet_height_mm')

        if not all([pallet_type, length, width, height]):
            return None

        # Convert to int if needed
        try:
            length = int(length)
            width = int(width)
            height = int(height)
        except (ValueError, TypeError):
            logger.warning("Invalid pallet dimensions: %s, %s, %s", length, width, height)
            return None

        query = select(PalletData).where(
            PalletData.pallet_type == pallet_type,
            PalletData.pallet_length_mm == length,
            PalletData.pallet_width_mm == width,
            PalletData.pallet_height_mm == height
        )
        pallet = self.session.execute(query).scalar_one_or_none()

        if pallet:
            return pallet.pallet_id

        # Create new pallet
        new_pallet = PalletData(
            pallet_type=pallet_type,
            pallet_length_mm=length,
            pallet_width_mm=width,
            pallet_height_mm=height,
            pallet_weight_kg=changes.get('pallet_weight_kg'),
            pallet_stacking=changes.get('pallet_stacking')
        )
        self.session.add(new_pallet)
        self.session.flush()
        return new_pallet.pallet_id

    def _ensure_line(self, line_code: str, line_name: Optional[str], workshop_code: Optional[str]) -> Optional[str]:
        """Create or find line."""
        if not line_code:
            return None

        query = select(LineData).where(LineData.line_code == line_code)
        line = self.session.execute(query).scalar_one_or_none()

        if line:
            return line.line_id

        # Need workshop
        if not workshop_code:
            raise ValueError(f"workshop_code required to create new line {line_code}")

        # Find or create workshop
        workshop_query = select(WorkshopData).where(
            WorkshopData.workshop_code == workshop_code
        )
        workshop = self.session.execute(workshop_query).scalar_one_or_none()

        if not workshop:
            new_workshop = WorkshopData(workshop_code=workshop_code)
            self.session.add(new_workshop)
            self.session.flush()
            workshop_id = new_workshop.workshop_id
        else:
            workshop_id = workshop.workshop_id

        # Create line
        new_line = LineData(
            line_code=line_code,
            line_name=line_name,
            workshop_id=workshop_id
        )
        self.session.add(new_line)
        self.session.flush()
        return new_line.line_id

    def _ensure_configuration(self, configuration: str) -> Optional[str]:
        """Create or find configuration."""
        if not configuration:
            return None

        query = select(ConfigurationData).where(
            ConfigurationData.configuration == configuration
        )
        config = self.session.execute(query).scalar_one_or_none()

        if config:
            return config.configuration_id

        new_config = ConfigurationData(configuration=configuration)
        self.session.add(new_config)
        self.session.flush()
        return new_config.configuration_id

    def _deactivate_part_for_model(self, part_id: str, model_id: str, breakpoint_id: str):
        """Deactivate part for specific model."""
        update_stmt = text("""
            UPDATE part_to_model
            SET is_active = false,
                deactivated_by_breakpoint_id = :breakpoint_id
            WHERE part_id = :part_id
              AND model_id = :model_id
              AND is_active = true
        """)

        result = self.session.execute(update_stmt, {
            'part_id': part_id,
            'model_id': model_id,
            'breakpoint_id': breakpoint_id
        })

        if result.rowcount == 0:
            logger.warning(
                "No active PartToModel to deactivate for part %s, model %s",
                part_id, model_id
            )

    def _activate_part_for_model(
        self,
        part_id: str,
        model_id: str,
        breakpoint_id: str,
        current_attrs: Dict[str, Any]
    ):
        """Activate part for specific model."""
        # Get configuration_id from temp storage or current attributes
        part = self.session.get(PartData, part_id)
        config_id = getattr(part, '_temp_config_id', None)

        if not config_id:
            # Try to find existing configuration from current attrs
            config_name = current_attrs.get('configuration')
            if config_name:
                config_query = select(ConfigurationData).where(
                    ConfigurationData.configuration == config_name
                )
                config = self.session.execute(config_query).scalar_one_or_none()
                config_id = config.configuration_id if config else None

        if not config_id:
            # Use default 'no data'
            default_config = select(ConfigurationData).where(
                ConfigurationData.configuration == 'no data'
            )
            config = self.session.execute(default_config).scalar_one_or_none()
            config_id = config.configuration_id if config else None

        if not config_id:
            raise ValueError("Cannot determine configuration for part")

        # Check if PartToModel already exists
        exists_query = text("""
            SELECT 1 FROM part_to_model 
            WHERE part_id = :part_id 
              AND model_id = :model_id
              AND configuration_id = :config_id
        """)
        exists = self.session.execute(exists_query, {
            'part_id': part_id,
            'model_id': model_id,
            'config_id': config_id
        }).first()

        part_per_vehicle = current_attrs.get('part_per_vehicle')

        if not exists:
            # Create new PartToModel
            insert_stmt = text("""
                INSERT INTO part_to_model (
                    part_id, model_id, configuration_id, is_active, part_per_vehicle
                ) VALUES (
                    :part_id, :model_id, :config_id, true, :part_per_vehicle
                )
            """)
            self.session.execute(insert_stmt, {
                'part_id': part_id,
                'model_id': model_id,
                'config_id': config_id,
                'part_per_vehicle': part_per_vehicle
            })
        else:
            # Update existing record
            update_stmt = text("""
                UPDATE part_to_model
                SET is_active = true,
                    deactivated_by_breakpoint_id = NULL,
                    part_per_vehicle = COALESCE(:part_per_vehicle, part_per_vehicle)
                WHERE part_id = :part_id
                  AND model_id = :model_id
                  AND configuration_id = :config_id
            """)
            self.session.execute(update_stmt, {
                'part_id': part_id,
                'model_id': model_id,
                'config_id': config_id,
                'part_per_vehicle': part_per_vehicle
            })

    def _create_transition_record(
        self,
        new_part_id: str,
        old_part_id: str,
        breakpoint_id: str,
        model_id: str
    ):
        """Create PartToBreakpoint record."""
        # Check if record already exists (prevent duplicates)
        exists_query = text("""
            SELECT 1 FROM part_to_breakpoint
            WHERE new_part_id = :new_part_id
              AND old_part_id = :old_part_id
              AND breakpoint_id = :breakpoint_id
              AND model_id = :model_id
        """)
        exists = self.session.execute(exists_query, {
            'new_part_id': new_part_id,
            'old_part_id': old_part_id,
            'breakpoint_id': breakpoint_id,
            'model_id': model_id
        }).first()

        if not exists:
            new_record = PartToBreakpoint(
                new_part_id=new_part_id,
                old_part_id=old_part_id,
                breakpoint_id=breakpoint_id,
                model_id=model_id
            )
            self.session.add(new_record)

    def _create_part_to_box(self, part_id: str, box_id: str, part_per_box: Optional[int] = None):
        """Create PartToBox relationship."""
        if not part_id or not box_id:
            return

        exists_query = text("""
            SELECT 1 FROM part_to_box 
            WHERE part_id = :part_id AND box_id = :box_id
        """)
        exists = self.session.execute(exists_query, {
            'part_id': part_id,
            'box_id': box_id
        }).first()

        if not exists:
            insert_stmt = text("""
                INSERT INTO part_to_box (part_id, box_id, part_per_box)
                VALUES (:part_id, :box_id, :part_per_box)
            """)
            self.session.execute(insert_stmt, {
                'part_id': part_id,
                'box_id': box_id,
                'part_per_box': part_per_box
            })

    def _create_box_to_pallet(
        self,
        part_id: str,
        box_id: str,
        pallet_id: str,
        box_per_pallet: Optional[int] = None
    ):
        """Create BoxToPallet relationship."""
        if not all([part_id, box_id, pallet_id]):
            return

        exists_query = text("""
            SELECT 1 FROM box_to_pallet 
            WHERE part_id = :part_id AND box_id = :box_id AND pallet_id = :pallet_id
        """)
        exists = self.session.execute(exists_query, {
            'part_id': part_id,
            'box_id': box_id,
            'pallet_id': pallet_id
        }).first()

        if not exists:
            insert_stmt = text("""
                INSERT INTO box_to_pallet (part_id, box_id, pallet_id, box_per_pallet)
                VALUES (:part_id, :box_id, :pallet_id, :box_per_pallet)
            """)
            self.session.execute(insert_stmt, {
                'part_id': part_id,
                'box_id': box_id,
                'pallet_id': pallet_id,
                'box_per_pallet': box_per_pallet
            })

    def _create_part_to_line(self, part_id: str, line_id: str):
        """Create PartToLine relationship."""
        if not part_id or not line_id:
            return

        exists_query = text("""
            SELECT 1 FROM part_to_line 
            WHERE part_id = :part_id AND line_id = :line_id
        """)
        exists = self.session.execute(exists_query, {
            'part_id': part_id,
            'line_id': line_id
        }).first()

        if not exists:
            insert_stmt = text("""
                INSERT INTO part_to_line (part_id, line_id)
                VALUES (:part_id, :line_id)
            """)
            self.session.execute(insert_stmt, {
                'part_id': part_id,
                'line_id': line_id
            })


# ============================================================================
# FLASK ROUTES
# ============================================================================

def get_db_session():
    """Get database session."""
    engine = initialize_database(create_tables=False)
    if not engine:
        raise RuntimeError("Failed to initialize database")

    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


@api_bp.route('/parts/<string:part_number>/modify', methods=['POST'])
@limiter.limit("10 per minute")
@jwt_required
@role_required(['admin', 'engineer', 'planner'])
def modify_part(part_number):
    """
    Modify a part with full audit trail.
    
    This endpoint creates a virtual breakpoint and applies changes to the part,
    ensuring complete history tracking.
    """
    try:
        # Validate request data
        schema = PartChangeSchema()
        try:
            data = schema.load(request.json)
        except ValidationError as err:
            return jsonify({
                'success': False,
                'error': 'Validation error',
                'details': err.messages
            }), 400

        # Get user from JWT
        changed_by = request.user_email or request.user or 'unknown'

        # Get database session
        engine = initialize_database(create_tables=False)
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500

        with Session(engine) as session:
            service = ManualChangeService(session)

            # ===== GET CURRENT VERSION FOR CLASSIFICATION =====
            # We need current attributes to determine if change is correction
            current_version = service._get_active_version(part_number, data['model_code'])
            if current_version:
                current_attrs = service._get_part_attributes(current_version['part_id'])
            else:
                current_attrs = {}

            # ===== APPLY CLASSIFICATION =====
            domain, nature = ChangeClassifier.classify(
                data['changes'],
                current_attrs
            )

            logger.debug(
                "Classification for part %s: domain=%s, nature=%s",
                part_number, domain, nature
            )

            # 1. Create virtual breakpoint
            breakpoint_id = service.create_virtual_breakpoint(
                part_number=part_number,
                model_code=data['model_code'],
                change_reason=data['change_reason'],
                ticket_number=data.get('ticket_number'),
                changed_by=changed_by,
                changes=data['changes'],
                change_domain=domain,
                change_nature=nature
            )

            # 2. Apply changes
            result = service.apply_changes_to_part(
                part_number=part_number,
                model_code=data['model_code'],
                changes=data['changes'],
                breakpoint_id=breakpoint_id,
                changed_by=changed_by,
                change_domain=domain,
                change_nature=nature
            )

            # 3. Get breakpoint info for response
            bp_info = session.get(BreakpointData, breakpoint_id)

            return jsonify({
                'success': True,
                'message': 'Part modified successfully',
                'breakpoint_id': breakpoint_id,
                'breakpoint_number': bp_info.breakpoint_number if bp_info else None,
                'old_part_id': result['old_part_id'],
                'new_part_id': result['new_part_id'],
                'old_version': result['old_version'],
                'new_version': result['new_version'],
                'changes_applied': result['changes_applied'],
                'change_domain': domain,     # ← НОВОЕ В ОТВЕТЕ
                'change_nature': nature,     # ← НОВОЕ В ОТВЕТЕ
                'created_at': datetime.now().isoformat(),
                'changed_by': changed_by
            }), 200

    except ValueError as e:
        logger.error("Validation error modifying part %s: %s", part_number, str(e))
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

    except Exception as e:
        logger.error("Error modifying part %s: %s", part_number, str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500


@api_bp.route('/parts/<string:part_number>/history', methods=['GET'])
@limiter.limit("100 per minute")
@jwt_required
def get_part_history(part_number):
    """
    Get full history of a part including manual changes.
    
    Returns all versions and transitions (both from BP pipeline and manual changes).
    """
    try:
        model_code = request.args.get('model_code')
        limit = request.args.get('limit', 100, type=int)

        if limit > 500:
            limit = 500

        engine = initialize_database(create_tables=False)
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500

        with Session(engine) as session:
            # Build query
            query = text("""
                SELECT 
                    p.part_id,
                    p.part_number,
                    p.version_number,
                    p.part_name,
                    p.part_weight_kg,
                    p.supplier_id,
                    p.created_at,
                    s.supplier_name,
                    ptm.is_active,
                    ptm.deactivated_at,
                    ptb.breakpoint_id,
                    bd.breakpoint_number,
                    bd.breakpoint_date,
                    bd.description,
                    CASE 
                        WHEN bd.breakpoint_number LIKE 'MAN-%' THEN 'manual'
                        ELSE 'automatic'
                    END as change_type
                FROM part_data p
                LEFT JOIN supplier_data s ON p.supplier_id = s.supplier_id
                LEFT JOIN part_to_model ptm ON p.part_id = ptm.part_id
                LEFT JOIN part_to_breakpoint ptb ON p.part_id = ptb.new_part_id OR p.part_id = ptb.old_part_id
                LEFT JOIN breakpoint_data bd ON ptb.breakpoint_id = bd.breakpoint_id
                WHERE p.part_number = :part_number
                AND (:model_code IS NULL OR ptm.model_id = (
                    SELECT model_id FROM model_data WHERE model_code = :model_code
                ))
                ORDER BY p.version_number DESC
                LIMIT :limit
            """)

            results = session.execute(query, {
                'part_number': part_number,
                'model_code': model_code,
                'limit': limit
            }).all()

            history = []
            for row in results:
                row_dict = dict(row._mapping)
                # Convert datetime objects to ISO format for JSON serialization
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                history.append(row_dict)

            return jsonify({
                'success': True,
                'part_number': part_number,
                'total_versions': len(history),
                'history': history
            }), 200

    except Exception as e:
        logger.error("Error getting history for part %s: %s", part_number, str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Failed to get history: {str(e)}'
        }), 500


@api_bp.route('/parts/<string:part_number>/versions/<int:version>/rollback', methods=['POST'])
@limiter.limit("5 per minute")
@jwt_required
@role_required(['admin', 'engineer'])
def rollback_part_version(part_number, version):
    """
    Rollback part to a specific version.
    
    This creates a new version that is a copy of the specified version.
    """
    try:
        # Validate request
        schema = RollbackSchema()
        try:
            data = schema.load(request.json or {})
        except ValidationError as err:
            return jsonify({
                'success': False,
                'error': 'Validation error',
                'details': err.messages
            }), 400

        model_code = data['model_code']
        reason = data.get('reason', f'Rollback to version {version}')
        changed_by = request.user_email or request.user or 'system'
        ticket_number = data.get('ticket_number')

        engine = initialize_database(create_tables=False)
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500

        with Session(engine) as session:
            # Get target version
            target_query = text("""
                SELECT p.* FROM part_data p
                WHERE p.part_number = :part_number
                  AND p.version_number = :version
                LIMIT 1
            """)
            target = session.execute(target_query, {
                'part_number': part_number,
                'version': version
            }).first()

            if not target:
                return jsonify({
                    'success': False,
                    'error': f'Version {version} not found for part {part_number}'
                }), 404

            target_dict = dict(target._mapping)

            # Get current active version
            service = ManualChangeService(session)
            current = service._get_active_version(part_number, model_code)

            if not current:
                return jsonify({
                    'success': False,
                    'error': f'No active version found for part {part_number} on model {model_code}'
                }), 404

            # Prepare changes to rollback to target version
            target_attrs = service._get_part_attributes(target_dict['part_id'])
            current_attrs = service._get_part_attributes(current['part_id'])

            changes = {}
            for key, value in target_attrs.items():
                if key in current_attrs and str(current_attrs.get(key)) != str(value):
                    changes[key] = value

            if not changes:
                return jsonify({
                    'success': True,
                    'message': f'Part {part_number} is already at version {version}',
                    'breakpoint_id': None,
                    'old_version': current['version_number'],
                    'new_version': current['version_number']
                }), 200

            # ===== APPLY CLASSIFICATION FOR ROLLBACK =====
            domain, nature = ChangeClassifier.classify(changes, current_attrs)

            logger.debug(
                "Rollback classification for part %s: domain=%s, nature=%s",
                part_number, domain, nature
            )

            # Create virtual breakpoint for rollback
            breakpoint_id = service.create_virtual_breakpoint(
                part_number=part_number,
                model_code=model_code,
                change_reason=f"Rollback to version {version}: {reason}",
                ticket_number=ticket_number,
                changed_by=changed_by,
                changes=changes,
                change_domain=domain,
                change_nature=nature
            )

            # Apply changes
            result = service.apply_changes_to_part(
                part_number=part_number,
                model_code=model_code,
                changes=changes,
                breakpoint_id=breakpoint_id,
                changed_by=changed_by,
                change_domain=domain,
                change_nature=nature
            )

            return jsonify({
                'success': True,
                'message': f'Successfully rolled back part {part_number} to version {version}',
                'breakpoint_id': breakpoint_id,
                'old_version': result['old_version'],
                'new_version': result['new_version'],
                'changes_applied': result['changes_applied'],
                'change_domain': domain,
                'change_nature': nature,
                'changed_by': changed_by
            }), 200

    except ValueError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400
    except Exception as e:
        logger.error("Error rolling back part %s: %s", part_number, str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Failed to rollback: {str(e)}'
        }), 500


@api_bp.route('/parts/<string:part_number>/versions', methods=['GET'])
@limiter.limit("100 per minute")
@jwt_required
def get_part_versions(part_number):
    """
    Get all versions of a part.
    
    Returns simplified list of versions without full history details.
    """
    try:
        model_code = request.args.get('model_code')

        engine = initialize_database(create_tables=False)
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500

        with Session(engine) as session:
            query = text("""
                SELECT 
                    p.part_id,
                    p.version_number,
                    p.part_name,
                    p.part_weight_kg,
                    p.created_at,
                    ptm.is_active,
                    ptm.deactivated_at,
                    s.supplier_name
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
                row_dict = dict(row._mapping)
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                versions.append(row_dict)

            return jsonify({
                'success': True,
                'part_number': part_number,
                'total_versions': len(versions),
                'versions': versions
            }), 200

    except Exception as e:
        logger.error("Error getting versions for part %s: %s", part_number, str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Failed to get versions: {str(e)}'
        }), 500


@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint (no auth required)."""
    try:
        engine = initialize_database(create_tables=False)
        db_status = 'healthy' if engine else 'unhealthy'

        return jsonify({
            'status': 'healthy',
            'service': 'MFT Manual Modification API',
            'version': '1.0.0',
            'timestamp': datetime.now().isoformat(),
            'database': db_status
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'service': 'MFT Manual Modification API',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 503


@api_bp.route('/auth/token', methods=['POST'])
@limiter.limit("20 per minute")
def generate_token():
    """
    Generate JWT token for testing.
    
    This endpoint is for development only. In production, use a proper
    authentication system (OAuth2, LDAP, etc.)
    """
    try:
        data = request.json or {}
        username = data.get('username', 'test_user')
        email = data.get('email', 'test@company.com')
        roles = data.get('roles', ['engineer'])

        # In production, validate credentials against your auth system

        payload = {
            'sub': username,
            'email': email,
            'roles': roles,
            'exp': datetime.utcnow() + datetime.timedelta(hours=8),
            'iat': datetime.utcnow()
        }

        token = jwt.encode(
            payload,
            app.config['SECRET_KEY'],
            algorithm=app.config['JWT_ALGORITHM']
        )

        return jsonify({
            'success': True,
            'token': token,
            'expires_in': 28800,  # 8 hours in seconds
            'user': {
                'username': username,
                'email': email,
                'roles': roles
            }
        }), 200

    except Exception as e:
        logger.error("Error generating token: %s", str(e))
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================================
# REGISTER BLUEPRINT AND ERROR HANDLERS
# ============================================================================

app.register_blueprint(api_bp)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'success': False, 'error': 'Resource not found'}), 404

@app.errorhandler(429)
def ratelimit_handler(error):
    return jsonify({
        'success': False,
        'error': 'Rate limit exceeded. Please try again later.',
        'details': str(error.description)
    }), 429

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'success': False, 'error': 'Internal server error'}), 500


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run the app
    app.run(
        host='0.0.0.0',
        port=8000,
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    )
