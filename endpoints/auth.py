# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Authentication and Authorization Module for MFT APIs.

This module provides centralized JWT authentication and role-based access control
for all API endpoints. It uses Flask's `g` object for request-scoped user data
and provides decorators for protecting endpoints.

ROLES (from .env):
    - admin: Full access to all operations
    - editor: Read, insert, and update privileges (API level)
    - viewer: Read-only access (API level)

These roles correspond to DatabaseUser classes in user_roles.py:
    - admin    → DatabaseAdmin (full database access)
    - editor   → DatabaseEditor (read, insert, update)
    - viewer   → DatabaseViewer (read-only)

KEY FEATURES:
    - JWT token generation and validation
    - Role-based access control (admin, editor, viewer)
    - User information storage in Flask's `g` object
    - Consistent error handling
    - Environment-based configuration

USAGE:
    from endpoints.auth import jwt_required, role_required, get_current_user, generate_token

    # Protect endpoint with JWT
    @jwt_required
    @role_required(['admin', 'editor'])
    def my_endpoint():
        user = get_current_user()
        return jsonify({'user': user})

    # Generate token (for testing/development)
    token = generate_token('username', 'user@company.com', ['editor'])

DEPENDENCIES:
    - Flask 3.0.3+: Web framework
    - PyJWT 2.8.0+: JWT token handling
    - python-dotenv: Environment variable management

Version: 1.0.0
Compatibility: Python 3.14.4+, Flask 3.0.3+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-19
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import os
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, Any, List, Optional, Tuple

# Third-party imports
from flask import request, jsonify, g
import jwt
from dotenv import load_dotenv

# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    PROJECT_ROOT = Path("/opt/airflow")

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# JWT Configuration
SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
JWT_EXPIRATION_HOURS = int(os.getenv('JWT_EXPIRATION_HOURS', '8'))

# Role names from .env (matching user_roles.py)
ADMIN_ROLE = os.getenv('ADMIN_ROLE', 'admin')
EDITOR_ROLE = os.getenv('EDITOR_ROLE', 'editor')
VIEWER_ROLE = os.getenv('VIEWER_ROLE', 'viewer')

# Available roles
ROLES = {
    'ADMIN': ADMIN_ROLE,
    'EDITOR': EDITOR_ROLE,
    'VIEWER': VIEWER_ROLE,
}

# Role hierarchy (higher level includes lower permissions)
# admin > editor > viewer
ROLE_HIERARCHY = {
    ROLES['ADMIN']: [ROLES['ADMIN'], ROLES['EDITOR'], ROLES['VIEWER']],
    ROLES['EDITOR']: [ROLES['EDITOR'], ROLES['VIEWER']],
    ROLES['VIEWER']: [ROLES['VIEWER']],
}

# Mapping from API roles to DatabaseUser classes
# (for reference, actual mapping is in user_roles.py)
ROLE_TO_DATABASE_USER = {
    ROLES['ADMIN']: 'DatabaseAdmin',      # Full database access
    ROLES['EDITOR']: 'DatabaseEditor',    # Read, insert, update
    ROLES['VIEWER']: 'DatabaseViewer',    # Read-only
}


# ============================================================================
# TOKEN GENERATION
# ============================================================================

def generate_token(
    username: str,
    email: Optional[str] = None,
    roles: Optional[List[str]] = None,
    expiration_hours: Optional[int] = None,
    custom_claims: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a JWT token for authentication.

    Args:
        username: Username or subject (required)
        email: User email (optional)
        roles: List of roles (default: ['viewer'])
        expiration_hours: Token expiration in hours (default: JWT_EXPIRATION_HOURS)
        custom_claims: Additional custom claims (optional)

    Returns:
        JWT token as string

    Example:
        >>> token = generate_token('john_doe', 'john@company.com', ['admin', 'editor'])
        >>> print(token)
        eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
    """
    if roles is None:
        roles = [ROLES['VIEWER']]

    if expiration_hours is None:
        expiration_hours = JWT_EXPIRATION_HOURS

    # Validate roles
    valid_roles = list(ROLES.values())
    for role in roles:
        if role not in valid_roles:
            logger.warning("Invalid role '%s' in token generation, skipping", role)

    payload = {
        'sub': username,
        'email': email or username,
        'roles': roles,
        'exp': datetime.utcnow() + timedelta(hours=expiration_hours),
        'iat': datetime.utcnow(),
    }

    # Add custom claims if provided
    if custom_claims:
        payload.update(custom_claims)

    token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.debug("Generated token for user '%s' with roles %s", username, roles)

    return token


# ============================================================================
# TOKEN DECODING
# ============================================================================


def decode_token(token: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Decode and validate a JWT token.

    Args:
        token: JWT token string

    Returns:
        Tuple (success, payload, error_message)
        - success: True if token is valid
        - payload: Decoded payload if valid
        - error_message: Error description if invalid
    """
    try:
        if not token or not isinstance(token, str):
            return False, None, "Token is empty or invalid type"

        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])

        # Validate roles in payload
        roles = payload.get('roles', [])
        valid_roles = list(ROLES.values())
        invalid_roles = [r for r in roles if r not in valid_roles]
        if invalid_roles:
            logger.warning("Invalid roles in token: %s", invalid_roles)
            # Filter out invalid roles
            payload['roles'] = [r for r in roles if r in valid_roles]

        return True, payload, None

    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        return False, None, "Token has expired"

    except jwt.InvalidTokenError as e:
        logger.warning("Invalid token: %s", str(e))
        return False, None, f"Invalid token: {str(e)}"

    except AttributeError as e:
        logger.error("Attribute error decoding token: %s", str(e))
        return False, None, "Invalid token format"

    except Exception as e:
        logger.error("Unexpected error decoding token: %s", str(e), exc_info=True)
        return False, None, f"Unexpected error: {str(e)}"


# ============================================================================
# USER DATA ACCESS
# ============================================================================

def set_current_user(
    username: str,
    email: Optional[str] = None,
    roles: Optional[List[str]] = None
) -> None:
    """
    Store current user information in Flask's `g` object.

    Args:
        username: Username
        email: User email (optional)
        roles: List of roles (optional)
    """
    g.user = username
    g.user_email = email or username
    g.user_roles = roles or []


def get_current_user() -> Dict[str, Any]:
    """
    Get current user information from Flask's `g` object.

    Returns:
        Dict with user information:
        - username: Username
        - email: User email
        - roles: List of roles

    Example:
        >>> user = get_current_user()
        >>> print(f"User: {user['username']}, Roles: {user['roles']}")
    """
    return {
        'username': getattr(g, 'user', 'unknown'),
        'email': getattr(g, 'user_email', 'unknown'),
        'roles': getattr(g, 'user_roles', []),
    }


def get_current_username() -> str:
    """Get current username."""
    return getattr(g, 'user', 'unknown')


def get_current_email() -> str:
    """Get current user email."""
    return getattr(g, 'user_email', 'unknown')


def get_current_roles() -> List[str]:
    """Get current user roles."""
    return getattr(g, 'user_roles', [])


def has_role(role: str) -> bool:
    """
    Check if current user has a specific role.

    Args:
        role: Role to check

    Returns:
        True if user has the role, False otherwise

    Example:
        >>> if has_role('admin'):
        ...     print("User is admin")
    """
    return role in get_current_roles()


def has_any_role(roles: List[str]) -> bool:
    """
    Check if current user has any of the specified roles.

    Args:
        roles: List of roles to check

    Returns:
        True if user has any of the roles, False otherwise

    Example:
        >>> if has_any_role(['admin', 'editor']):
        ...     print("User has admin or editor role")
    """
    user_roles = get_current_roles()
    return any(role in user_roles for role in roles)


def get_database_role() -> Optional[str]:
    """
    Get the corresponding database role for the current user.

    Returns the highest privilege database role that matches the user's roles.
    Mapping: admin → DatabaseAdmin, editor → DatabaseEditor, viewer → DatabaseViewer

    Returns:
        Database role name or None if no matching role found

    Example:
        >>> db_role = get_database_role()
        >>> # Returns: 'DatabaseAdmin', 'DatabaseEditor', or 'DatabaseViewer'
    """
    user_roles = get_current_roles()

    # Check in order of highest privilege
    if ROLES['ADMIN'] in user_roles:
        return ROLE_TO_DATABASE_USER[ROLES['ADMIN']]
    elif ROLES['EDITOR'] in user_roles:
        return ROLE_TO_DATABASE_USER[ROLES['EDITOR']]
    elif ROLES['VIEWER'] in user_roles:
        return ROLE_TO_DATABASE_USER[ROLES['VIEWER']]

    return None


# ============================================================================
# DECORATORS
# ============================================================================

def jwt_required(f):
    """
    Decorator to protect endpoints with JWT authentication.

    Validates JWT token from Authorization header and stores user info in `g`.

    Usage:
        @app.route('/protected')
        @jwt_required
        def protected_endpoint():
            user = get_current_user()
            return jsonify({'user': user})

    Returns:
        401: Missing or invalid Authorization header
        401: Invalid token or expired
    """
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

            # Decode and validate token
            success, payload, error = decode_token(token)

            if not success:
                return jsonify({
                    'success': False,
                    'error': error
                }), 401

            # Check if payload is None (shouldn't happen if success=True, but safe check)
            if payload is None:
                logger.error("Token payload is None after successful decode")
                return jsonify({
                    'success': False,
                    'error': 'Invalid token payload'
                }), 401

            # Store user info in g
            set_current_user(
                username=payload.get('sub', 'unknown'),
                email=payload.get('email'),
                roles=payload.get('roles', [])
            )

            logger.debug("JWT authenticated: %s", g.user)

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

        except AttributeError as e:
            logger.error("Attribute error in JWT validation: %s", str(e))
            return jsonify({
                'success': False,
                'error': 'Invalid token format'
            }), 401

        except Exception as e:
            logger.error("JWT validation error: %s", str(e), exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Authentication error'
            }), 401

        return f(*args, **kwargs)

    return decorated_function


def role_required(required_roles: List[str]):
    """
    Decorator to check user roles.

    Requires jwt_required to be applied first.

    Usage:
        @app.route('/editor-only')
        @jwt_required
        @role_required(['editor', 'admin'])
        def editor_endpoint():
            return jsonify({'message': 'Editor or admin only'})

    Args:
        required_roles: List of roles that are allowed to access the endpoint

    Returns:
        401: Authentication required (jwt_required not applied)
        403: Insufficient permissions
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_roles = getattr(g, 'user_roles', None)

            if user_roles is None:
                return jsonify({
                    'success': False,
                    'error': 'Authentication required'
                }), 401

            if not any(role in user_roles for role in required_roles):
                return jsonify({
                    'success': False,
                    'error': f'Insufficient permissions. Required roles: {required_roles}'
                }), 403

            return f(*args, **kwargs)

        return decorated_function

    return decorator


def role_hierarchy_required(required_role: str):
    """
    Decorator to check user role with hierarchy support.

    Higher roles include lower permissions:
    - admin > editor > viewer

    Usage:
        @app.route('/editor-plus')
        @jwt_required
        @role_hierarchy_required('editor')
        def editor_endpoint():
            return jsonify({'message': 'Editor or higher'})

    Args:
        required_role: Minimum role required (admin, editor, viewer)

    Returns:
        401: Authentication required
        403: Insufficient permissions
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_roles = getattr(g, 'user_roles', None)

            if user_roles is None:
                return jsonify({
                    'success': False,
                    'error': 'Authentication required'
                }), 401

            # Check if user has the required role or higher
            allowed_roles = ROLE_HIERARCHY.get(required_role, [required_role])

            if not any(role in user_roles for role in allowed_roles):
                return jsonify({
                    'success': False,
                    'error': f'Insufficient permissions. Required role: {required_role} or higher'
                }), 403

            return f(*args, **kwargs)

        return decorated_function

    return decorator


# ============================================================================
# PUBLIC INTERFACE
# ============================================================================

__all__ = [
    # Constants
    'ROLES',
    'ROLE_HIERARCHY',
    'SECRET_KEY',
    'JWT_ALGORITHM',
    'ADMIN_ROLE',
    'EDITOR_ROLE',
    'VIEWER_ROLE',
    'ROLE_TO_DATABASE_USER',

    # Token functions
    'generate_token',
    'decode_token',

    # User data functions
    'set_current_user',
    'get_current_user',
    'get_current_username',
    'get_current_email',
    'get_current_roles',
    'has_role',
    'has_any_role',
    'get_database_role',

    # Decorators
    'jwt_required',
    'role_required',
    'role_hierarchy_required',
]
