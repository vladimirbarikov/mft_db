"""
Flask API for Database User Management.

This module provides a REST API for creating and managing PostgreSQL users
with different privilege levels through Flask endpoints.

Endpoints:
    POST /api/users/create - Create a new user with specified role
    GET /api/users/test-connection/<username> - Test user connection
    GET /api/users/list - Get list of all users
    DELETE /api/users/delete/<username> - Delete a user

Components used:
    - Flask: Web framework for creating API
    - SQLAlchemy: For executing administrative SQL queries
    - user_roles.py: User role management module

Usage example:
    Creating an administrator:
    POST /api/users/create
    {
        "username": "sys_admin",
        "password": "secure_password_123",
        "role": "admin",
        "description": "System administrator"
    }

    Testing connection:
    GET /api/users/test-connection/sys_admin

    Getting user list:
    GET /api/users/list

    Deleting a user:
    DELETE /api/users/delete/sys_admin

Environment requirements:
    - Installed libraries: Flask, SQLAlchemy, python-dotenv, psycopg2-binary
    - Configured .env file with database connection parameters
    - Access to PostgreSQL with administrative privileges

Version: 1.0.0
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""
import os
import logging
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError, IntegrityError
from dotenv import load_dotenv

# Import user classes from user_roles.py
from database.user_roles import DatabaseAdmin, DatabaseEditor, DatabaseViewer

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

# Create Flask application
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'mft_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_admin_engine():
    """Create connection with administrative privileges"""
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        return engine
    except OperationalError as e:
        logger.error("Database connection error: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error creating engine: %s", e)
        return None

@app.route('/api/users/create', methods=['POST'])
def create_user():
    """
    Create a new database user
    """
    try:
        # Check JSON
        if not request.is_json:
            return jsonify({
                'success': False,
                'error': 'Content-Type must be application/json'
            }), 415

        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'JSON request body is required'
            }), 400

        required_fields = ['username', 'password', 'role']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required field: {field}'
                }), 400

        username = data['username'].strip()
        password = data['password']
        role = data['role'].lower().strip()
        description = data.get('description', '')

        # User name validation
        if not username:
            return jsonify({
                'success': False,
                'error': 'Username cannot be empty'
            }), 400

        if len(username) > 50:  # PostgreSQL limit for role names
            return jsonify({
                'success': False,
                'error': 'Username must be 50 characters or less'
            }), 400

        # Password validation
        if not password:
            return jsonify({
                'success': False,
                'error': 'Password cannot be empty'
            }), 400

        # Role validation
        valid_roles = ['admin', 'editor', 'viewer']
        if role not in valid_roles:
            return jsonify({
                'success': False,
                'error': f'Invalid role. Valid values: {", ".join(valid_roles)}'
            }), 400

        # Creating user objects
        if role == 'admin':
            user = DatabaseAdmin(username, password, description)
        elif role == 'editor':
            user = DatabaseEditor(username, password, description)
        else:  # viewer
            user = DatabaseViewer(username, password, description)

        # Creating users in database
        result = user.create_db_user()

        if result:
            logger.info("User '%s' successfully created with role '%s'", username, role)
            return jsonify({
                'success': True,
                'message': f'User {username} created successfully',
                'user': {
                    'username': username,
                    'role': role,
                    'description': description,
                    'connection_string': user.get_connection_string()
                }
            }), 201
        else:
            logger.error("Failed to create user '%s'", username)
            return jsonify({
                'success': False,
                'error': f'Failed to create user {username}'
            }), 500

    except KeyError as e:
        logger.error("Missing key in request: %s", e)
        return jsonify({
            'success': False,
            'error': f'Invalid request structure: missing {str(e)}'
        }), 400

    except ValueError as e:
        logger.error("Value error: %s", e)
        return jsonify({
            'success': False,
            'error': f'Invalid value: {str(e)}'
        }), 400

    except SQLAlchemyError as e:
        logger.error("Database error creating user: %s", e)
        if isinstance(e, IntegrityError):
            return jsonify({
                'success': False,
                'error': f'User {username} already exists'
            }), 409
        elif isinstance(e, OperationalError):
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            }), 500
        else:
            return jsonify({
                'success': False,
                'error': f'Database error: {str(e)}'
            }), 500

    except Exception as e:
        logger.error("Unexpected error creating user: %s", e)
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@app.route('/api/users/test-connection/<username>', methods=['GET'])
def test_user_connection(username: str):
    """
    Test user connection to database
    """
    engine = None
    try:
        if not username or not username.strip():
            return jsonify({
                'success': False,
                'error': 'Username is required'
            }), 400

        username = username.strip()

        engine = get_admin_engine()
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Failed to connect to database'
            }), 500

        with engine.connect() as connection:
            query = text("""
                SELECT 
                    rolname,
                    rolsuper,
                    rolcanlogin,
                    rolcreatedb,
                    rolcreaterole
                FROM pg_roles 
                WHERE rolname = :username
            """)
            result = connection.execute(query, {'username': username})
            user_info = result.fetchone()

            if not user_info:
                return jsonify({
                    'success': False,
                    'error': f'User {username} not found'
                }), 404

            user_info_dict = {
                'rolname': user_info[0],
                'rolsuper': user_info[1],
                'rolcanlogin': user_info[2],
                'rolcreatedb': user_info[3],
                'rolcreaterole': user_info[4]
            }

            if not user_info_dict['rolcanlogin']:
                return jsonify({
                    'success': False,
                    'error': f'User {username} does not have login permission'
                }), 400

            return jsonify({
                'success': True,
                'message': f'User {username} exists and can connect',
                'user_info': {
                    'username': user_info_dict['rolname'],
                    'is_superuser': user_info_dict['rolsuper'],
                    'can_login': user_info_dict['rolcanlogin'],
                    'can_create_db': user_info_dict['rolcreatedb'],
                    'can_create_roles': user_info_dict['rolcreaterole']
                }
            })

    except OperationalError as e:
        logger.error("Database connection error testing user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': 'Database connection failed'
        }), 500

    except ProgrammingError as e:
        logger.error("SQL error testing user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': 'Database query error'
        }), 500

    except Exception as e:
        logger.error("Unexpected error testing user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

    finally:
        if engine:
            engine.dispose()

@app.route('/api/users/list', methods=['GET'])
def list_users():
    """
    Get list of all database users
    """
    engine = None
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            query = text("""
                SELECT 
                    rolname as username,
                    rolsuper as is_superuser,
                    rolcanlogin as can_login,
                    rolcreatedb as can_create_db,
                    rolcreaterole as can_create_roles,
                    rolconnlimit as connection_limit,
                    rolvaliduntil as password_valid_until
                FROM pg_roles 
                WHERE rolcanlogin = true 
                ORDER BY rolname
            """)
            result = connection.execute(query)
            users = []

            for row in result:
                users.append({
                    'username': row[0],
                    'is_superuser': row[1],
                    'can_login': row[2],
                    'can_create_db': row[3],
                    'can_create_roles': row[4],
                    'connection_limit': row[5],
                    'password_valid_until': str(row[6]) if row[6] else None
                })

            return jsonify({
                'success': True,
                'users': users,
                'count': len(users)
            })

    except OperationalError as e:
        logger.error("Database connection error getting user list: %s", e)
        return jsonify({
            'success': False,
            'error': 'Database connection failed'
        }), 500

    except ProgrammingError as e:
        logger.error("SQL error getting user list: %s", e)
        return jsonify({
            'success': False,
            'error': 'Database query error'
        }), 500

    except Exception as e:
        logger.error("Unexpected error getting user list: %s", e)
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

    finally:
        if engine:
            engine.dispose()

@app.route('/api/users/delete/<username>', methods=['DELETE'])
def delete_user(username: str):
    """
    Delete a database user
    """
    engine = None
    try:
        # User name validation
        if not username or not username.strip():
            return jsonify({
                'success': False,
                'error': 'Username is required'
            }), 400

        username = username.strip()

        # Check for protected users
        protected_users = [DB_USER, 'postgres']
        if username in protected_users:
            return jsonify({
                'success': False,
                'error': f'Cannot delete system user {username}'
            }), 400

        engine = get_admin_engine()
        if not engine:
            return jsonify({
                'success': False,
                'error': 'Failed to connect to database'
            }), 500

        with engine.connect() as connection:
            # Verifying the user's existence
            check_query = text("SELECT 1 FROM pg_roles WHERE rolname = :username")
            result = connection.execute(check_query, {'username': username})

            if not result.fetchone():
                return jsonify({
                    'success': False,
                    'error': f'User {username} not found'
                }), 404

            try:
                # Revocation of privileges
                revoke_query = text(f"REVOKE ALL PRIVILEGES FROM {username}")
                connection.execute(revoke_query)

                # Deleting a role
                delete_query = text(f"DROP ROLE IF EXISTS {username}")
                connection.execute(delete_query)

            except ProgrammingError as e:
                error_msg = str(e)
                # Check if the user has active connections.
                if "cannot be dropped" in error_msg and "has dependent objects" in error_msg:
                    logger.error("User %s has dependent objects: %s", username, e)
                    return jsonify({
                        'success': False,
                        'error': f'Cannot delete user {username} because they own database objects. Reassign or drop objects first.'
                    }), 409
                elif "cannot be dropped" in error_msg and "active sessions" in error_msg:
                    logger.error("User %s has active sessions: %s", username, e)
                    return jsonify({
                        'success': False,
                        'error': f'Cannot delete user {username} because they have active sessions. Terminate sessions first.'
                    }), 409
                else:
                    raise

            logger.info("User '%s' successfully deleted", username)
            return jsonify({
                'success': True,
                'message': f'User {username} successfully deleted'
            })

    except OperationalError as e:
        logger.error("Database connection error deleting user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': 'Database connection failed'
        }), 500

    except ProgrammingError as e:
        logger.error("SQL error deleting user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500

    except Exception as e:
        logger.error("Unexpected error deleting user %s: %s", username, e)
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

    finally:
        if engine:
            engine.dispose()

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check API and database connection health"""
    engine = None
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            result = connection.execute(text("SELECT version(), current_timestamp, current_database()"))
            row = result.fetchone()
            db_version = row[0]
            timestamp = row[1]
            db_name = row[2]

            return jsonify({
                'status': 'healthy',
                'message': 'API and database are working normally',
                'database': {
                    'available': True,
                    'version': db_version,
                    'name': db_name,
                    'timestamp': str(timestamp)
                }
            })

    except OperationalError as e:
        logger.error("Database connection health check failed: %s", e)
        return jsonify({
            'status': 'degraded',
            'message': 'Database connection failed',
            'database': {
                'available': False,
                'error': str(e)
            }
        }), 500

    except Exception as e:
        logger.error("Health check error: %s", e)
        return jsonify({
            'status': 'error',
            'message': 'Internal server error',
            'database': 'unknown'
        }), 500

    finally:
        if engine:
            engine.dispose()

@app.errorhandler(404)
def not_found(_error):
    """Handler for non-existent endpoints"""
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404

@app.errorhandler(405)
def method_not_allowed(_error):
    """Handler for invalid HTTP methods"""
    return jsonify({
        'success': False,
        'error': 'Method not allowed for this endpoint'
    }), 405

if __name__ == '__main__':
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port_str = os.getenv('FLASK_PORT', '5000')
    port = int(port_str)
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

    logger.info("Starting User Manager API on %s:%s", host, port)
    logger.info("Debug mode: %s", debug)
    logger.info("Database connection: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)

    app.run(host=host, port=port, debug=debug)
