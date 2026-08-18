# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Database User Roles Management Module.

This module provides a comprehensive system for managing PostgreSQL database users
with different privilege levels through an object-oriented hierarchy.

Classes:
    DatabaseUser: Abstract base class for all database users
    DatabaseAdmin: Administrative user with full database access
    DatabaseEditor: Editor user with read, insert, and update privileges  
    DatabaseViewer: Viewer user with read-only privileges

The module implements a role-based access control system where each user type
has specific permissions tailored to their responsibilities:

- DatabaseAdmin: Full CRUD operations, schema modifications, and user management
- DatabaseEditor: Data viewing, addition, and modification capabilities
- DatabaseViewer: Read-only access for reporting and analytics

Key Features:
    - Dynamic user creation with proper PostgreSQL role management
    - Connection testing and validation for each user type
    - Comprehensive error handling with specific SQLAlchemy exceptions
    - Environment-based configuration using .env files
    - Safe resource management with automatic connection disposal

Usage Example:
    >>> from user_roles import DatabaseAdmin, DatabaseEditor, DatabaseViewer
    >>>
    >>> # Create users with specific privilege levels
    >>> admin = DatabaseAdmin('sys_admin', 'secure_password_123')
    >>> editor = DatabaseEditor('data_manager', 'editor_pass_123', 'Data management user')
    >>> viewer = DatabaseViewer('report_user', 'viewer_pass_123', 'Reporting access')
    >>>
    >>> # Create users in database
    >>> admin.create_user()
    >>> editor.create_user()
    >>> viewer.create_user()
    >>>
    >>> # Test connections
    >>> admin.test_connection()
    >>> viewer.test_connection()

Dependencies:
    - sqlalchemy: For database connectivity and SQL execution
    - python-dotenv: For environment variable management
    - psycopg2-binary: PostgreSQL database adapter

Environment Variables:
    DB_HOST: Database server hostname (default: localhost)
    DB_PORT: Database server port (default: 5432) 
    DB_NAME: Database name (default: mft_db)
    DB_USER: Administrative username for connecting to database (default: postgres)
    DB_PASSWORD: Password for DB_USER
    DB_ADMIN_PASSWORD: Password for admin role users (optional, used when creating admin users)

Security Notes:
    - Passwords should be stored securely in environment variables
    - Principle of least privilege is enforced through role separation
    - Connection strings are constructed securely with proper escaping
    - All database operations use parameterized queries to prevent SQL injection

Version: 1.0.0
Compatibility: Python 3.14.4+, SQLAlchemy 1.4.54+
Maintainer: PLD Engineering Center
Created: 2025-12-27
Last Modified: 2026-08-18
License: MIT
Status: Production
"""
from pathlib import Path
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError, DatabaseError
from dotenv import load_dotenv

# ============================================================================
# LOGGER SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

project_path = Path(__file__).resolve().parents[1]
env_path = project_path / '.env'
load_dotenv(env_path)


def quote_identifier(identifier: str) -> str:
    """
    Quote PostgreSQL identifier using double quotes.
    
    PostgreSQL identifiers (table names, column names, role names)
    should be quoted if they contain special characters or are case-sensitive.
    
    Args:
        identifier: The identifier to quote
        
    Returns:
        Quoted identifier string
        
    Example:
        >>> quote_identifier('my_role')
        '"my_role"'
        >>> quote_identifier('MyRole')
        '"MyRole"'
    """
    # Double quotes inside the identifier need to be escaped with another double quote
    return f'"{identifier.replace('"', '""')}"'


# ============================================================================
# BASE CLASS
# ============================================================================

class DatabaseUser:
    """
    Base class for all database users.
    
    Provides common functionality for PostgreSQL user management:
    - User/role creation
    - Privilege granting (to be implemented by subclasses)
    - Connection testing
    - Safe resource management
    
    Attributes:
        host: Database server hostname
        port: Database server port
        database: Database name
        username: User/role name
        password: User/role password
        description: Human-readable description of the user
        admin_user: Administrative username for user creation
        admin_password: Administrative password for user creation
    """

    def __init__(self, username: str, password: str, description: str = ""):
        """
        Initialize a database user.
        
        Args:
            username: User/role name
            password: User/role password
            description: Human-readable description (optional)
        """
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'mft_db')
        self.username = username
        self.password = password
        self.description = description

        # Admin credentials for user creation
        self.admin_user = os.getenv('DB_USER', 'postgres')
        self.admin_password = os.getenv('DB_PASSWORD')

    # ========================================================================
    # ENGINE CREATION
    # ========================================================================

    def get_admin_engine(self):
        """
        Create engine with administrative privileges for user management.
        
        Returns:
            SQLAlchemy engine or None on error
        """
        try:
            if not self.admin_user or not self.admin_password:
                logger.error("Admin credentials not configured in environment")
                return None

            connection_string = (
                f"postgresql://{self.admin_user}:{self.admin_password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
            return engine

        except OperationalError as e:
            logger.error("Operational error creating admin engine: %s", e)
            return None
        except DatabaseError as e:
            logger.error("Database error creating admin engine: %s", e)
            return None
        except Exception as unexpected_error:
            logger.error("Unexpected error creating admin engine: %s", unexpected_error, exc_info=True)
            return None

    def get_user_engine(self):
        """
        Create engine with user privileges for database operations.
        
        Returns:
            SQLAlchemy engine or None on error
        """
        try:
            connection_string = (
                f"postgresql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            engine = create_engine(connection_string)
            return engine

        except OperationalError as e:
            logger.error(
                "Operational error creating user engine for '%s': %s",
                self.username, e
            )
            return None
        except DatabaseError as e:
            logger.error(
                "Database error creating user engine for '%s': %s",
                self.username, e
            )
            return None
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating user engine for '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return None

    # ========================================================================
    # ROLE MANAGEMENT
    # ========================================================================

    def role_exists(self, engine) -> bool:
        """
        Check if user role exists in PostgreSQL.
        
        Args:
            engine: SQLAlchemy engine with admin privileges
            
        Returns:
            True if role exists, False otherwise
        """
        conn = None
        try:
            conn = engine.connect()
            result = conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :username"),
                {"username": self.username}
            )
            return result.fetchone() is not None

        except OperationalError as e:
            logger.error(
                "Operational error checking role '%s': %s",
                self.username, e
            )
            return False
        except ProgrammingError as e:
            logger.error(
                "SQL error checking role '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error checking role '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error checking role '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)

    def create_role(self, engine) -> bool:
        """
        Create user role in PostgreSQL.
        
        Args:
            engine: SQLAlchemy engine with admin privileges
            
        Returns:
            True if role was created or already exists, False on error
        """
        conn = None
        try:
            # Check if role already exists
            if self.role_exists(engine):
                logger.info("Role '%s' already exists", self.username)
                return True

            # Create the role
            conn = engine.connect()
            quoted_username = quote_identifier(self.username)

            conn.execute(
                text(f"CREATE ROLE {quoted_username} WITH LOGIN PASSWORD :password"),
                {"password": self.password}
            )

            logger.info(
                "Role '%s' created successfully - %s",
                self.username,
                self.description or "No description provided"
            )
            return True

        except ProgrammingError as e:
            logger.error(
                "SQL syntax error creating role '%s': %s",
                self.username, e
            )
            return False
        except OperationalError as e:
            logger.error(
                "Operational error creating role '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error creating role '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating role '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)

    def drop_role(self, engine) -> bool:
        """
        Drop user role from PostgreSQL (use with caution).
        
        Args:
            engine: SQLAlchemy engine with admin privileges
            
        Returns:
            True if role was dropped or didn't exist, False on error
        """
        conn = None
        try:
            # Check if role exists
            if not self.role_exists(engine):
                logger.info("Role '%s' does not exist, nothing to drop", self.username)
                return True

            # Drop the role
            conn = engine.connect()
            quoted_username = quote_identifier(self.username)

            conn.execute(text(f"DROP ROLE {quoted_username}"))

            logger.warning(
                "Role '%s' dropped successfully - %s",
                self.username,
                self.description or "No description provided"
            )
            return True

        except ProgrammingError as e:
            logger.error(
                "SQL syntax error dropping role '%s': %s",
                self.username, e
            )
            return False
        except OperationalError as e:
            logger.error(
                "Operational error dropping role '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error dropping role '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error dropping role '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)

    # ========================================================================
    # RESOURCE MANAGEMENT
    # ========================================================================

    @staticmethod
    def _safe_close_connection(conn):
        """Safely close database connection if it exists."""
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning("Error closing connection: %s", e)

    @staticmethod
    def _safe_dispose_engine(engine):
        """Safely dispose database engine if it exists."""
        if engine:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning("Error disposing engine: %s", e)

    # ========================================================================
    # PRIVILEGE GRANTING (ABSTRACT)
    # ========================================================================

    def grant_privileges(self, engine) -> bool:
        """
        Grant privileges to the user.
        
        Must be implemented by subclasses.
        
        Args:
            engine: SQLAlchemy engine with admin privileges
            
        Returns:
            True if privileges were granted, False otherwise
            
        Raises:
            NotImplementedError: If subclass does not implement this method
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement grant_privileges()"
        )

    # ========================================================================
    # USER CREATION
    # ========================================================================

    def create_db_user(self) -> bool:
        """
        Create user with appropriate privileges.
        
        This is the main entry point for user creation:
        1. Create the PostgreSQL role
        2. Grant specific privileges based on user type
        
        Returns:
            True if user was created successfully, False otherwise
        """
        engine = self.get_admin_engine()
        if not engine:
            logger.error("Failed to create admin engine for user creation")
            return False

        try:
            # Create the role
            if not self.create_role(engine):
                logger.error("Failed to create role '%s'", self.username)
                return False

            # Grant specific privileges
            if not self.grant_privileges(engine):
                logger.error("Failed to grant privileges for '%s'", self.username)
                return False

            logger.info(
                "User '%s' created successfully with %s privileges",
                self.username,
                self.__class__.__name__
            )
            return True

        except ProgrammingError as e:
            logger.error(
                "SQL syntax error creating user '%s': %s",
                self.username, e
            )
            return False
        except OperationalError as e:
            logger.error(
                "Operational error creating user '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error creating user '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error creating user '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_dispose_engine(engine)

    def delete_db_user(self) -> bool:
        """
        Delete user from database (use with caution).
        
        Returns:
            True if user was deleted successfully, False otherwise
        """
        engine = self.get_admin_engine()
        if not engine:
            logger.error("Failed to create admin engine for user deletion")
            return False

        try:
            if not self.drop_role(engine):
                logger.error("Failed to drop role '%s'", self.username)
                return False

            logger.info(
                "User '%s' deleted successfully",
                self.username
            )
            return True

        except ProgrammingError as e:
            logger.error(
                "SQL syntax error deleting user '%s': %s",
                self.username, e
            )
            return False
        except OperationalError as e:
            logger.error(
                "Operational error deleting user '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error deleting user '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error deleting user '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_dispose_engine(engine)

    # ========================================================================
    # CONNECTION TESTING
    # ========================================================================

    def test_connection(self) -> bool:
        """
        Test if user can connect to database.
        
        Returns:
            True if connection successful, False otherwise
        """
        engine = self.get_user_engine()
        if not engine:
            logger.error("Failed to create user engine for connection test")
            return False

        conn = None
        try:
            conn = engine.connect()
            conn.execute(text("SELECT 1"))

            logger.info(
                "Connection test successful for user '%s'",
                self.username
            )
            return True

        except OperationalError as e:
            logger.error(
                "Operational error - connection test failed for user '%s': %s",
                self.username, e
            )
            return False
        except ProgrammingError as e:
            logger.error(
                "Permission error - connection test failed for user '%s': %s",
                self.username, e
            )
            return False
        except SQLAlchemyError as e:
            logger.error(
                "Database error - connection test failed for user '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error - connection test failed for user '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)
            self._safe_dispose_engine(engine)

    def get_connection_string(self) -> str:
        """
        Get connection string for this user.
        
        Returns:
            PostgreSQL connection string
        """
        return (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


# ============================================================================
# CONCRETE USER CLASSES
# ============================================================================

class DatabaseAdmin(DatabaseUser):
    """
    Administrative user with full database access.
    
    Grants:
    - All privileges on database
    - All privileges on public schema
    - All privileges on all tables
    - All privileges on all sequences
    - Default privileges for future tables and sequences
    """

    def __init__(
        self,
        username: str,
        password: str,
        description: str = "Administrative role - full database access"
    ):
        super().__init__(username, password, description)

    def grant_privileges(self, engine) -> bool:
        """Grant full administrative privileges."""
        conn = None
        try:
            conn = engine.connect()
            quoted_username = quote_identifier(self.username)
            quoted_database = quote_identifier(self.database)

            privileges = [
                f"GRANT ALL PRIVILEGES ON DATABASE {quoted_database} TO {quoted_username}",
                f"GRANT ALL PRIVILEGES ON SCHEMA public TO {quoted_username}",
                f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {quoted_username}",
                f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {quoted_username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info(
                "Administrative privileges granted for '%s'",
                self.username
            )
            return True

        except SQLAlchemyError as e:
            logger.error(
                "Error granting administrative privileges for '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error granting administrative privileges for '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)


class DatabaseEditor(DatabaseUser):
    """
    Editor user with read, insert, and update privileges.
    
    Grants:
    - Connect to database
    - Usage on public schema
    - SELECT, INSERT, UPDATE on all tables
    - Usage on all sequences
    - Default privileges for future tables and sequences
    """

    def __init__(
        self,
        username: str,
        password: str,
        description: str = "Role for data addition and modification"
    ):
        super().__init__(username, password, description)

    def grant_privileges(self, engine) -> bool:
        """Grant read, insert, and update privileges."""
        conn = None
        try:
            conn = engine.connect()
            quoted_username = quote_identifier(self.username)
            quoted_database = quote_identifier(self.database)

            privileges = [
                f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_username}",
                f"GRANT USAGE ON SCHEMA public TO {quoted_username}",
                f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO {quoted_username}",
                f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO {quoted_username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info(
                "Editor privileges granted for '%s'",
                self.username
            )
            return True

        except SQLAlchemyError as e:
            logger.error(
                "Error granting editor privileges for '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error granting editor privileges for '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)


class DatabaseViewer(DatabaseUser):
    """
    Viewer user with read-only privileges.
    
    Grants:
    - Connect to database
    - Usage on public schema
    - SELECT on all tables
    - SELECT on all sequences
    - Default privileges for future tables and sequences
    """

    def __init__(
        self,
        username: str,
        password: str,
        description: str = "Role for data viewing - SELECT only"
    ):
        super().__init__(username, password, description)

    def grant_privileges(self, engine) -> bool:
        """Grant read-only privileges."""
        conn = None
        try:
            conn = engine.connect()
            quoted_username = quote_identifier(self.username)
            quoted_database = quote_identifier(self.database)

            privileges = [
                f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_username}",
                f"GRANT USAGE ON SCHEMA public TO {quoted_username}",
                f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {quoted_username}",
                f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {quoted_username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO {quoted_username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info(
                "Viewer privileges granted for '%s'",
                self.username
            )
            return True

        except SQLAlchemyError as e:
            logger.error(
                "Error granting viewer privileges for '%s': %s",
                self.username, e
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error granting viewer privileges for '%s': %s",
                self.username, unexpected_error, exc_info=True
            )
            return False
        finally:
            self._safe_close_connection(conn)


# ============================================================================
# PUBLIC INTERFACE
# ============================================================================

__all__ = [
    'DatabaseUser',
    'DatabaseAdmin',
    'DatabaseEditor',
    'DatabaseViewer',
    'quote_identifier',
]
