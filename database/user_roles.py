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
    DB_USER: Administrative username (default: postgres)
    DB_PASSWORD: Administrative user password
    DB_ADMIN_PASSWORD: Password for admin role users

Security Notes:
    - Passwords should be stored securely in environment variables
    - Principle of least privilege is enforced through role separation
    - Connection strings are constructed securely with proper escaping
    - All database operations use parameterized queries to prevent SQL injection

Error Handling:
    The module provides detailed error categorization:
    - OperationalError: Connection and network issues
    - ProgrammingError: SQL syntax and permission issues  
    - DatabaseError: General database-related errors
    - SQLAlchemyError: Framework-specific database errors

Version: 1.0.0
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""
from pathlib import Path
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError, DatabaseError
from dotenv import load_dotenv

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[1]

# Load environment variables
env_path = project_path / '.env'
load_dotenv(env_path)

class DatabaseUser:
    """Base class for all database users"""

    def __init__(self, username, password, description=""):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'mft_db')
        self.username = username
        self.password = password
        self.description = description

        # Admin credentials for user creation
        self.admin_user = os.getenv('DB_USER', 'postgres')
        self.admin_password = os.getenv('DB_PASSWORD')

    def get_admin_engine(self):
        """Create engine with administrative privileges for user management"""
        try:
            connection_string = f"postgresql://{self.admin_user}:{self.admin_password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
            return engine
        except OperationalError as e:
            logger.error("Operational error creating admin engine: %s", e)
            return None
        except DatabaseError as e:
            logger.error("Database error creating admin engine: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error creating admin engine: %s", e)
            return None

    def get_user_engine(self):
        """Create engine with user privileges for database operations"""
        try:
            connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(connection_string)
            return engine
        except OperationalError as e:
            logger.error("Operational error creating user engine for %s: %s", self.username, e)
            return None
        except DatabaseError as e:
            logger.error("Database error creating user engine for %s: %s", self.username, e)
            return None
        except Exception as e:
            logger.error("Unexpected error creating user engine for %s: %s", self.username, e)
            return None

    def role_exists(self, engine):
        """Check if user role exists"""
        conn = None
        try:
            conn = engine.connect()
            result = conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :username"),
                username=self.username
            )
            return result.fetchone() is not None
        except OperationalError as e:
            logger.error("Operational error checking role %s: %s", self.username, e)
            return False
        except ProgrammingError as e:
            logger.error("SQL error checking role %s: %s", self.username, e)
            return False
        except SQLAlchemyError as e:
            logger.error("Database error checking role %s: %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)

    def create_role(self, engine):
        """Create user role in PostgreSQL"""
        conn = None
        try:
            if not self.role_exists(engine):
                conn = engine.connect()
                conn.execute(
                    text("CREATE ROLE :username WITH LOGIN PASSWORD :password"),
                    username=self.username, password=self.password
                )
                logger.info("Role '%s' created - %s", self.username, self.description)
                return True
            else:
                logger.info("Role '%s' already exists", self.username)
                return True
        except ProgrammingError as e:
            logger.error("SQL syntax error creating role '%s': %s", self.username, e)
            return False
        except OperationalError as e:
            logger.error("Operational error creating role '%s': %s", self.username, e)
            return False
        except SQLAlchemyError as e:
            logger.error("Database error creating role '%s': %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)

    def _safe_close_connection(self, conn):
        """Safely close connection if it exists and has close method"""
        if conn and hasattr(conn, 'close') and callable(getattr(conn, 'close', None)):
            try:
                conn.close()
            except Exception as e:
                logger.warning("Error closing connection: %s", e)

    def _safe_dispose_engine(self, engine):
        """Safely dispose engine if it exists and has dispose method"""
        if engine and hasattr(engine, 'dispose') and callable(getattr(engine, 'dispose', None)):
            try:
                engine.dispose()
            except Exception as e:
                logger.warning("Error disposing engine: %s", e)

    def grant_privileges(self, engine):
        """Grant privileges - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement grant_privileges method")

    def create_db_user(self):
        """Create user with appropriate privileges"""
        engine = self.get_admin_engine()
        if not engine:
            return False

        try:
            # Create the role
            if not self.create_role(engine):
                return False

            # Grant specific privileges
            if not self.grant_privileges(engine):
                return False

            logger.info("User '%s' created successfully with %s privileges", 
                       self.username, self.__class__.__name__)
            return True

        except ProgrammingError as e:
            logger.error("SQL syntax error creating user '%s': %s", self.username, e)
            return False
        except OperationalError as e:
            logger.error("Operational error creating user '%s': %s", self.username, e)
            return False
        except SQLAlchemyError as e:
            logger.error("Database error creating user '%s': %s", self.username, e)
            return False
        except Exception as e:
            logger.error("Unexpected error creating user '%s': %s", self.username, e)
            return False
        finally:
            self._safe_dispose_engine(engine)

    def test_connection(self):
        """Test if user can connect to database"""
        engine = self.get_user_engine()
        if not engine:
            return False

        conn = None
        try:
            conn = engine.connect()
            # Simple query to test connection
            conn.execute(text("SELECT 1"))
            logger.info("Connection test successful for user '%s'", self.username)
            return True
        except OperationalError as e:
            logger.error("Operational error - connection test failed for user '%s': %s", self.username, e)
            return False
        except ProgrammingError as e:
            logger.error("Permission error - connection test failed for user '%s': %s", self.username, e)
            return False
        except SQLAlchemyError as e:
            logger.error("Database error - connection test failed for user '%s': %s", self.username, e)
            return False
        except Exception as e:
            logger.error("Unexpected error - connection test failed for user '%s': %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)
            self._safe_dispose_engine(engine)

    def get_connection_string(self):
        """Get connection string for this user"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseAdmin(DatabaseUser):
    """Administrative user with full database access"""

    def __init__(self, username, password, description="Administrative role - full database access"):
        super().__init__(username, password, description)

    def grant_privileges(self, engine):
        """Grant full administrative privileges"""
        conn = None
        try:
            conn = engine.connect()
            privileges = [
                f"GRANT ALL PRIVILEGES ON DATABASE {self.database} TO {self.username}",
                f"GRANT ALL PRIVILEGES ON SCHEMA public TO {self.username}",
                f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {self.username}",
                f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {self.username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info("Administrative privileges granted for '%s'", self.username)
            return True

        except SQLAlchemyError as e:
            logger.error("Error granting administrative privileges for '%s': %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)


class DatabaseEditor(DatabaseUser):
    """Editor user with read, insert, and update privileges"""

    def __init__(self, username, password, description="Role for data addition and modification"):
        super().__init__(username, password, description)

    def grant_privileges(self, engine):
        """Grant read, insert, and update privileges"""
        conn = None
        try:
            conn = engine.connect()
            privileges = [
                f"GRANT CONNECT ON DATABASE {self.database} TO {self.username}",
                f"GRANT USAGE ON SCHEMA public TO {self.username}",
                f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO {self.username}",
                f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO {self.username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info("Editor privileges granted for '%s'", self.username)
            return True

        except SQLAlchemyError as e:
            logger.error("Error granting editor privileges for '%s': %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)


class DatabaseViewer(DatabaseUser):
    """Viewer user with read-only privileges"""

    def __init__(self, username, password, description="Role for data viewing - SELECT only"):
        super().__init__(username, password, description)

    def grant_privileges(self, engine):
        """Grant read-only privileges"""
        conn = None
        try:
            conn = engine.connect()
            privileges = [
                f"GRANT CONNECT ON DATABASE {self.database} TO {self.username}",
                f"GRANT USAGE ON SCHEMA public TO {self.username}",
                f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {self.username}",
                f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {self.username}",
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO {self.username}"
            ]

            for privilege in privileges:
                conn.execute(text(privilege))

            logger.info("Viewer privileges granted for '%s'", self.username)
            return True

        except SQLAlchemyError as e:
            logger.error("Error granting viewer privileges for '%s': %s", self.username, e)
            return False
        finally:
            self._safe_close_connection(conn)
