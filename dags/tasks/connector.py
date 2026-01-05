"""
Database connection and management module for PostgreSQL with SQLAlchemy.

This module provides a comprehensive solution for:
1. Database connection management with retry logic
2. Automatic host detection (local, Docker, network)
3. Environment-based configuration with .env file support
4. Database existence verification and creation
5. Table management based on SQLAlchemy models
6. Health checking and monitoring

Key Features:
- Lazy loading and caching of database models
- Secure password handling (never logged)
- Multiple connection strategies with fallback
- Comprehensive error handling and logging
- Support for both local and Docker environments

Architecture:
The module follows a factory pattern for model checking and uses
dependency injection for database engine management.

Usage Example:
    >>> from connector import initialize_database
    >>> engine = initialize_database()
    >>> if engine:
    ...     # Use engine for database operations
    ...     pass

Environment Variables:
    DB_HOST: Database hostname (optional, auto-detected)
    DB_PORT: Database port (default: 5432)
    DB_NAME: Database name (default: mft_db)
    DB_USER: Database user (default: postgres)
    DB_PASSWORD: Database password (default: postgres)
    DB_MAX_RETRIES: Maximum connection attempts (default: 5)
    DB_RETRY_DELAY: Delay between retries in seconds (default: 2)

Dependencies:
    - SQLAlchemy: ORM and database abstraction
    - SQLAlchemy-Utils: Database existence utilities
    - python-dotenv: Environment variable management (optional)

Logging:
    Configured with INFO level by default. Sensitive information
    (passwords) is automatically redacted from logs.

Error Handling:
    All functions include comprehensive error handling with
    appropriate logging and exception propagation.

Thread Safety:
    Functions are generally thread-safe for read operations.
    Write operations may require external synchronization.

Note:
    This module is designed for PostgreSQL but can be adapted
    for other databases by modifying connection strings and
    SQL dialect-specific features.

Version: 1.0.0
Compatibility: Python 3.12.3, SQLAlchemy 1.4.54, PostgreSQL 12+
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production
"""

# pyright: basic
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalContextManager=false
# pyright: reportOptionalSubscript=false
# pyright: reportOptionalIterable=false

from pathlib import Path
import os
import sys
import socket
import time

from typing import List, cast, Optional, Any, Tuple, Callable, Type

from sqlalchemy import create_engine, inspect as sqlalchemy_inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_utils import database_exists, create_database

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

# Import logger configuration
from config import get_logger

# Import SQLAlchemy ORM base class to create data models
from database.database import Base as DatabaseBase


# Logger setup
logger = get_logger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    project_path = Path(__file__).resolve().parents[2]
    env_path = project_path / '.env'

    if env_path.exists():
        if env_path.is_file():
            try:
                load_dotenv(dotenv_path=env_path, override=True)
                logger.info("Loaded environment from %s", env_path)

            except (PermissionError, ValueError, UnicodeDecodeError) as e:
                # The .env file exists, but it is corrupted
                logger.critical("Corrupted .env file: %s", e)
                raise RuntimeError("Invalid .env file") from e
        else:
            # The path exists, but it is not an .env file
            logger.critical("%s is not a file", env_path)
            raise RuntimeError(f"Expected file, got directory: {env_path}")
    else:
        # There is no .env file
        logger.debug(".env not found, using system environment")

except ImportError:
    # python-dotenv is not installed
    logger.info("python-dotenv not installed")
except Exception as e:
    # Unexpected error
    logger.critical("Unexpected error: %s", e, exc_info=True)
    raise RuntimeError("Environment setup failed") from e

def make_models_checker()-> Tuple[
    Callable[[], bool],
    Callable[[], Optional[Type[Any]]],
    Callable[[], None]
]:
    """
    Factory function that creates and manages database model availability checks.

    Returns a tuple of three callable functions:
    1. models_available(): Check if database models are accessible and valid
    2. get_base(): Get the SQLAlchemy Base class if available
    3. reset(): Reset the internal state (primarily for testing)
    """
    _base = None
    _has_database_models = False
    _attempted = False

    def models_available() -> bool:
        """
        Check if database models are available and properly structured.
        
        Performs lazy initialization on first call:
        1. Attempts to import DatabaseBase from database.database
        2. Validates that Base has 'metadata' attribute
        3. Caches the result for subsequent calls
        
        Returns:
            True if models are available and valid, False otherwise.
            
        Raises:
            No exceptions are raised; all errors are logged and handled gracefully.
        """
        nonlocal _base, _has_database_models, _attempted

        if _has_database_models:
            return True

        if _attempted:
            return False

        _attempted = True

        try:
            _base = DatabaseBase

            if hasattr(_base, 'metadata'):
                _has_database_models = True
                logger.info("Database models imported successfully")
                return True
            else:
                logger.warning("Base imported but has no 'metadata' attribute")
                _base = None
                return False

        except NameError as e:
            # DatabaseBase is not defined
            logger.warning("Database models not found: %s", e)
            return False
        except AttributeError as e:
            # DatabaseBase is defined, but the structure is incomplete
            logger.warning("Database models incomplete: %s", e)
            _base = None
            return False
        except Exception as e:
            logger.error("Unexpected error importing database models: %s", e)
            return False

    def get_base()-> Optional[Type[Any]]:
        """
        Retrieve the SQLAlchemy Base class if models are available.
        
        Returns:
            The Base class if models_available() returns True, 
            otherwise returns None.
            
        Note:
            This function depends on models_available() for validation.
            Call models_available() first if you need to check availability.
        """
        nonlocal _base
        if models_available():
            return _base
        return None

    def reset() -> None:
        """
        Reset the internal state of the model checker.
        
        Primarily intended for testing purposes to force re-evaluation
        of model availability. Clears all cached state.
        
        After calling reset(), the next call to models_available() will
        attempt to import and validate models again.
        
        Warning:
            Do not use in production unless absolutely necessary, as it
            will cause redundant import operations.
        """
        nonlocal _base, _has_database_models, _attempted
        _base = None
        _has_database_models = False
        _attempted = False
        logger.debug("Database models state reset")

    return models_available, get_base, reset

# Creating functions
check_models_available, get_base_model, reset_models = make_models_checker()

def can_resolve_host(host: str) -> bool:
    """Check if hostname can be resolved"""
    try:
        socket.gethostbyname(host)
        logger.debug("Host %s resolved successfully", host)
        return True
    except socket.gaierror as e:
        logger.debug("Failed to resolve host %s: %s", host, e)
        return False

def determine_db_host() -> str:
    """Determine correct host for database connection"""
    # Option 1: Explicit host from environment variable
    explicit_host = os.getenv('DB_HOST')
    if explicit_host:
        # Check the availability of the host before returning
        if can_resolve_host(explicit_host):
            logger.info("Using host from DB_HOST: %s", explicit_host)
            return explicit_host
        else:
            logger.warning(
                "Host from DB_HOST (%s) is not available. Switching to automatic detection.",
                explicit_host
            )

    # Option 2: Automatic detection
    logger.info("Automatic database host detection...")

    test_hosts = [
        'localhost',            # For local execution
        '127.0.0.1',            # Localhost alternative
        'postgres_mft_db',      # Docker service name
        'postgres',             # Short service name
        'host.docker.internal'  # For Docker Desktop
    ]

    for host in test_hosts:
        if can_resolve_host(host):
            logger.info("Host available: %s", host)
            return host

    logger.warning("Could not determine available host, using localhost")
    return 'localhost'

def get_public_db_config() -> dict:
    """Config without password for logging and debugging only"""

    public_config = {
        'host': determine_db_host(),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'mft_db'),
        'user': os.getenv('DB_USER', 'postgres')
    }

    # Secure logging
    logger.debug(
        "Public DB config: host=%s, port=%s, database=%s, user=%s",
        public_config['host'], public_config['port'],
        public_config['database'], public_config['user']
    )

    return public_config

def get_private_db_config() -> dict:
    """Config with password to establish a database connection"""

    public_config = get_public_db_config()

    private_config = {
        **public_config,  # Unpacking the public config
        'password': os.getenv('DB_PASSWORD', 'postgres')
    }

    return private_config

def get_connection_string(config: dict | None = None) -> str:
    """Return connection string"""
    if config is None:
        #  Config with a password to connect to database
        config = get_private_db_config()

    conn_str = (f"postgresql://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}")

    # Hide password in logs for security
    safe_conn_str = conn_str.replace(config['password'], '******')
    logger.debug("Connection string: %s", safe_conn_str)

    return conn_str

def ensure_database_exists():
    """Ensure database exists, create if it doesn't"""
    try:
        # Get private config (with password)
        private_config = get_private_db_config()

        # Create config for connection to system database 'postgres'
        postgres_config = {
            'host': private_config['host'],
            'port': private_config['port'],
            'database': 'postgres',                 # System database!
            'user': private_config['user'],
            'password': private_config['password']
        }

        # Connect to 'postgres'
        postgres_url = get_connection_string(postgres_config)
        postgres_engine = create_engine(postgres_url)

        # Create URL for target database (for checking)
        target_db_url = get_connection_string(private_config)

        # Check if target database exists
        target_database = private_config['database']

        if not database_exists(target_db_url):
            logger.warning("Database '%s' does not exist. Creating...", target_database)

            # Create database via connection to 'postgres'
            create_database(target_db_url, encoding='utf8', template='template0')
            logger.info("Database '%s' created successfully", target_database)
        else:
            logger.info("Database '%s' already exists", target_database)

        # Close connection
        postgres_engine.dispose()

    except OperationalError as e:
        # Connection errors: host unreachable, authentication failed, etc.
        logger.error("Database connection error: %s", e)
        raise ConnectionError(f"Failed to connect to PostgreSQL server: {e}") from e

    except SQLAlchemyError as e:
        # SQLAlchemy specific errors
        logger.error("SQLAlchemy error during database check: %s", e)
        raise RuntimeError(f"Database operation failed: {e}") from e

    except socket.gaierror as e:
        # Host resolution errors
        logger.error("Host resolution error: %s", e)
        raise ConnectionError(f"Cannot resolve database host: {e}") from e

    except KeyError as e:
        # Missing configuration keys
        logger.error("Missing configuration key: %s", e)
        raise ValueError(f"Database configuration incomplete. Missing key: {e}") from e

    except PermissionError as e:
        # Permission errors (e.g., cannot create database)
        logger.error("Permission denied: %s", e)
        raise PermissionError(f"Insufficient permissions to create database: {e}") from e

    except Exception as e:
        # Catch-all for unexpected errors
        logger.critical("Unexpected error ensuring database exists: %s", e, exc_info=True)
        raise RuntimeError(f"Unexpected error during database setup: {e}") from e

def create_database_tables(engine: Engine) -> List[str]:
    """
    Create all tables from models in database.py
    Verify and get information about database tables

    Args:
        engine: SQLAlchemy engine object

    Returns:
        List[str]: List of created tables from models in database.py
    """
    if not check_models_available():
        logger.error("Cannot create tables: database models not available")
        raise ImportError(
            "Database models not available or incomplete. Please check database.py file."
        )

    try:
        # Validate engine
        if engine is None:
            raise ValueError("Database engine is None")

        # Getting the Base via get_base()
        base = get_base_model()

        # Safety check: check_models_available() guarantees Base is not None
        if base is None:
            raise RuntimeError("Base is None despite models being available")

        # Use Base.metadata from database.py to create all tables
        base.metadata.create_all(bind=engine)
        logger.info("Tables created successfully using Base.metadata")

        # Creating an inspector
        inspector_obj = sqlalchemy_inspect(engine)
        if inspector_obj is None:
            raise RuntimeError("Failed to create SQLAlchemy inspector")

        # Explicitly converting the type to Inspector
        inspector: Inspector = cast(Inspector, inspector_obj)

        # Get list of tables
        tables: List[str] = inspector.get_table_names()

        logger.info("Found %d tables in database", len(tables))
        logger.info("Table list: %s", ', '.join(sorted(tables)))

        # Log table structure
        for table_name in tables:
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            logger.debug("Table '%s': %s", table_name, ', '.join(column_names))

        return tables

    except AttributeError as e:
        logger.error("Attribute error: %s", e, exc_info=True)
        raise RuntimeError(f"Database models structure error: {e}") from e
    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error creating tables: %s", e, exc_info=True)
        raise RuntimeError(f"SQLAlchemy error during table creation: {e}") from e
    except Exception as e:
        logger.error("Unexpected error during table verification: %s", e, exc_info=True)
        raise RuntimeError(f"Failed to verify database tables: {e}") from e

def check_database_health(engine: Engine) -> bool:
    """
    Check database health and existence of tables from database.py
    
    Returns:
        bool: True if database is healthy, False if issues
    """
    connection = None
    try:
        connection = engine.connect()

        # Check PostgreSQL version
        result = connection.execute(text("SELECT version()"))
        version = result.scalar()

        if version is None:
            logger.warning("Could not retrieve PostgreSQL version")
            return False

        logger.info("PostgreSQL version: %s", version)

        # Check existence of tables from database.py
        if check_models_available():
            base = get_base_model()

            # Creating an inspector
            inspector_obj = sqlalchemy_inspect(engine)
            if inspector_obj is None:
                raise RuntimeError("Failed to create SQLAlchemy inspector")

            # Explicitly converting the type to Inspector
            inspector: Inspector = cast(Inspector, inspector_obj)
            existing_tables = set(inspector.get_table_names())

            # Get expected tables from models
            expected_tables = set(base.metadata.tables.keys())

            # Check if all tables are created
            missing_tables = expected_tables - existing_tables
            if missing_tables:
                logger.warning("Missing tables: %s", missing_tables)
                return False
            else:
                logger.info(
                    "All tables from database.py present (%d tables)",
                    len(expected_tables)
                )

        return True

    except SQLAlchemyError as e:
        logger.error("Error checking database health: %s", e)
        return False
    finally:
        if connection:
            connection.close()

def connect_to_database(max_retries: int = 3, retry_delay: int = 2, create_tables: bool = True)-> Engine:
    """
    Connect to database with retry logic and table creation from database.py
    
    Args:
        max_retries (int): Maximum number of connection attempts
        retry_delay (int): Delay between attempts in seconds
        create_tables (bool): Whether to create tables from database.py
    
    Returns:
        engine: SQLAlchemy engine object
    """
    # For logging we use a public config without a password
    public_config = get_public_db_config()

    # For connecting we use a private config with a password
    private_config = get_private_db_config()

    logger.info(
        "Connecting to PostgreSQL: %s:%s/%s",
        public_config['host'], public_config['port'], public_config['database']
    )
    logger.info(
        "Parameters: attempts=%d, delay=%ds, create_tables=%s",
        max_retries, retry_delay, create_tables
    )

    for attempt in range(max_retries):
        try:
            logger.info("Connection attempt %d/%d", attempt + 1, max_retries)

            # Ensure database exists
            ensure_database_exists()

            # Create main engine for our database
            connection_string = get_connection_string(private_config)
            engine = create_engine(
                connection_string,
                echo=False,                           # Set to True for SQL query debugging
                pool_pre_ping=True,                   # Check connection before use
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,                    # Recreate connections every hour
                connect_args={'connect_timeout': 10}  # Connection timeout
            )

            # Test connection
            connection = None
            try:
                connection = engine.connect()
                result = connection.execute(text("SELECT 1"))
                if result.scalar() == 1:
                    logger.info(
                        "Database connection successful (attempt %d/%d)",
                        attempt + 1, max_retries
                    )

                    # Create tables from database.py
                    if create_tables and check_models_available():
                        logger.info("Creating tables from models in database.py...")
                        created_tables = create_database_tables(engine)
                        logger.info("Created %d tables", len(created_tables))

                    # Check database health
                    if check_database_health(engine):
                        logger.info("Database health check passed")
                        return engine
            finally:
                if connection:
                    connection.close()

        except OperationalError as e:
            logger.warning("Attempt %d/%d failed: %s", attempt + 1, max_retries, e)

            if attempt < max_retries - 1:
                logger.info("Retrying in %d seconds...", retry_delay)
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to database after all attempts")
                raise ConnectionError(
                    f"Failed to connect to database after {max_retries} attempts"
                ) from e

        except SQLAlchemyError as e:
            logger.error("SQLAlchemy connection error: %s", e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    raise ConnectionError("Failed to connect to database")

def get_database_info(engine: Engine) -> dict:
    """Get information about database and tables"""
    info = {}
    connection = None

    try:
        connection = engine.connect()

        # Basic database information
        result = connection.execute(text("""
            SELECT 
                current_database() as db_name,
                current_user as db_user,
                inet_server_addr() as server_ip,
                inet_server_port() as server_port,
                pg_database_size(current_database()) as db_size_bytes
        """))
        row = result.fetchone()

        if row:
            info['db_name'] = row[0] if row[0] is not None else 'unknown'
            info['db_user'] = row[1] if row[1] is not None else 'unknown'
            info['server_ip'] = str(row[2]) if row[2] else None
            info['server_port'] = row[3] if row[3] else 0
            info['db_size_mb'] = round(cast(int, row[4]) / (1024 * 1024), 2) if row[4] else 0

        # Information about tables from database.py
        if check_models_available():
            # Creating an inspector
            inspector_obj = sqlalchemy_inspect(engine)
            if inspector_obj is None:
                raise RuntimeError("Failed to create SQLAlchemy inspector")

            # Explicitly converting the type to Inspector
            inspector: Inspector = cast(Inspector, inspector_obj)
            tables = inspector.get_table_names()
            info['tables_count'] = len(tables)
            info['tables'] = sorted(tables)

            # Count records in each table
            table_counts = {}
            for table in tables:
                try:
                    count_result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    table_counts[table] = count_result.scalar() or 0
                except Exception:
                    table_counts[table] = 0

            info['table_record_counts'] = table_counts

        logger.info(
            "Database info: %s (%d tables, %s MB)",
            info.get('db_name', 'unknown'),
            info.get('tables_count', 0),
            info.get('db_size_mb', 0)
        )
        return info

    except SQLAlchemyError as e:
        logger.error("Error getting database information: %s", e)
        return {}
    finally:
        if connection:
            connection.close()

# Main function for database initialization
def initialize_database(create_tables: bool = True):
    """
    Main database initialization function
    
    Args:
        create_tables (bool): Whether to create tables from database.py
    
    Returns:
        engine: SQLAlchemy engine object or None on error
    """

    logger.info("=" * 60)
    logger.info("DATABASE INITIALIZATION FROM DATABASE.PY")
    logger.info("=" * 60)

    if not check_models_available():
        logger.error("database.py file with models not found!")
        logger.error("Make sure database.py is in the same directory.")
        return None

    try:
        # Connect to database and create tables
        engine = connect_to_database(
            max_retries=int(os.getenv('DB_MAX_RETRIES', '5')),
            retry_delay=int(os.getenv('DB_RETRY_DELAY', '2')),
            create_tables=create_tables
        )

        # Get database information
        db_info = get_database_info(engine)
        if db_info:
            logger.info("Database '%s' ready for use", db_info.get('db_name'))
            logger.info("Tables: %d", db_info.get('tables_count', 0))
            logger.info("Size: %s MB", db_info.get('db_size_mb', 0))

            # Log record counts in tables
            if 'table_record_counts' in db_info:
                logger.info("Record counts in tables:")
                for table, count in db_info['table_record_counts'].items():
                    logger.info("     - %s: %d records", table, count)

        logger.info("=" * 60)
        logger.info("INITIALIZATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

        return engine

    except ConnectionError as e:
        logger.error("CRITICAL CONNECTION ERROR: %s", e)
        return None
    except Exception as e:
        logger.error("UNEXPECTED ERROR: %s", e, exc_info=True)
        return None
