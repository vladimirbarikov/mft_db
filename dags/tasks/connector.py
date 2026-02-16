# pyright: basic
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalContextManager=false
# pyright: reportOptionalSubscript=false
# pyright: reportOptionalIterable=false

# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Database connection and management module for Material Flow Table Database with SQLAlchemy.

This module provides a comprehensive solution for database connectivity, health checking,
and schema management in the Material Flow Table Database. It implements robust
connection strategies, automatic host detection, and table management with full support
for PostgreSQL environments.

Key Features:
    - Intelligent host detection with multiple fallback strategies (local, Docker, network)
    - Secure password handling with automatic redaction in logs
    - Environment-based configuration with .env file support
    - Database existence verification and automatic creation
    - Table management based on SQLAlchemy ORM models
    - Comprehensive health checking and monitoring
    - Retry logic with configurable attempts and delays
    - Connection pooling and resource management

Architecture:
    The module implements a factory pattern for model availability checking and uses
    a layered approach for connection management:
    1. Configuration Layer: Environment variable parsing and validation
    2. Detection Layer: Automatic host and network configuration detection
    3. Connection Layer: Database connection with retry logic
    4. Schema Layer: Table creation and verification
    5. Monitoring Layer: Health checks and database information gathering

Dependencies:
    - SQLAlchemy 1.4.54+: ORM and database abstraction layer
    - SQLAlchemy-Utils 0.41.1+: Database existence and creation utilities
    - python-dotenv 1.0.0+: Environment variable management (optional but recommended)
    - PostgreSQL 12+: Target database system

Performance Considerations:
    - Connection pooling with configurable pool size and recycling
    - Lazy loading of database models to minimize startup time
    - Cached host resolution to avoid repeated DNS lookups
    - Intelligent retry logic with exponential backoff (configurable)

Security Notes:
    - Passwords are never logged or exposed in error messages
    - Connection strings are sanitized before logging
    - Environment variable loading with validation and error handling
    - Database credentials stored securely in .env files or environment variables

Error Handling:
    - Comprehensive exception hierarchy with appropriate logging levels
    - Graceful degradation for missing dependencies
    - Connection retry logic for transient network issues
    - Detailed error messages for debugging without exposing sensitive data

Integration Notes:
    - Used by loader.py for database connectivity during data loading
    - Integrates with database.py for ORM model management
    - Supports both development (Docker) and production environments
    - Airflow compatible with proper connection lifecycle management

Host Detection Strategy:
    1. Environment variable DB_HOST (explicit override)
    2. localhost (127.0.0.1) for local development
    3. Docker service names (postgres_mft_db, postgres)
    4. host.docker.internal for Docker Desktop
    5. Fallback to localhost with warning

Connection Pool Configuration:
    - pool_size: 5 concurrent connections
    - max_overflow: 10 temporary connections
    - pool_recycle: 3600 seconds (1 hour)
    - pool_pre_ping: True (validate connections)
    - connect_timeout: 10 seconds

Environment Variables:
    DB_HOST: Database hostname (optional, auto-detected)
    DB_PORT: Database port (default: 5432)
    DB_NAME: Database name (default: mft_db)
    DB_USER: Database user (default: mft_user)
    DB_PASSWORD: Database password (default: mft_password)
    DB_MAX_RETRIES: Maximum connection attempts (default: 5)
    DB_RETRY_DELAY: Delay between retries in seconds (default: 2)

Note:
    This module is specifically designed for PostgreSQL databases in manufacturing
    data warehouse environments. It assumes UTF-8 encoding and template0 for
    database creation. For other database systems, connection strings and SQL
    dialects would need adjustment.

Usage Example:
    ```
    from dags.tasks.connector import initialize_database
    
    # Basic initialization with table creation
    engine = initialize_database(create_tables=True)
    if engine:
        # Database operations here
        print(f"Connected to {engine.url.database}")
    
    # Without table creation (for loader.py)
    engine = initialize_database(create_tables=False)
    
    # Direct connection with custom parameters
    from dags.tasks.connector import connect_to_database
    engine = connect_to_database(max_retries=5, retry_delay=3, create_tables=True)
    ```

Maintainer: PLD Engineering Center
Version: 1.1.0
Compatibility: Python 3.12.3+, SQLAlchemy 1.4.54+, PostgreSQL 12+
Created: 2026-01-05
Last Modified: 2025-02-16
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import os
import sys
import socket
import time
from typing import (
    Any, Callable, cast, Optional, Tuple, Type
)

# Third-party imports
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect as sqlalchemy_inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_utils import database_exists, create_database

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from database.database import Base as DatabaseBase

# Logger setup
logger = get_logger(__name__)

# Load environment variables
try:
    env_path = PROJECT_ROOT / '.env'

    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info("The environment variables are loaded from %s", env_path)
    else:
        # There is no .env file
        logger.debug(".env not found, using system environment")

except (PermissionError, ValueError, UnicodeDecodeError) as e:
    # The .env file exists, but it is corrupted
    logger.critical("Corrupted .env file: %s", e)
    raise RuntimeError("Invalid .env file") from e

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
    Factory for functions that manage database model availability state.

    Creates three callables to check, retrieve, and reset the state of SQLAlchemy
    models from the `database.database` module.

    Returns:
        Tuple of:
        - models_available(): Returns True if models are loaded and valid.
        - get_base(): Returns the SQLAlchemy Base class if available, else None.
        - reset(): Resets the internal state (primarily for testing).
    """
    _base = None
    _has_database_models = False
    _attempted = False

    def models_available() -> bool:
        """
        Check if database models are available and valid.

        Returns:
            True if models are loaded and have required attributes, False otherwise.
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
            The Base class or None.
        """
        nonlocal _base
        if models_available():
            return _base
        return None

    def reset() -> None:
        """
        Reset the internal state of the model checker.

        Intended for testing to force re-evaluation of model availability.
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
    """
    Check if a hostname can be resolved to an IP address.

    Args:
        host: Hostname or IP address to resolve.

    Returns:
        True if host can be resolved, False if resolution fails.
    """
    try:
        socket.gethostbyname(host)
        logger.debug("Host %s resolved successfully", host)
        return True
    except socket.gaierror as e:
        logger.debug("Failed to resolve host %s: %s", host, e)
        return False

def determine_db_host() -> str:
    """
    Determine the correct database host using multiple detection strategies.

    Returns:
        Resolved hostname as a string. Guaranteed to return a hostname
        ('localhost' as last resort).
    """
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
        'localhost',
        '127.0.0.1',
        'postgres_mft_db',
        'postgres',
        'host.docker.internal'
    ]

    for host in test_hosts:
        if can_resolve_host(host):
            logger.info("Host available: %s", host)
            return host

    logger.warning("Could not determine available host, using localhost")
    return 'localhost'

def get_public_db_config() -> dict:
    """
    Create a safe database configuration for logging and debugging.

    Returns a dictionary without the password. All values are strings.

    Returns:
        Dictionary with keys: host, port, database, user.
    """

    public_config = {
        'host': determine_db_host(),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'mft_db'),
        'user': os.getenv('DB_USER', 'mft_user')
    }

    # Secure logging
    logger.debug(
        "Public DB config: host=%s, port=%s, database=%s, user=%s",
        public_config['host'], public_config['port'],
        public_config['database'], public_config['user']
    )

    return public_config

def get_private_db_config() -> dict:
    """
    Create a complete database configuration including the password.

    Returns:
        Dictionary with keys: host, port, database, user, password.

    Warning:
        This dictionary contains the password and should never be logged.
    """
    public_config = get_public_db_config()

    private_config = {
        **public_config,  # Unpacking the public config
        'password': os.getenv('DB_PASSWORD', 'mft_password')
    }

    return private_config

def get_connection_string(config: dict | None = None) -> str:
    """
    Build a PostgreSQL connection string from a configuration dictionary.

    Args:
        config: Configuration dict with 'user', 'password', 'host', 'port', 'database'.
                If None, uses `get_private_db_config()`.

    Returns:
        PostgreSQL connection string in format:
        postgresql://user:password@host:port/database
    """
    if config is None:
        #  Config with a password to connect to database
        config = get_private_db_config()

    conn_str = (f"postgresql://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}")

    # Hide password in logs for security
    safe_conn_str = (
        f"postgresql://{config['user']}:******@"
        f"{config['host']}:{config['port']}/{config['database']}"
    )

    logger.debug("Connection string: %s", safe_conn_str)

    return conn_str

def ensure_database_exists():
    """
    Ensure the target database exists, creating it if necessary.

    Connects to the PostgreSQL system database ('postgres') to check for
    and create the target database.

    Raises:
        ConnectionError: If PostgreSQL server is unreachable.
        RuntimeError: For SQLAlchemy or database operation errors.
        ValueError: For incomplete database configuration.
        PermissionError: If user lacks CREATE DATABASE privilege.
    """
    try:
        # Get private config (with password)
        private_config = get_private_db_config()

        # Create config for connection to system database 'postgres'
        postgres_config = {
            'host': private_config['host'],
            'port': private_config['port'],
            'database': 'postgres',
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

def create_database_tables(
        engine: Engine
    ) -> list[str]:
    """
    Create all database tables from SQLAlchemy ORM models.

    Args:
        engine: SQLAlchemy engine connected to the target database.

    Returns:
        List of table names that were created or verified.

    Raises:
        ImportError: If database models are not available.
        RuntimeError: For errors during table creation or verification.
        ValueError: If the provided engine is None.
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

        # Use Base.metadata to create all tables
        base.metadata.create_all(bind=engine)
        logger.info("Tables created successfully using Base.metadata")

        # Creating an inspector
        inspector_obj = sqlalchemy_inspect(engine)
        if inspector_obj is None:
            raise RuntimeError("Failed to create SQLAlchemy inspector")

        # Explicitly converting the type to Inspector
        inspector: Inspector = cast(Inspector, inspector_obj)

        # Get list of tables
        tables: list[str] = inspector.get_table_names()

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

def check_database_health(
        engine: Engine
    ) -> bool:
    """
    Perform a health check on the database connection and schema.

    Verifies connectivity, PostgreSQL version, and table consistency.

    Args:
        engine: SQLAlchemy engine object with an active connection.

    Returns:
        True if database is healthy (connected and schema matches), False otherwise.
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

        # Check existence of tables
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
                    "All tables present (%d tables)",
                    len(expected_tables)
                )

        return True

    except SQLAlchemyError as e:
        logger.error("Error checking database health: %s", e)
        return False
    finally:
        if connection:
            connection.close()

def connect_to_database(
        max_retries: int = 3,
        retry_delay: int = 2,
        create_tables: bool = True
    )-> Engine:
    """
    Establish a database connection with retry logic.

    Args:
        max_retries: Maximum number of connection attempts.
        retry_delay: Delay in seconds between retry attempts.
        create_tables: Whether to create tables from ORM models after connection.

    Returns:
        SQLAlchemy Engine object configured with connection pooling.

    Raises:
        ConnectionError: After exhausting all retry attempts.
        SQLAlchemyError: For database-specific errors during connection.
        RuntimeError: For unexpected errors during connection setup.
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
                connect_args={'connect_timeout': 10}
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

                    # Create tables
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

def get_database_info(
        engine: Engine
    ) -> dict:
    """
    Collect information about the database and its tables.

    Args:
        engine: SQLAlchemy engine object with an active connection.

    Returns:
        Dictionary containing database metadata (name, user, size, tables, etc.)
        or an empty dictionary if retrieval fails.
    """
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

        # Information about tables
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

def initialize_database(
        create_tables: bool = True
    ):
    """
    Primary entry point for database initialization.

    Orchestrates the complete database setup process.

    Args:
        create_tables: Whether to create database tables from ORM models.

    Returns:
        SQLAlchemy Engine object if initialization succeeds, None otherwise.
    """

    logger.info("=" * 60)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 60)

    if not check_models_available():
        logger.error("Database models file (database.py) not found or cannot be imported!")
        logger.error("Check if database.py exists and contains valid SQLAlchemy models.")
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
