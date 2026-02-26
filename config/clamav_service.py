# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Service for checking files via ClamAV antivirus before saving to Airflow data folder.

This module provides:
- Connection to ClamAV container running in Docker
- Memory-based virus scanning of uploaded files
- Automatic cleanup of temporary files
- Monitoring and management endpoints for administrators

The scanner is used by the upload API to validate files before they are
saved to the Airflow data directory and processed by ETL DAGs.

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-25
Last Modified: 2026-02-25
License: MIT
Status: Production
"""

# Standard library imports
import os
import sys
import tempfile
import threading
import shutil
from socket import gaierror, timeout
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple

# Third-party imports
import clamd
from dotenv import load_dotenv

# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    # If __file__ is not defined (in exec() or interactive mode)
    PROJECT_ROOT = Path("/opt/airflow")

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)

# Load environment variables
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

class ClamAVScanner:
    """
    Scanner for virus checking files using ClamAV antivirus.
    
    This class provides integration with ClamAV running in a Docker container.
    It is specifically designed to scan files uploaded via the API before they
    are saved to the Airflow data directory.
    
    The scanner operates in memory (scan_stream method) to avoid writing
    potentially malicious files to disk before verification.
    
    Attributes:
        host: ClamAV server hostname
        port: ClamAV server port
        available: Whether ClamAV is currently accessible
        temp_dir: Directory for temporary files (if needed by ClamAV)
    
    Usage example:
        from config.clamav_service import clamav_scanner
    
        if clamav_scanner.is_available():
            is_clean, result = clamav_scanner.scan_stream(file_data, "file.xlsx")
            if is_clean:
                # Save file to Airflow data directory
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        max_file_size_mb: Optional[int] = None,
        temp_retention_hours: Optional[int] = None,
        enable_auto_cleanup: bool = True
    ):
        """
        Initialize the ClamAV scanner with configuration from environment variables.
        
        Args:
            host: ClamAV server host (overrides CLAMAV_HOST from .env)
            port: ClamAV server port (overrides CLAMAV_PORT from .env)
            max_file_size_mb: Maximum file size to scan (overrides MAX_FILE_SIZE_MB from .env)
            temp_retention_hours: How long to keep temporary files (overrides TEMP_RETENTION_HOURS)
            enable_auto_cleanup: Whether to start background cleanup thread
        
        Environment variables (with defaults):
            CLAMAV_HOST: 'clamav' (Docker service name)
            CLAMAV_PORT: 3310 (default ClamAV port)
            MAX_FILE_SIZE_MB: 5 (maximum file size for scanning)
            TEMP_RETENTION_HOURS: 1 (how long to keep temp files)
        """
        # Loading the configuration with the possibility of redefinition
        self.host: str = host or os.getenv('CLAMAV_HOST', 'clamav')
        self.port: int = port or int(os.getenv('CLAMAV_PORT', '3310'))
        self.max_file_size_mb: int = max_file_size_mb or int(os.getenv('MAX_FILE_SIZE_MB', '5'))
        self.temp_retention_hours: int = temp_retention_hours or int(
            os.getenv('TEMP_RETENTION_HOURS', '1')
        )

        self.max_file_size_bytes: int = self.max_file_size_mb * 1024 * 1024
        self.cd: Optional[clamd.ClamdNetworkSocket] = None
        self.temp_dir: Optional[Path] = None
        self.available: bool = False
        self._cleanup_thread: Optional[threading.Thread] = None
        self._stop_cleanup: threading.Event = threading.Event()

        # Initialization
        self._connect()
        self._init_temp_directory()
        self._cleanup_old_files()

        if enable_auto_cleanup and self.temp_dir:
            self._start_auto_cleanup()

        if self.available:
            logger.info(
                "ClamAVScanner is initialized. Available: %s",
                self.available
            )
        else:
            logger.warning("ClamAVScanner is initialized. Not available")

    def _connect(self) -> None:
        """
        Establish connection to ClamAV server.
        
        Tests the connection by sending a PING command and retrieving the version.
        Sets self.available to True if connection successful.
        
        Handles various connection errors:
        - clamd.ConnectionError: ClamAV not reachable
        - socket.gaierror: DNS resolution failed
        - socket.timeout: Connection timeout
        """
        try:
            self.cd = clamd.ClamdNetworkSocket(
                host=self.host,
                port=self.port,
                timeout=30
            )
            # Checking the connection
            ping_result = self.cd.ping()
            self.available = ping_result == 'PONG'

            if self.available:
                # Getting the ClamAV version
                version = self.cd.version()
                logger.info("Connected to ClamAV %s:%d", self.host, self.port)
                logger.info("Version: %s", version)
            else:
                logger.error("ClamAV did not respond to ping: %s:%d", self.host, self.port)

        except clamd.ConnectionError as e:
            logger.error("Error connecting to ClamAV: %s", e)
            logger.error(
                "Check if the clamav container is running and if the port is available. %d",
                self.port
            )
            self.available = False

        except (gaierror, timeout) as e:
            logger.error("Network error when connecting to ClamAV: %s", e)
            self.available = False

        except Exception as e:
            logger.error("Unexpected error when connecting to ClamAV: %s", e, exc_info=True)
            self.available = False

    def _init_temp_directory(self) -> None:
        """
        Create temporary directory for ClamAV operations.
        
        Attempts to use RAM disk (/dev/shm) for better performance if available.
        Falls back to system temp directory if RAM disk not present.
        
        Performs write test to verify permissions and checks available disk space.
        """
        try:
            # Trying to use RAM disk for speed
            if os.path.exists('/dev/shm'):
                base_temp = Path('/dev/shm') / 'mft_clamav'
                logger.info("RAM disk (/dev/shm) is used for temporary files")
            else:
                base_temp = Path(tempfile.gettempdir()) / 'mft_clamav'
                logger.info("The system temp is in use: %s", tempfile.gettempdir())

            base_temp.mkdir(parents=True, exist_ok=True)
            self.temp_dir = base_temp

            # Checking the recording rights
            test_file = self.temp_dir / f'.write_test_{os.getpid()}'
            test_file.touch()
            test_file.write_text('test')
            test_file.unlink()

            # Checking the available space
            disk_usage = shutil.disk_usage(self.temp_dir)
            free_mb = disk_usage.free / (1024 * 1024)

            logger.info("Temporary directory: %s", self.temp_dir)
            logger.info("There are %.0f MB free", free_mb)
            logger.info("There are %d hours to keep files", self.temp_retention_hours)

        except PermissionError as e:
            logger.error("Access rights error when creating a temporary directory: %s", e)
            self._fallback_temp_directory()

        except OSError as e:
            logger.error("I/O error when creating a temporary directory: %s", e)
            self._fallback_temp_directory()

        except Exception as e:
            logger.error(
                "Unexpected error when creating a temporary directory: %s",
                e, exc_info=True
            )
            self._fallback_temp_directory()

    def _fallback_temp_directory(self) -> None:
        """
        Create fallback temporary directory when primary location fails.
        
        Uses tempfile.mkdtemp to create a unique temporary directory.
        This is a last resort when /dev/shm or system temp is not writable.
        """
        try:
            self.temp_dir = Path(tempfile.mkdtemp(prefix='mft_clamav_'))
            logger.info("The backup directory is in use: %s", self.temp_dir)

        except (PermissionError, OSError) as e:
            logger.error("Critical error: failed to create a backup directory: %s", e)
            self.temp_dir = None

    def _cleanup_old_files(self) -> int:
        """
        Remove temporary files older than retention period.
        
        Returns:
            Number of files successfully deleted
            
        Files matching pattern 'scan_*' in temp_dir are checked against
        retention period (temp_retention_hours). Old files are deleted
        and total freed space is logged.
        """
        if not self.temp_dir or not self.temp_dir.exists():
            return 0

        try:
            cutoff_time = datetime.now() - timedelta(hours=self.temp_retention_hours)
            cutoff_timestamp = cutoff_time.timestamp()

            deleted = 0
            total_size = 0

            for file_path in self.temp_dir.glob('scan_*'):
                if file_path.is_file():
                    try:
                        # Check the age of the file
                        if file_path.stat().st_mtime < cutoff_timestamp:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            deleted += 1
                            total_size += file_size
                            logger.debug("Old file deleted: %s", file_path.name)
                    except (PermissionError, OSError) as e:
                        logger.warning("Couldn't delete the file %s: %s", file_path.name, e)

            if deleted > 0:
                logger.info(
                    "Number of cleared temporary files: %d (%.2f MB)",
                    deleted, total_size / (1024 * 1024)
                )

            return deleted

        except (PermissionError, OSError) as e:
            logger.error("Access error when clearing temporary files: %s", e)
            return 0
        except Exception as e:
            logger.error("Unexpected error when clearing temporary files: %s", e, exc_info=True)
            return 0

    def _start_auto_cleanup(self, interval_hours: int = 6) -> None:
        """
        Start background thread for automatic cleanup of old temporary files.
        
        Args:
            interval_hours: Time between cleanup runs (default: 6 hours)
            
        The cleanup thread runs as a daemon so it won't prevent program exit.
        Uses threading.Event for clean shutdown.
        """
        def cleanup_worker():
            logger.info("Background cleaning is running (interval: %d h)", interval_hours)
            while not self._stop_cleanup.is_set():
                try:
                    # Waiting for the specified interval
                    self._stop_cleanup.wait(interval_hours * 3600)
                    if not self._stop_cleanup.is_set():
                        self._cleanup_old_files()

                except threading.ThreadError as e:
                    logger.error("Thread error in background cleaning: %s", e)

                except Exception as e:
                    logger.error("Unexpected error in background cleaning: %s", e, exc_info=True)

        try:
            self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
            self._cleanup_thread.start()

        except threading.ThreadError as e:
            logger.error("Couldn't start the background cleanup thread: %s", e)

    def scan_stream(self, data: bytes, filename: str) -> Tuple[bool, Optional[str]]:
        """
        Scan file data in memory using ClamAV's INSTREAM command.
        
        This is the primary method used by the upload API to validate files
        before they are saved to disk. The file is scanned in memory, preventing
        potentially malicious files from ever being written to disk.
        
        Args:
            data: Raw file bytes from the upload request
            filename: Original filename for logging purposes
            
        Returns:
            Tuple[bool, Optional[str]]:
                (True, None): File is clean and safe to process
                (False, "virus_name"): Virus detected with signature name
                (False, "ERROR: ..."): Scan failed with error description
                
        Note:
            If ClamAV is unavailable, returns (True, None) to avoid blocking uploads.
            This behavior can be changed by modifying the early return condition.
        """
        if not self.available:
            logger.warning("ClamAV is not available - skip checking the %s file", filename)
            return True, None  # Return True in order not to block the uploading

        if not self.cd:
            logger.error("ClamAV connection is not established for %s file", filename)
            return False, "ERROR: ClamAV connection is not established"

        # Checking the size
        if len(data) > self.max_file_size_bytes:
            logger.warning(
                "The %s file exceeds the %d MB limit",
                filename, self.max_file_size_mb
            )
            return False, f"ERROR: File too large (max {self.max_file_size_mb}MB)"

        try:
            logger.info("Memory scan: %s (%d bytes)", filename, len(data))
            result = self.cd.instream(data)

            if result:
                for _, (status, signature) in result.items():
                    if status == 'FOUND':
                        logger.warning("VIRUS detected in %s: %s", filename, signature)
                        return False, signature

                    elif status == 'ERROR':
                        logger.error("Scan error %s: %s", filename, signature)
                        return False, f"ERROR: {signature}"

            logger.info("The file is clean: %s", filename)
            return True, None

        except clamd.ConnectionError as e:
            logger.error("Loss of connection to ClamAV during scanning %s: %s", filename, e)
            self.available = False
            return False, "ERROR: Connection lost"

        except clamd.BufferTooLongError as e:
            logger.error("File too large for buffer %s: %s", filename, e)
            return False, "ERROR: Buffer too long"

        except timeout as e:
            logger.error("Socket timeout during scanning %s: %s", filename, e)
            return False, "ERROR: Timeout"

        except gaierror as e:
            logger.error("Address resolution error during scanning %s: %s", filename, e)
            return False, "ERROR: Address resolution failed"

        except OSError as e:
            logger.error("Network/OS error during scanning %s: %s", filename, e)
            return False, f"ERROR: Network error: {str(e)}"

        except MemoryError as e:
            logger.error("Not enough memory to scan %s: %s", filename, e)
            return False, "ERROR: Out of memory"

        except Exception as e:
            logger.error("Unexpected error during scanning %s: %s", filename, e, exc_info=True)
            return False, f"ERROR: {str(e)}"

    def manual_cleanup(self, max_age_hours: Optional[int] = None) -> int:
        """
        Manually trigger cleanup of temporary files.
        
        This method can be called via API endpoint for administrative purposes.
        Useful for immediate cleanup or testing.
        
        Args:
            max_age_hours: Delete files older than this many hours.
                          If None, uses self.temp_retention_hours.
            
        Returns:
            Number of files successfully deleted
            
        Example:
            deleted = scanner.manual_cleanup(max_age_hours=24)
            print(f"Deleted {deleted} files older than 24 hours")
        """
        if max_age_hours is not None:
            original_retention = self.temp_retention_hours
            self.temp_retention_hours = max_age_hours
            try:
                deleted = self._cleanup_old_files()
            finally:
                self.temp_retention_hours = original_retention
        else:
            deleted = self._cleanup_old_files()

        return deleted

    def get_stats(self) -> dict:
        """
        Get current scanner statistics and status.
        
        Returns dictionary containing:
        - Connection status and configuration
        - Temporary directory information
        - Number of temp files and their total size
        - Available disk space
        
        Returns:
            dict: Scanner statistics for monitoring and debugging
        """
        stats = {
            'available': self.available,
            'host': self.host,
            'port': self.port,
            'max_file_size_mb': self.max_file_size_mb,
            'temp_retention_hours': self.temp_retention_hours,
            'temp_dir': str(self.temp_dir) if self.temp_dir else None,
        }

        if self.temp_dir and self.temp_dir.exists():
            try:
                # Counting temporary files
                temp_files = list(self.temp_dir.glob('scan_*'))
                total_size = 0
                valid_files = []

                for f in temp_files:
                    if f.is_file():
                        try:
                            total_size += f.stat().st_size
                            valid_files.append(f)

                        except (OSError, PermissionError):
                            continue

                stats['temp_stats'] = {
                    'files_count': len(valid_files),
                    'total_size_mb': round(total_size / (1024 * 1024), 2),
                }

                # Information about available space
                try:
                    disk_usage = shutil.disk_usage(self.temp_dir)
                    stats['temp_stats']['free_space_mb'] = round(disk_usage.free / (1024 * 1024), 2)

                except (OSError, PermissionError) as e:
                    logger.warning("Couldn't get information about the disk: %s", e)
                    stats['temp_stats']['free_space_mb'] = None

            except (OSError, PermissionError) as e:
                logger.warning("Couldn't get statistics on temporary files: %s", e)
                stats['temp_stats'] = {'error': str(e)}

        return stats

    def is_available(self) -> bool:
        """
        Check if ClamAV is currently available and responding.
        
        Returns:
            bool: True if ClamAV connection is active and working
        """
        return self.available

    def __del__(self):
        """Clean up background threads when scanner is destroyed."""
        if hasattr(self, '_stop_cleanup'):
            self._stop_cleanup.set()
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=2)


# Creating a global instance of the scanner
try:
    clamav_scanner = ClamAVScanner()

except (
    ValueError, OSError, PermissionError, clamd.ConnectionError, ImportError, AttributeError
) as e:
    logger.critical("Critical error during initialization of ClamAVScanner: %s", e, exc_info=True)

    # Creating a stub
    class DummyScanner:
        """
        Fallback scanner when real ClamAV scanner cannot be initialized.
        
        This dummy implementation allows the application to continue running
        without virus scanning capability. All scan operations return success
        with warnings logged.
        """
        def is_available(self) -> bool:
            """Return False indicating scanner is not available."""
            return False

        def scan_stream(self, data: bytes, filename: str) -> Tuple[bool, Optional[str]]:
            """
            Log warning and return success to avoid blocking uploads.
            
            Args:
                data: File data (ignored)
                filename: Name of file being checked
                
            Returns:
                Tuple[bool, None]: Always returns (True, None) with warning log
            """
            logger.warning("DummyScanner is in use - skip checking the file %s", filename)
            return True, None

        def get_stats(self) -> dict:
            """Return basic stats indicating scanner is unavailable."""
            return {'available': False, 'error': 'Initialization failed'}

        def manual_cleanup(self, *args, **kwargs) -> int:
            """Return 0 as no files can be cleaned."""
            return 0

    clamav_scanner = DummyScanner()

except Exception as e:
    # Unexpected errors - re-raise to let the application crash
    logger.error("Unexpected error during ClamAVScanner initialization: %s", e, exc_info=True)
    raise

def scan_stream(data: bytes, filename: str) -> Tuple[bool, Optional[str]]:
    """
    Convenience function for backward compatibility.
    
    Args:
        data: Raw file bytes to scan
        filename: Original filename for logging
        
    Returns:
        Same as ClamAVScanner.scan_stream()
    """
    return clamav_scanner.scan_stream(data, filename)
