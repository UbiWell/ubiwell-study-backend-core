"""
Centralized configuration for the study framework.

This module provides a single source of truth for all configuration settings
that are shared across all components of the study framework.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict


# Collection names - centralized for consistency
class CollectionNames:
    """Centralized collection names for MongoDB collections."""
    # iOS data collections
    IOS_LOCATION = 'ios_location'
    IOS_BRIGHTNESS = 'ios_brightness'
    IOS_BLUETOOTH = 'ios_bluetooth'
    IOS_WIFI = 'ios_wifi'
    IOS_BATTERY = 'ios_battery'
    IOS_LOCK_UNLOCK = 'ios_lock_unlock'
    IOS_STEPS = 'ios_steps'
    IOS_ACTIVITY = 'ios_activity'
    IOS_ACCELEROMETER = 'ios_accelerometer'
    IOS_CALLLOG = 'ios_calllog'
    
    # Garmin data collections
    GARMIN_HR = 'garmin_hr'
    GARMIN_STRESS = 'garmin_stress'
    GARMIN_IBI = 'garmin_ibi'
    GARMIN_ACCELEROMETER = 'garmin_accelerometer'
    GARMIN_RESPIRATION = 'garmin_respiration'
    GARMIN_STEPS = 'garmin_steps'
    GARMIN_ENERGY = 'garmin_energy'
    
    # EmpaTica data collections - REMOVED (outdated, no longer used)
    
    # App and EMA collections
    EMA_RESPONSE = "ema_response"
    EMA_STATUS_EVENTS = "ema_status_events"
    APP_USAGE_LOGS = "app_usage_logs"
    NOTIFICATION_EVENTS = "notification_events"
    APP_SCREEN_EVENTS = "app_screen_events"
    
    # Summary collections
    DAILY_SUMMARY = 'daily_summaries'
    
    # Unknown events collection
    UNKNOWN_EVENTS = 'unknown_events_data'
    
    # User collections
    USERS = 'users'
    USER_CODE_MAPPINGS = 'user_code_mappings'
    USER_PINGS = 'user_pings'


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str = "localhost"
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None
    database: str = "study_db"
    auth_source: str = "admin"
    auth_mechanism: str = "SCRAM-SHA-1"


@dataclass
class ServerConfig:
    """Server configuration settings."""
    host: str = "127.0.0.1"  # Localhost for production with reverse proxy
    port: int = 8000  # Only used for development or direct access
    debug: bool = False
    workers: int = 3
    timeout: int = 120
    socket_path: Optional[str] = None  # Unix socket path for production


@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None  # Keep for backward compatibility, but not used
    max_bytes: int = 10485760  # 10MB
    backup_count: int = 5

@dataclass
class SecurityConfig:
    """Security configuration settings."""
    secret_key: Optional[str] = None
    auth_key: str = "your-auth-key"
    tokens: list = None
    allowed_ips: list = None
    cors_origins: list = None
    announcement_pass_key: str = "study123"
    
    def __post_init__(self):
        if self.tokens is None:
            self.tokens = []
        if self.allowed_ips is None:
            self.allowed_ips = []
        if self.cors_origins is None:
            self.cors_origins = []


@dataclass
class PathsConfig:
    """Path configuration settings."""
    base_dir: str = "/mnt/study"
    data_dir: str = "/mnt/study/data"
    logs_dir: str = "/mnt/study/logs"
    sockets_dir: str = "/var/sockets"
    static_dir: str = "/mnt/study/static"
    uploads_dir: str = "/mnt/study/uploads"
    data_upload_path: str = "/mnt/study/data_uploads/uploads"
    data_processed_path: str = "/mnt/study/data_uploads/processed"
    data_exceptions_path: str = "/mnt/study/data_uploads/exceptions"
    data_upload_logs_path: str = "/mnt/study/data_uploads/logs"
    active_sensing_upload_path: str = "/mnt/study/active_sensing"
    ema_file_path: str = "/mnt/study/ema_surveys"
    scheduled_ema_path: str = "/mnt/study/scheduled-ema-surveys"
    scheduled_config_path: str = "/mnt/study/scheduled-config-files"
    config_dir: str = "/mnt/study/config-files"


class StudyFrameworkConfig:
    """
    Centralized configuration manager for the study framework.
    
    This class provides a single source of truth for all configuration settings
    that are shared across all components of the study framework.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to configuration file (JSON format)
        """
        self.config_file = config_file
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file or use defaults."""
        if self.config_file and os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
        else:
            config_data = {}
        
        # Initialize configuration sections
        self.database = DatabaseConfig(**config_data.get('database', {}))
        self.server = ServerConfig(**config_data.get('server', {}))
        self.logging = LoggingConfig(**config_data.get('logging', {}))
        self.collections = CollectionNames()  # Centralized collection names
        self.security = SecurityConfig(**config_data.get('security', {}))
        self.paths = PathsConfig(**config_data.get('paths', {}))
        
        # Load from environment variables if not set
        self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Database
        if os.getenv('DB_HOST'):
            self.database.host = os.getenv('DB_HOST')
        if os.getenv('DB_PORT'):
            self.database.port = int(os.getenv('DB_PORT'))
        if os.getenv('DB_USERNAME'):
            self.database.username = os.getenv('DB_USERNAME')
        if os.getenv('DB_PASSWORD'):
            self.database.password = os.getenv('DB_PASSWORD')
        
        # Server
        if os.getenv('SERVER_HOST'):
            self.server.host = os.getenv('SERVER_HOST')
        if os.getenv('SERVER_PORT'):
            self.server.port = int(os.getenv('SERVER_PORT'))
        if os.getenv('SERVER_WORKERS'):
            self.server.workers = int(os.getenv('SERVER_WORKERS'))
        
        # Logging
        if os.getenv('LOG_LEVEL'):
            self.logging.level = os.getenv('LOG_LEVEL')
        if os.getenv('LOG_FORMAT'):
            self.logging.format = os.getenv('LOG_FORMAT')
        
        # Security
        if os.getenv('SECRET_KEY'):
            self.security.secret_key = os.getenv('SECRET_KEY')
        if os.getenv('AUTH_KEY'):
            self.security.auth_key = os.getenv('AUTH_KEY')
        if os.getenv('ALLOWED_TOKENS'):
            self.security.tokens = os.getenv('ALLOWED_TOKENS').split(',')
        if os.getenv('ANNOUNCEMENT_PASS_KEY'):
            self.security.announcement_pass_key = os.getenv('ANNOUNCEMENT_PASS_KEY')
        
        # Paths - Load from config file first, then environment variables as fallback
        if self.config_file and os.path.exists(self.config_file):
            # Config file should override defaults
            pass  # Already loaded above
        else:
            # Fallback to environment variables
            if os.getenv('BASE_DIR'):
                self.paths.base_dir = os.getenv('BASE_DIR')
            if os.getenv('LOGS_DIR'):
                self.paths.logs_dir = os.getenv('LOGS_DIR')
            if os.getenv('DATA_UPLOAD_PATH'):
                self.paths.data_upload_path = os.getenv('DATA_UPLOAD_PATH')
            if os.getenv('DATA_PROCESSED_PATH'):
                self.paths.data_processed_path = os.getenv('DATA_PROCESSED_PATH')
            if os.getenv('EMA_FILE_PATH'):
                self.paths.ema_file_path = os.getenv('EMA_FILE_PATH')
            if os.getenv('DATA_UPLOAD_LOGS_PATH'):
                self.paths.data_upload_logs_path = os.getenv('DATA_UPLOAD_LOGS_PATH')
            if os.getenv('ACTIVE_SENSING_UPLOAD_PATH'):
                self.paths.active_sensing_upload_path = os.getenv('ACTIVE_SENSING_UPLOAD_PATH')
    
    def save_config(self, config_file: Optional[str] = None):
        """Save current configuration to file."""
        if config_file is None:
            config_file = self.config_file
        
        if config_file:
            config_data = {
                'database': asdict(self.database),
                'server': asdict(self.server),
                'logging': asdict(self.logging),
                'security': asdict(self.security),
                'paths': asdict(self.paths)
            }
            
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
    
    def get_database_url(self) -> str:
        """Get MongoDB connection URL."""
        if self.database.username and self.database.password:
            return f"mongodb://{self.database.username}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.auth_source}?authMechanism={self.database.auth_mechanism}"
        else:
            return f"mongodb://{self.database.host}:{self.database.port}"
    
    def get_log_file_path(self, component: str) -> str:
        """Get log file path for a specific component."""
        if self.logging.file_path:
            return self.logging.file_path
        
        log_dir = Path(self.paths.logs_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        return str(log_dir / f"{component}.log")
    
    def get_socket_path(self, component: str) -> str:
        """Get socket path for a specific component."""
        socket_dir = Path(self.paths.sockets_dir)
        socket_dir.mkdir(parents=True, exist_ok=True)
        return str(socket_dir / f"{component}.sock")
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        # Check required directories
        required_dirs = [
            self.paths.base_dir,
            self.paths.data_dir,
            self.paths.logs_dir,
            self.paths.static_dir,
            self.paths.uploads_dir,
            self.paths.data_upload_path,
            self.paths.data_processed_path,
            self.paths.data_exceptions_path,
            self.paths.data_upload_logs_path,
            self.paths.active_sensing_upload_path,
            self.paths.ema_file_path
        ]
        
        for directory in required_dirs:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                except Exception as e:
                    print(f"Error creating directory {directory}: {e}")
                    return False
        
        return True


# Global configuration instance
config = StudyFrameworkConfig(os.getenv('STUDY_CONFIG_FILE'))


def get_config() -> StudyFrameworkConfig:
    """Get the global configuration instance."""
    return config


def set_config_file(config_file: str):
    """Set the configuration file and reload configuration."""
    global config
    config = StudyFrameworkConfig(config_file)
