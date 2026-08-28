"""
Core module containing base classes and interfaces for the study framework.
"""

from .dashboard import DashboardBase, DashboardColumn
from .api import APIBase, CoreAPIEndpoints
from .processing import DataProcessorBase
from .processing_scripts import DataProcessor, process_all_data, generate_all_summaries, process_garmin_files
from .internal_web import InternalWebBase
from .landing_page import LandingPageBase
from .config import StudyFrameworkConfig, get_config, set_config_file
from .handlers import (
    login_check, login_code_check, save_info, save_user_ping,
    check_end_date, save_file, save_logfile, save_daily_diary_file,
    save_ema_file, allowed_file, get_db, get_db_client,
    get_latest_app_version, save_json_data, generate_token, generate_password,
    create_user, create_multiple_users, get_all_users, export_users_csv,
    create_admin_user, verify_admin_login
)
from .schemas import (
    LoginSchema, LoginCodeSchema, UserInfoSchema, UserPingSchema,
    EMASchema, UploadFileSchema, DebugSchema, SocialMediaSchema
)

__all__ = [
    'DashboardBase', 
    'DashboardColumn', 
    'APIBase', 
    'CoreAPIEndpoints',
    'DataProcessorBase',
    'InternalWebBase',
    'LandingPageBase',
    'StudyFrameworkConfig',
    'get_config',
    'set_config_file',
    # Handlers
    'login_check', 'login_code_check', 'save_info', 'save_user_ping',
    'check_end_date', 'save_file', 'save_logfile', 'save_daily_diary_file',
    'save_ema_file', 'allowed_file', 'get_db', 'get_db_client',
    'get_latest_app_version', 'save_json_data',
    'generate_token', 'generate_password', 'create_user', 'create_multiple_users',
    'get_all_users', 'export_users_csv', 'create_admin_user', 'verify_admin_login',
    # Processing
    'DataProcessor', 'process_all_data', 'generate_all_summaries', 'process_garmin_files',
    # Schemas
    'LoginSchema', 'LoginCodeSchema', 'UserInfoSchema', 'UserPingSchema',
    'EMASchema', 'UploadFileSchema', 'DebugSchema', 'SocialMediaSchema'
]
