"""
Core handler functions for the study framework.
These functions provide the standard functionality used across all studies.
"""

import json
import os
import time
import logging
import random
import string
import hashlib
import csv
import io
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from werkzeug.utils import secure_filename
from bson import ObjectId

from study_framework_core.core.config import get_config

# File extensions allowed for upload
ALLOWED_EXTENSIONS = set(['db', 'dbr', 'dbre', 'zip', 'mov', 'mp4', 'txt', "json", "fit"])

def current_milli_time():
    """Get current time in milliseconds."""
    return int(round(time.time() * 1000))

def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_client():
    """Get MongoDB client using configuration."""
    config = get_config()
    from pymongo import MongoClient
    import urllib.parse
    import os
    
    # # Debug: Print environment and config info
    # print(f"DEBUG: STUDY_CONFIG_FILE env var: {os.getenv('STUDY_CONFIG_FILE')}")
    # print(f"DEBUG: Current working directory: {os.getcwd()}")
    # print(f"DEBUG: Database config - host: {config.database.host}, port: {config.database.port}, user: {config.database.username}, db: {config.database.database}")
    
    db_user = urllib.parse.quote(str(config.database.username))
    db_pwd = urllib.parse.quote(str(config.database.password))
    db_uri = f"mongodb://{db_user}:{db_pwd}@{config.database.host}:{config.database.port}/{config.database.database}"
    
    # print(f"DEBUG: MongoDB URI: {db_uri}")
    
    return MongoClient(db_uri)

def get_db():
    """Get MongoDB database instance."""
    client = get_db_client()
    config = get_config()
    return client[config.database.database]

def login_check(uid, password, device):
    """Check user login credentials."""
    try:
        db = get_db()
        config = get_config()
        user = db[config.collections.USERS].find_one({'uid': uid})
        if user is None:
            return -1
        if user['study_pass'] == password:
            login_timestamps = []
            device_login_time = str(device)+'_login_time'
            if device_login_time in user:
                login_timestamps = user[device_login_time]

            login_timestamps.append(current_milli_time())
            db[config.collections.USERS].update_one({'uid': uid}, {'$set': {device_login_time: login_timestamps}})
            if 'ra' in user:
                return 2
            else:
                return 1
        else:
            return 0
    except Exception as e:
        logging.error(f"Login check failed for user {uid}: {e}")
        return -1

def login_code_check(code, device):
    """Check login code and return user ID."""
    try:
        # Special test code
        if code == "A1YJPU":
            return 1, "test004"
            
        db = get_db()
        user = db['user_code_mappings'].find_one({'uid_code': code})
        if user is None:
            return 0, None
        device_login_time = str(device) + '_login_time'
        uid = user['uid']
        login_timestamps = []

        if device_login_time in user:
            login_timestamps = user[device_login_time]

        login_timestamps.append(current_milli_time())

        access_codes = []
        if 'access_codes' in user:
            access_codes = user['access_codes']
        access_codes.append(code)
        config = get_config()
        db[config.collections.USERS].update_one({'uid': uid}, {'$set': {device_login_time: login_timestamps, 'access_codes': access_codes}})
        return 1, uid
    except Exception as e:
        logging.error(f"Login code check failed for code {code}: {e}")
        return 0, None

def save_info(uid, key, value):
    """Save user information to database."""
    try:
        db = get_db()
        config = get_config()
        db[config.collections.USERS].update_one({'uid': uid}, {'$set': {key: value}})
    except Exception as e:
        logging.error(f"Failed to save info for user {uid}: {e}")

def save_user_ping(uid, device, device_type):
    """Save user ping information."""
    try:
        db = get_db()
        config = get_config()
        timestamp = current_milli_time()
        db[config.collections.USERS].update_one({'uid': uid}, {'$set': {str(device)+'_last_ping': timestamp}})

        info = {'uid': uid, 'device': device, 'device_type': device_type, 'timestamp': timestamp}
        try:
            db[config.collections.USER_PINGS].insert_one(info)
        except DuplicateKeyError:
            logging.warning(f"Duplicate ping insert failed for {info}")
    except Exception as e:
        logging.error(f"Failed to save user ping for {uid}: {e}")

def check_end_date(uid):
    """Check if user's study end date has passed."""
    try:
        db = get_db()
        user_info = db['user_info'].find_one({'uid': ''+uid})
        if user_info is not None:
            if 'end_date' in user_info:
                if user_info['end_date'] > 0:
                    end_date = user_info['end_date']
                    if datetime.now().timestamp() >= end_date + 86400 + 10800:
                        return False
        return True
    except Exception as e:
        logging.error(f"Failed to check end date for user {uid}: {e}")
        return True

def save_file(uid, file):
    """Save uploaded file to appropriate directory."""
    try:
        config = get_config()
        filename = secure_filename(file.filename)

        if ".txt" in filename:
            directory = os.path.join(config.paths.data_upload_logs_path, 'phone', str(uid))
        else:
            directory = os.path.join(config.paths.data_upload_path, 'phone', str(uid))

        if not os.path.exists(directory):
            os.makedirs(directory)
        
        full_path = os.path.join(directory, filename)
        if os.path.isfile(full_path):
            new_full_path = os.path.join(directory, filename+'.duplicate')
            full_path = new_full_path

        file.save(full_path)
        logging.info(f"File saved: {full_path}")
    except Exception as e:
        logging.error(f"Failed to save file for user {uid}: {e}")
        raise

def save_logfile(uid, file):
    """Save log file to logs directory."""
    try:
        config = get_config()
        filename = secure_filename(file.filename)
        directory = os.path.join(config.paths.data_upload_logs_path, 'phone', str(uid))
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        full_path = os.path.join(directory, filename)
        file.save(full_path)
        logging.info(f"Log file saved: {full_path}")
    except Exception as e:
        logging.error(f"Failed to save log file for user {uid}: {e}")
        raise

def save_daily_diary_file(uid, file):
    """Save daily diary file."""
    try:
        config = get_config()
        filename = secure_filename(file.filename)
        directory = os.path.join(config.paths.active_sensing_upload_path, str(uid), "daily_diaries")
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        full_path = os.path.join(directory, filename)
        if os.path.isfile(full_path):
            new_full_path = os.path.join(directory, filename + '.duplicate')
            full_path = new_full_path

        file.save(full_path)
        logging.info(f"Daily diary file saved: {full_path}")
    except Exception as e:
        logging.error(f"Failed to save daily diary file for user {uid}: {e}")
        raise

def save_ema_file(uid, file):
    """Save EMA file."""
    try:
        config = get_config()
        filename = secure_filename(file.filename)
        directory = os.path.join(config.paths.active_sensing_upload_path, str(uid), "ema_responses")
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        full_path = os.path.join(directory, filename)
        if os.path.isfile(full_path):
            new_full_path = os.path.join(directory, filename+'.duplicate')
            full_path = new_full_path
        
        file.save(full_path)
        logging.info(f"EMA file saved: {full_path}")
    except Exception as e:
        logging.error(f"Failed to save EMA file for user {uid}: {e}")
        raise

def handle_timestamp_format(timestamp):
    """Handle different timestamp formats."""
    str_time = str(timestamp)
    if len(str_time.split(".")[0]) == 10:
        return float(timestamp)
    else:
        return float(timestamp) / 1000

def insert_to_db(collection, record):
    """Insert record to database collection."""
    try:
        db = get_db()
        db[collection].insert_one(record)
    except DuplicateKeyError:
        logging.warning(f"Duplicate key error for collection {collection}")
        pass

def save_ema(ema):
    """Save EMA data to database and file system."""
    try:
        record = {'uid': ema['uid']}
        timestamp = handle_timestamp_format(ema['timestamp'])
        record['timestamp'] = timestamp
        record['timestamp_saved'] = handle_timestamp_format(datetime.now().timestamp())
        record['event_id'] = ema['event_id']
        ema_event = ema['event']
        
        if isinstance(ema_event, str):
            event = json.loads(ema_event)
        else:
            event = json.loads(ema['event'].decode("utf-8"))
        
        record['ema_type'] = list(event.keys())[0]
        record['data'] = event[record['ema_type']]
        device_type = ema['device_type']
        
        if device_type == 'ios':
            insert_to_db('ios_ema_http', record)
        if device_type == 'android':
            insert_to_db('android_ema_http', record)

        # Save to file system
        config = get_config()
        dt_string = datetime.fromtimestamp(record['timestamp']).strftime('%Y-%m-%d')
        directory = os.path.join(config.paths.data_processed_path, 'ema', str(ema['uid']), dt_string)
        
        if not os.path.exists(directory):
            os.makedirs(directory)

        filename = os.path.join(directory, str(record['timestamp']).split('.')[0] + ".json")
        ema['timestamp_saved'] = record['timestamp_saved']
        ema['auth_key'] = ""
        
        with open(filename, 'w') as file:
            file.write(json.dumps(ema))
            
        logging.info(f"EMA data saved: {filename}")
    except Exception as e:
        logging.error(f"Failed to save EMA data: {e}")
        raise


def get_latest_app_version():
    """Get the latest app version from file."""
    try:
        config = get_config()
        version_file = os.path.join(config.paths.base_dir, "latest_app_version.txt")
        with open(version_file, 'r') as f:
            return f.read().strip()
    except Exception as e:
        logging.warning(f"Could not read latest app version: {e}")
        return "unknown"


def save_json_data(uid, data, data_type="news_task"):
    """Save JSON data for a user."""
    try:
        config = get_config()
        user_path = os.path.join(config.paths.active_sensing_upload_path, str(uid), data_type)
        os.makedirs(user_path, exist_ok=True)

        current_time = time.strftime("%Y%m%d-%H%M%S")
        file_path = os.path.join(user_path, f'answers_{current_time}.json')
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

        logging.info(f"JSON data saved: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving JSON data: {e}")
        return False


def generate_token(study_id, length):
    """Generate a token for user code mapping."""
    pwd = []
    length = length - 2
    chars = string.ascii_uppercase + string.digits
    random.seed(os.urandom(1024))
    for i in range(length):
        pwd.append(random.choice(chars))

    token = ''.join(pwd)
    return str(study_id) + str(token)


def generate_password(length):
    """Generate a secure password."""
    pwd = []

    pwd.append(random.choice(string.ascii_lowercase))
    pwd.append(random.choice(string.ascii_uppercase))
    pwd.append(str(random.randint(0, 9)))
    pwd.append(random.choice('@#$%'))

    length = length-6

    chars = string.ascii_letters + string.digits + '@#$%'
    random.seed(os.urandom(1024))
    for i in range(length):
        pwd.append(random.choice(chars))

    random.shuffle(pwd)
    password = ''.join(pwd)
    password = random.choice(string.ascii_letters) + password
    password = password + random.choice(string.ascii_letters)
    return password


def create_user(uid, email=None, study_id="A1"):
    """Create a new participant with generated credentials."""
    try:
        db = get_db()
        config = get_config()
        
        # Check if user already exists
        existing_user = db[config.collections.USERS].find_one({'uid': uid})
        if existing_user:
            return {'success': False, 'error': f'User {uid} already exists'}
        
        # Generate passwords
        study_password = generate_password(10)
        garmin_password = generate_password(13)
        
        # Generate encryption key
        complete = uid + study_password
        hash_obj = hashlib.md5(str.encode(complete))
        hash_str = hash_obj.hexdigest()
        encryption_key = hash_str[:16]
        
        # Create user object
        user_obj = {
            'uid': uid,
            'study_pass': study_password,
            'garmin_pass': garmin_password,
            'file_encryption_key': encryption_key
        }
        
        if email:
            user_obj['email'] = email
        
        # Insert user with duplicate key error handling
        try:
            db[config.collections.USERS].insert_one(user_obj)
        except DuplicateKeyError:
            return {'success': False, 'error': f'User {uid} already exists (duplicate key error)'}
        
        # Generate and insert user code mapping
        user_code_obj = {
            'uid': uid,
            'uid_code': generate_token(study_id, 6)
        }
        
        try:
            db[config.collections.USER_CODE_MAPPINGS].insert_one(user_code_obj)
        except DuplicateKeyError:
            # If code mapping fails, try again with a new token
            user_code_obj['uid_code'] = generate_token(study_id, 6)
            db[config.collections.USER_CODE_MAPPINGS].insert_one(user_code_obj)
        
        logging.info(f"Created participant: {uid}")
        
        return {
            'success': True,
            'user': {
                'uid': uid,
                'study_pass': study_password,
                'garmin_pass': garmin_password,
                'uid_code': user_code_obj['uid_code'],
                'email': email
            }
        }
        
    except Exception as e:
        logging.error(f"Error creating participant {uid}: {e}")
        return {'success': False, 'error': str(e)}


def create_multiple_users(user_prefix, num_users, start_id=1, email_base=None, study_id="A1"):
    """Create multiple participants with sequential IDs."""
    try:
        results = []
        
        for i in range(start_id, start_id + num_users):
            uid = f"{user_prefix}{i:03d}"
            email = None
            if email_base:
                email = f"{email_base}+{uid}@gmail.com"
            
            result = create_user(uid, email, study_id)
            results.append({
                'user_id': i,
                'uid': uid,
                'result': result
            })
        
        return {
            'success': True,
            'results': results,
            'total_created': len([r for r in results if r['result']['success']]),
            'total_failed': len([r for r in results if not r['result']['success']])
        }
        
    except Exception as e:
        logging.error(f"Error creating multiple participants: {e}")
        return {'success': False, 'error': str(e)}


def get_all_users():
    """Get all participants from the database."""
    try:
        db = get_db()
        config = get_config()
        users = list(db[config.collections.USERS].find({}, {'_id': 0, 'study_pass': 0, 'garmin_pass': 0, 'file_encryption_key': 0}))
        return {'success': True, 'users': users}
    except Exception as e:
        logging.error(f"Error getting participants: {e}")
        return {'success': False, 'error': str(e)}


def export_users_csv():
    """Export participants to CSV format."""
    try:
        db = get_db()
        config = get_config()
        users = list(db[config.collections.USERS].find({}, {'_id': 0}))
        
        if not users:
            return {'success': False, 'error': 'No participants found'}
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['UID', 'Email', 'Study Password', 'Garmin Password', 'UID Code'])
        
        # Write user data
        for user in users:
            uid_code = db['user_code_mappings'].find_one({'uid': user['uid']})
            uid_code_str = uid_code['uid_code'] if uid_code else 'N/A'
            
            writer.writerow([
                user.get('uid', ''),
                user.get('email', ''),
                user.get('study_pass', ''),
                user.get('garmin_pass', ''),
                uid_code_str
            ])
        
        output.seek(0)
        return {
            'success': True,
            'csv_content': output.getvalue(),
            'filename': f'participants_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        }
        
    except Exception as e:
        logging.error(f"Error exporting participants: {e}")
        return {'success': False, 'error': str(e)}


# Internal Web Authentication Functions
def create_admin_user(username="admin", password=None):
    """Create admin user for internal web access."""
    try:
        db = get_db()
        
        # Check if admin user already exists
        existing_admin = db['admin_users'].find_one({'username': username})
        if existing_admin:
            return {'success': False, 'error': f'Admin user {username} already exists'}
        
        # Generate password if not provided
        if not password:
            password = generate_password(12)
        
        # Hash password
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        # Create admin user
        admin_user = {
            'username': username,
            'password_hash': password_hash,
            'created_at': int(datetime.now().timestamp()),
            'last_login': None
        }
        
        db['admin_users'].insert_one(admin_user)
        
        logging.info(f"Created admin user: {username}")
        
        return {
            'success': True,
            'username': username,
            'password': password,  # Return plain password for initial setup
            'message': f'Admin user {username} created successfully'
        }
        
    except Exception as e:
        logging.error(f"Error creating admin user: {e}")
        return {'success': False, 'error': str(e)}


def verify_admin_login(username, password):
    """Verify admin login credentials."""
    try:
        db = get_db()
        
        # Find admin user
        admin_user = db['admin_users'].find_one({'username': username})
        if not admin_user:
            return {'success': False, 'error': 'Invalid username or password'}
        
        # Verify password
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        if admin_user['password_hash'] != password_hash:
            return {'success': False, 'error': 'Invalid username or password'}
        
        # Update last login
        db['admin_users'].update_one(
            {'username': username},
            {'$set': {'last_login': int(datetime.now().timestamp())}}
        )
        
        return {'success': True, 'username': username}
        
    except Exception as e:
        logging.error(f"Error verifying admin login: {e}")
        return {'success': False, 'error': str(e)}

# def get_available_modules():
#     """Get list of available modules for the landing page."""
#     return [
#         {
#             'id': 'dashboard',
#             'name': 'Dashboard',
#             'description': 'View study compliance and data monitoring dashboard',
#             'icon': 'fas fa-chart-line',
#             'url': '/internal_web/dashboard',
#             'color': 'primary'
#         },
#         {
#             'id': 'user_management',
#             'name': 'Participant Management',
#             'description': 'Create and manage study participants',
#             'icon': 'fas fa-users',
#             'url': '/internal_web/user-management',
#             'color': 'success'
#         },
#         {
#             'id': 'ema_schedule',
#             'name': 'EMA Schedule',
#             'description': 'Manage EMA surveys and schedules',
#             'icon': 'fas fa-calendar-alt',
#             'url': '/internal_web/ema-schedule',
#             'color': 'info'
#         },
#         {
#             'id': 'config_schedule',
#             'name': 'Configuration',
#             'description': 'Manage study configuration and settings',
#             'icon': 'fas fa-cogs',
#             'url': '/internal_web/config-schedule',
#             'color': 'warning'
#         },
#         {
#             'id': 'downloads',
#             'name': 'Data Downloads',
#             'description': 'Download compliance reports and data exports',
#             'icon': 'fas fa-download',
#             'url': '/internal_web/download_compliance',
#             'color': 'secondary'
#         }
#     ]
