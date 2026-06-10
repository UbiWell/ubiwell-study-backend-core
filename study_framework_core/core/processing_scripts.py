"""
Backend processing scripts for the study framework.
Handles data processing, Garmin FIT files, daily summaries, and data visualization.
"""

import os
import sys
import subprocess
import logging
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict, Any

import pandas as pd
import pymongo
from geopy import distance
import plotly.graph_objects as go

from study_framework_core.core.config import get_config
from study_framework_core.core.handlers import get_db


class DataProcessor:
    """Main data processor for handling all backend processing tasks."""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db()
        self.records = {}  # For batch processing
        self.batch_size = 2000  # Batch size for bulk inserts

        # Set to True to store accelerometer as time-windowed chunks (≤30 s / ≤750 samples per doc).
        # Set to False to store one document per sample (original behaviour).
        self.CHUNK_ACCELEROMETER = True
        self._ACC_MAX_CHUNK_SECONDS = 30.0
        self._ACC_MAX_SAMPLES_PER_CHUNK = 750

        self.setup_logging()
        self.init_collections()  # Initialize collections with indexes
    
    def setup_logging(self):
        """Setup logging for data processing."""
        # Debug: Print config paths
        # print(f"DEBUG: Config paths:")
        # print(f"  base_dir: {self.config.paths.base_dir}")
        # print(f"  logs_dir: {self.config.paths.logs_dir}")
        # print(f"  data_dir: {self.config.paths.data_dir}")
        
        # Ensure logs directory exists
        logs_dir = Path(self.config.paths.logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Get logging level from config
        log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)
        
        # Check for environment variable override
        if os.getenv('REDUCE_LOGGING', 'false').lower() == 'true':
            log_level = logging.WARNING
        elif os.getenv('LOG_LEVEL'):
            env_level = os.getenv('LOG_LEVEL').upper()
            log_level = getattr(logging, env_level, log_level)
        
        logging.basicConfig(
            level=log_level,
            format=self.config.logging.format,
            handlers=[
                logging.FileHandler(logs_dir / "data_processing.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def init_collections(self):
        """Initialize MongoDB collections with indexes for optimal performance."""
        try:
            import pymongo
            
            # iOS data collections
            self.db[self.config.collections.IOS_LOCATION].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING),
                ('event_id', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_WIFI].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING),
                ('event_id', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_BLUETOOTH].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_BRIGHTNESS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_LOCK_UNLOCK].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_BATTERY].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_ACTIVITY].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_STEPS].create_index([
                ('uid', pymongo.ASCENDING),
                ('start_timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_ACCELEROMETER].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.IOS_CALLLOG].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)

            self.db[self.config.collections.UNKNOWN_EVENTS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)

            # Android data collections (raw ingestion from .dbr files).
            # event_id here is the per-file monotonic row counter from Android's DBAdapter,
            # not the sensor event type (that lives in data_type). The (uid, timestamp, event_id)
            # triple gives us a stable dedupe key for retries / re-uploads.
            for android_collection in (
                self.config.collections.ANDROID_LOCATION,
                self.config.collections.ANDROID_LOCATION_PING,
                self.config.collections.ANDROID_WIFI,
                self.config.collections.ANDROID_WIFI_CONNECTED,
                self.config.collections.ANDROID_BLUETOOTH,
                self.config.collections.ANDROID_SCREEN_EVENT,
                self.config.collections.ANDROID_BATTERY,
                self.config.collections.ANDROID_ACTIVITY,
                self.config.collections.ANDROID_STEPS,
                self.config.collections.ANDROID_ACCELEROMETER,
                self.config.collections.ANDROID_APP_USAGE,
                self.config.collections.ANDROID_CALLLOG,
                self.config.collections.ANDROID_SMSLOG,
                self.config.collections.ANDROID_NOTIFICATION,
                self.config.collections.ANDROID_RUNNING_SERVICES,
                self.config.collections.ANDROID_SERVICES_STARTED,
                self.config.collections.ANDROID_UNKNOWN_EVENTS,
            ):
                self.db[android_collection].create_index([
                    ('uid', pymongo.ASCENDING),
                    ('timestamp', pymongo.ASCENDING),
                    ('event_id', pymongo.ASCENDING)
                ], unique=True, dropDups=True)
            
            # Garmin data collections
            self.db[self.config.collections.GARMIN_HR].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.GARMIN_STRESS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)

            self.db[self.config.collections.GARMIN_ACCELEROMETER].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.GARMIN_STEPS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.GARMIN_RESPIRATION].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.GARMIN_IBI].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.GARMIN_ENERGY].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            # EmpaTica data collections - REMOVED (outdated, no longer used)
            
            # App and EMA collections
            self.db[self.config.collections.EMA_RESPONSE].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.EMA_STATUS_EVENTS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.APP_USAGE_LOGS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.NOTIFICATION_EVENTS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.APP_SCREEN_EVENTS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            # Summary collections
            self.db[self.config.collections.DAILY_SUMMARY].create_index([
                ('uid', pymongo.ASCENDING),
                ('date', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            # User collections
            self.db[self.config.collections.USERS].create_index([
                ('uid', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            self.db[self.config.collections.USER_CODE_MAPPINGS].create_index([
                ('uid', pymongo.ASCENDING),
                ('uid_code', pymongo.ASCENDING)
            ], unique=True, dropDups=True)

            self.db[self.config.collections.USER_PINGS].create_index([
                ('uid', pymongo.ASCENDING),
                ('timestamp', pymongo.ASCENDING)
            ], unique=True, dropDups=True)
            
            
            self.logger.info("Successfully initialized MongoDB collections with indexes")
            
        except Exception as e:
            self.logger.error(f"Error initializing collections: {e}")
    
    def add_record(self, collection: str, record: dict):
        """Add a record to the batch processing queue."""
        if collection not in self.records:
            self.records[collection] = []
        self.records[collection].append(record)
        
        # Flush batch if it reaches the batch size
        if len(self.records[collection]) >= self.batch_size:
            self.flush_records(collection)
    
    def flush_records(self, collection: str = None):
        """Flush records to MongoDB using bulk insert."""
        
        if collection:
            collections_to_flush = [collection]
        else:
            collections_to_flush = list(self.records.keys())
            
        for coll in collections_to_flush:
            try:
                if coll in self.records and self.records[coll]:
                    self.db[coll].insert_many(self.records[coll], ordered=False)
                    self.logger.info(f"Bulk inserted {len(self.records[coll])} records to {coll}")
                    
            except Exception as e:
                self.logger.error(f"Error flushing records to {coll}: {e}")

            self.records[coll].clear()
    
    def archive_file(self, user: str, file_path: str):
        """Move processed file from upload to processed directory."""
        try:
            archive_dir = Path(self.config.paths.data_processed_path) / "phone" / user
            archive_dir.mkdir(parents=True, exist_ok=True)
            
            file_name = Path(file_path).name
            archive_path = archive_dir / file_name
            
            shutil.move(file_path, archive_path)
            self.logger.info(f"Archived file: {file_path} -> {archive_path}")
            
        except Exception as e:
            self.logger.error(f"Error archiving file {file_path}: {e}")
    
    def _build_accelerometer_chunk_records(self,  user: str, acc_df) -> List[Tuple[str, Dict[str, Any]]]:

        """Return (collection, record) tuples with accelerometer samples chunked into
        windows of at most _ACC_MAX_CHUNK_SECONDS seconds or _ACC_MAX_SAMPLES_PER_CHUNK samples."""
        out: List[Tuple[str, Dict[str, Any]]] = []
        if acc_df is None or acc_df.empty:
            return out

        df = acc_df.copy()
        df['_ts'] = df['timestamp'].astype(float) + df['micros'].astype(float) / 1_000_000
        df = df.sort_values('_ts').reset_index(drop=True)
        ts = df['_ts']
        n = len(df)
        processed_at = datetime.now().timestamp()

        start = 0
        while start < n:
            end = start + 1
            while end < n:
                if (float(ts.iloc[end] - ts.iloc[start]) > self._ACC_MAX_CHUNK_SECONDS
                        or (end - start) >= self._ACC_MAX_SAMPLES_PER_CHUNK):
                    break
                end += 1
            sl = df.iloc[start:end]
            ts_list = sl['_ts'].astype(float).tolist()
            rec = {
                'uid': user,
                'timestamp': ts_list[0],
                'window_end': ts_list[-1],
                'sample_count': len(ts_list),
                'timestamps': ts_list,
                'x': sl['x'].astype(float).tolist(),
                'y': sl['y'].astype(float).tolist(),
                'z': sl['z'].astype(float).tolist(),
                'event_id': 447,
                'processed_at': processed_at,
            }
            out.append((self.config.collections.GARMIN_ACCELEROMETER, rec))
            start = end

        return out

    def process_garmin_fit_file(self, user: str, input_file: str, 
                               output_path: Optional[str] = None,
                               types_to_process: Optional[str] = None,
                               separate_types: bool = True,
                               date_time_format: Optional[str] = None) -> bool:
        """
        Process a single Garmin FIT/SDK file to CSV format using the CLI JAR.
        
        Args:
            user: User ID
            input_file: Path to the FIT/SDK file to process
            output_path: Directory where CSV files will be saved
            types_to_process: Comma-separated list of types to process
            separate_types: If True, each data type gets separate file
            date_time_format: Format for timestamps
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Check if JAR file exists (in the core framework)
            jar_path = Path(__file__).parent / "processing" / "load_files" / "fit-processing-cli.jar"
            if not jar_path.exists():
                self.logger.error(f"JAR file not found: {jar_path}")
                return False
            
            # Check if input file exists
            input_path = Path(input_file)
            if not input_path.exists():
                self.logger.error(f"Input file not found: {input_file}")
                return False
            
            # Set output path
            if output_path is None:
                output_path = Path(self.config.paths.data_processed_path) / "garmin" / user
            else:
                output_path = Path(output_path)
            
            output_path.mkdir(parents=True, exist_ok=True)
            
            # The JAR file creates a subdirectory with the FIT file name + _csv_out
            # So we need to look for CSV files in that subdirectory
            fit_filename = input_path.stem  # Get filename without extension
            csv_subdirectory = output_path / f"{fit_filename}_csv_out"
            
            self.logger.info(f"Processing Garmin FIT file: {input_file}")
            
            # Build the Java command
            cmd = [
                "java",
                "-jar",
                str(jar_path),
                str(input_path),
                "--output_file", str(output_path),
                "--output_format", "CSV"
            ]
            
            # Add optional parameters
            if types_to_process:
                cmd.extend(["--types_to_process", types_to_process])
            
            if date_time_format:
                cmd.extend(["--date_time_format", date_time_format])
            
            # Execute the command
            self.logger.info(f"Executing: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            # Debug: Check what the JAR file output
            self.logger.info(f"JAR stdout: {result.stdout}")
            if result.stderr:
                self.logger.info(f"JAR stderr: {result.stderr}")
            self.logger.info(f"JAR return code: {result.returncode}")
            
            if result.returncode == 0:
                self.logger.info(f"Successfully converted FIT to CSV: {input_file}")
                
                # Now process the CSV files and load into MongoDB
                csv_success = self._process_garmin_csv_files(user, csv_subdirectory)
                
                if csv_success:
                    self.logger.info(f"Successfully loaded Garmin data to MongoDB for: {input_file}")
                    return True
                else:
                    self.logger.error(f"Failed to load CSV data to MongoDB for: {input_file}")
                    return False
            else:
                self.logger.error(f"Error processing {input_file}")
                self.logger.error(f"Return code: {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Error output: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Exception processing Garmin file: {e}")
            return False
    
    def _process_garmin_csv_files(self, user: str, csv_directory: Path) -> bool:
        """Process CSV files generated from Garmin FIT files and load into MongoDB."""
        try:
            import pandas as pd
            
            self.logger.info(f"Processing CSV files in directory: {csv_directory}")
            
            if not csv_directory.exists():
                self.logger.error(f"CSV directory not found: {csv_directory}")
                return False
            
            if not csv_directory.is_dir():
                self.logger.error(f"Path is not a directory: {csv_directory}")
                return False
            
            # Debug: List all files in the directory
            all_files = list(csv_directory.glob("*"))
            self.logger.info(f"All files in directory: {[f.name for f in all_files]}")
            
            records = []
            
            # List all CSV files found
            csv_files = list(csv_directory.glob("*.csv"))
            self.logger.info(f"Found {len(csv_files)} CSV files: {[f.name for f in csv_files]}")
            
            for csv_file in csv_files:
                self.logger.info(f"Processing CSV file: {csv_file}")
                try:
                    if "ACCELEROMETER" in csv_file.name:
                        acc_df = pd.read_csv(csv_file)
                        if self.CHUNK_ACCELEROMETER:
                            records.extend(self._build_accelerometer_chunk_records(user, acc_df))
                        else:
                            for row in acc_df.itertuples(index=False):
                                record = self._handle_garmin_accelerometer(user, row)
                                if record:
                                    records.append(record)
                    
                    elif "BBI" in csv_file.name:
                        self.logger.info(f"Processing BBI data for user {user} in file {csv_file}")
                        bbi_df = pd.read_csv(csv_file)
                        for row in bbi_df.itertuples(index=False):
                            record = self._handle_garmin_ibi(user, row)
                            if record:
                                records.append(record)
                    
                    elif "HEART_RATE" in csv_file.name:
                        hr_df = pd.read_csv(csv_file)
                        for row in hr_df.itertuples(index=False):
                            record = self._handle_garmin_hr(user, row)
                            if record:
                                records.append(record)
                    
                    elif "RESPIRATION" in csv_file.name:
                        resp_df = pd.read_csv(csv_file)
                        for row in resp_df.itertuples(index=False):
                            record = self._handle_garmin_respiration(user, row)
                            if record:
                                records.append(record)
                    
                    elif "STEPS" in csv_file.name:
                        steps_df = pd.read_csv(csv_file)
                        for row in steps_df.itertuples(index=False):
                            record = self._handle_garmin_steps(user, row)
                            if record:
                                records.append(record)
                    
                    elif "STRESS" in csv_file.name:
                        stress_df = pd.read_csv(csv_file)
                        for row in stress_df.itertuples(index=False):
                            record = self._handle_garmin_stress(user, row)
                            if record:
                                records.append(record)
                    
                    else:
                        self.logger.info(f"Skipping unrecognized file: {csv_file.name}")
                
                except pd.errors.EmptyDataError:
                    self.logger.info(f"Skipping empty CSV file: {csv_file.name}")
                except Exception as e:
                    self.logger.error(f"Error reading {csv_file.name}: {e}")
            
            # Group records by collection and insert into MongoDB
            self.logger.info(f"Total records created: {len(records)}")
            collections_records = {}
            for record in records:
                if record:
                    collection_name = record[0]
                    record_data = record[1]
                    if collection_name in collections_records:
                        collections_records[collection_name].append(record_data)
                    else:
                        collections_records[collection_name] = [record_data]
            
            self.logger.info(f"Records grouped by collection: {list(collections_records.keys())}")
            for collection_name, record_list in collections_records.items():
                self.logger.info(f"  {collection_name}: {len(record_list)} records")
            
            # Insert records into MongoDB
            for collection_name, record_list in collections_records.items():
                if record_list:
                    try:
                        self.db[collection_name].insert_many(record_list, ordered=False)
                        self.logger.info(f"Inserted {len(record_list)} records into {collection_name}")
                    except Exception as e:
                        self.logger.error(f"Error inserting records into {collection_name}: {e}")
            
            # Clean up CSV files
            try:
                import shutil
                shutil.rmtree(csv_directory)
                self.logger.info(f"Cleaned up CSV directory: {csv_directory}")
            except Exception as e:
                self.logger.error(f"Failed to clean up CSV directory {csv_directory}: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Exception processing Garmin CSV files: {e}")
            return False
    
    def process_phone_data(self, user: str) -> bool:
        """
        Process phone data for a specific user.
        
        Args:
            user: User ID
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            self.logger.info(f"Processing phone data for user: {user}")
            
            # Load phone data from uploads
            self.logger.info(f"Config data_upload_path: {self.config.paths.data_upload_path}")
            
            upload_path = Path(self.config.paths.data_upload_path) / "phone" / user
            self.logger.info(f"Trying path: {upload_path}")
            if not upload_path.exists():
                self.logger.warning(f"Path does not exist: {upload_path}")
                # Try without phone subdirectory for backward compatibility
                upload_path = Path(self.config.paths.data_upload_path) / user
                self.logger.info(f"Trying fallback path: {upload_path}")
                if not upload_path.exists():
                    self.logger.warning(f"Fallback path also does not exist: {upload_path}")
                    self.logger.warning(f"No upload directory found for user: {user}")
                    self.logger.warning(f"Tried paths: {Path(self.config.paths.data_upload_path) / 'phone' / user} and {Path(self.config.paths.data_upload_path) / user}")
                    return False
            else:
                self.logger.info(f"Found upload directory: {upload_path}")
            
            # Process different types of phone data
            self._process_location_data(user, upload_path)
            self._process_sensor_data(user, upload_path)
            
            # Flush any remaining records
            self.flush_records()
            
            self.logger.info(f"Successfully processed phone data for user: {user}")
            return True
            
        except Exception as e:
            self.logger.error(f"Exception processing phone data for {user}: {e}")
            return False
    
    def process_garmin_data(self, user: str) -> bool:
        """
        Process Garmin data for a specific user.
        
        Args:
            user: User ID
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            self.logger.info(f"Processing Garmin data for user: {user}")
            
            # Look for FIT files in the same directory structure as phone data
            upload_path = Path(self.config.paths.data_upload_path) / "phone" / user
            self.logger.info(f"Checking for Garmin files in: {upload_path}")
            
            if not upload_path.exists():
                self.logger.info(f"Primary path does not exist: {upload_path}")
                # Try without phone subdirectory for backward compatibility
                upload_path = Path(self.config.paths.data_upload_path) / user
                self.logger.info(f"Checking fallback path: {upload_path}")
                if not upload_path.exists():
                    self.logger.warning(f"No upload directory found for user: {user}")
                    return False
            else:
                self.logger.info(f"Found upload directory: {upload_path}")
            
            # Find all FIT files for this user
            fit_files = list(upload_path.glob("*.fit"))
            self.logger.info(f"Found {len(fit_files)} FIT files in {upload_path}")
            
            if not fit_files:
                self.logger.info(f"No FIT files found for user: {user}")
                return True  # Not an error if no files exist
            
            processed_count = 0
            for fit_file in fit_files:
                try:
                    self.logger.info(f"Processing Garmin FIT file: {fit_file}")
                    success = self.process_garmin_fit_file(user, str(fit_file))
                    if success:
                        processed_count += 1
                        # Archive the processed file
                        self.archive_file(user, str(fit_file))
                except Exception as e:
                    self.logger.error(f"Error processing FIT file {fit_file}: {e}")
                    continue
            
            self.logger.info(f"Successfully processed {processed_count} Garmin files for user: {user}")
            return True
            
        except Exception as e:
            self.logger.error(f"Exception processing Garmin data for {user}: {e}")
            return False
    
    def _process_location_data(self, user: str, upload_path: Path):
        """Process location/GPS data from iOS database files."""
        # Location data is now processed as part of sensor data from iOS databases
        # This method is kept for backward compatibility but location processing
        # is handled in _process_sensor_data when processing iOS .db files
        self.logger.info(f"Location data processing is handled in sensor data processing for user: {user}")
    
    def _process_sensor_data(self, user: str, upload_path: Path):
        """Process iOS sensor data from SQLite database files."""
        # Look for iOS database files (.db files)
        db_files = list(upload_path.glob("*.db"))

        for file_path in db_files:
            try:
                self.logger.info(f"Processing iOS database file: {file_path}")
                self._process_ios_database(user, file_path)

            except Exception as e:
                self.logger.error(f"Error processing iOS database file {file_path}: {e}")

        # Android .dbr files (raw ingestion, separate code path from iOS .db).
        dbr_files = list(upload_path.glob("*.dbr"))
        for file_path in dbr_files:
            try:
                self.logger.info(f"Processing Android database file: {file_path}")
                self._process_android_database(user, file_path)
            except Exception as e:
                self.logger.error(f"Error processing Android database file {file_path}: {e}")

        # Encrypted Android files (.dbre) are not yet supported. Leave them in place so
        # they're visible if the Android client's ENCRYPT_FILES flag gets turned on.
        for dbre_file in upload_path.glob("*.dbre"):
            self.logger.warning(
                f"Skipping encrypted Android file (decryption not yet supported): {dbre_file}"
            )
    
    def _process_ios_database(self, user: str, db_file: Path):
        """Process iOS SQLite database file containing sensor data."""
        try:
            import sqlite3
            
            # Connect to the SQLite database
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Get all tables in the database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                self.logger.info(f"Processing table: {table_name}")
                
                # Get all records from the table
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                
                for row in rows:
                    try:
                        # Process based on event_id (row[3])
                        if len(row) >= 4:
                            event_id = row[3]
                            self._process_event_by_id(user, row, event_id)
                            
                    except Exception as e:
                        self.logger.error(f"Error processing row in table {table_name}: {e}")
                        continue
            
            conn.close()

            self.logger.info(f"Flushing records for {db_file}...")
            self.flush_records()
            
            # Archive the processed file
            self.archive_file(user, str(db_file))
            
            self.logger.info(f"Successfully processed iOS database: {db_file}")
            
        except Exception as e:
            self.logger.error(f"Error processing iOS database {db_file}: {e}")
    
    def _process_android_database(self, user: str, dbr_file: Path):
        """Process an Android .dbr SQLite file.

        Schema: { event_id, timestamp, data_type, data } where event_id is a
        per-file monotonic row counter, data_type is the sensor event type
        (see EventDefinition.java in the Android sensing framework), and data is a
        UTF-8 JSON blob (or occasionally a bare scalar like "1"). Ingestion is
        raw — payloads are kept as-is, only routed by data_type.
        """
        try:
            import sqlite3

            conn = sqlite3.connect(str(dbr_file))
            cursor = conn.cursor()
            cursor.execute("SELECT event_id, timestamp, data_type, data FROM events")
            rows = cursor.fetchall()

            for row in rows:
                try:
                    if len(row) >= 3:
                        data_type = row[2]
                        self._process_android_event_by_data_type(user, row, data_type)
                except Exception as e:
                    self.logger.error(f"Error processing Android row in {dbr_file}: {e}")
                    continue

            conn.close()

            self.logger.info(f"Flushing records for {dbr_file}...")
            self.flush_records()

            self.archive_file(user, str(dbr_file))

            self.logger.info(f"Successfully processed Android database: {dbr_file}")

        except Exception as e:
            self.logger.error(f"Error processing Android database {dbr_file}: {e}")

    def _process_android_event_by_data_type(self, user: str, row, data_type):
        """Process Android event based on data_type (the sensor event type)."""
        try:
            if data_type == 2:      # Location
                self._process_android_location_record(user, row)
            elif data_type == 9:    # WiFi scan
                self._process_android_wifi_record(user, row)
            elif data_type == 10:   # Bluetooth scan
                self._process_android_bluetooth_record(user, row)
            elif data_type == 11:   # Services started
                self._process_android_services_started_record(user, row)
            elif data_type == 22:   # App usage
                self._process_android_app_usage_record(user, row)
            elif data_type == 91:   # WiFi connected
                self._process_android_wifi_connected_record(user, row)
            elif data_type == 136:  # Screen event
                self._process_android_screen_event_record(user, row)
            elif data_type == 171:  # Battery level
                self._process_android_battery_record(user, row)
            elif data_type == 199:  # Running services
                self._process_android_running_services_record(user, row)
            elif data_type == 200:  # Accelerometer
                self._process_android_accelerometer_record(user, row)
            elif data_type == 201:  # Activity recognition
                self._process_android_activity_record(user, row)
            elif data_type == 202:  # Step count
                self._process_android_steps_record(user, row)
            elif data_type == 210:  # Call log
                self._process_android_calllog_record(user, row)
            elif data_type == 211:  # SMS log
                self._process_android_smslog_record(user, row)
            elif data_type == 301:  # Notification
                self._process_android_notification_record(user, row)
            elif data_type == 902:  # Location ping
                self._process_android_location_ping_record(user, row)
            else:
                # Unknown data_type - store in generic Android collection
                self._process_android_unknown_event_record(user, row, data_type)

        except Exception as e:
            self.logger.error(f"Error processing Android data_type {data_type}: {e}")

    def _decode_android_payload(self, data_blob):
        """Decode an Android ``.dbr`` data column blob and JSON-parse when possible.

        Returns the parsed value (dict / list / scalar), the raw text if JSON
        parsing fails, or None for empty/null blobs. See
        ``docs/ANDROID_SCHEMA_DESIGN.md`` for the schema this feeds into.
        """
        if isinstance(data_blob, (bytes, bytearray, memoryview)):
            data_text = bytes(data_blob).decode("utf-8", errors="ignore")
        elif data_blob is None:
            data_text = ""
        else:
            data_text = str(data_blob)

        try:
            return json.loads(data_text) if data_text else None
        except (json.JSONDecodeError, ValueError):
            return data_text

    def _android_base_record(self, user, row):
        """Build the reserved-field portion of an android document.

        Returns ``(payload, base)`` where ``payload`` is the decoded sensor
        data and ``base`` carries ``uid``, ``event_id``, ``timestamp``,
        ``data_type``, ``processed_at``. Callers add payload fields on top
        per the rules in ``docs/ANDROID_SCHEMA_DESIGN.md``.
        """
        event_id, timestamp, data_type, data_blob = row
        payload = self._decode_android_payload(data_blob)
        base = {
            'uid': user,
            'event_id': int(event_id) if event_id is not None else None,
            'timestamp': float(timestamp) if timestamp is not None else None,
            'data_type': int(data_type),
            'processed_at': datetime.now().timestamp(),
        }
        return payload, base

    def _process_android_location_record(self, user: str, row):
        """Process Android location record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_LOCATION, record)
        except Exception as e:
            self.logger.error(f"Error processing Android location record: {e}")

    def _process_android_location_ping_record(self, user: str, row):
        """Process Android location ping record (scalar payload dropped, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            _, record = self._android_base_record(user, row)
            self.add_record(self.config.collections.ANDROID_LOCATION_PING, record)
        except Exception as e:
            self.logger.error(f"Error processing Android location_ping record: {e}")

    def _process_android_wifi_record(self, user: str, row):
        """Process Android WiFi scan record (wrapper key lowercased, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update({k.lower(): v for k, v in payload.items()})
            self.add_record(self.config.collections.ANDROID_WIFI, record)
        except Exception as e:
            self.logger.error(f"Error processing Android wifi record: {e}")

    def _process_android_wifi_connected_record(self, user: str, row):
        """Process Android WiFi connected/disconnected record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_WIFI_CONNECTED, record)
        except Exception as e:
            self.logger.error(f"Error processing Android wifi_connected record: {e}")

    def _process_android_bluetooth_record(self, user: str, row):
        """Process Android Bluetooth scan record (wrapper key lowercased, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update({k.lower(): v for k, v in payload.items()})
            self.add_record(self.config.collections.ANDROID_BLUETOOTH, record)
        except Exception as e:
            self.logger.error(f"Error processing Android bluetooth record: {e}")

    def _process_android_screen_event_record(self, user: str, row):
        """Process Android screen event record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_SCREEN_EVENT, record)
        except Exception as e:
            self.logger.error(f"Error processing Android screen_event record: {e}")

    def _process_android_battery_record(self, user: str, row):
        """Process Android battery level record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_BATTERY, record)
        except Exception as e:
            self.logger.error(f"Error processing Android battery record: {e}")

    def _process_android_activity_record(self, user: str, row):
        """Process Android activity recognition record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_ACTIVITY, record)
        except Exception as e:
            self.logger.error(f"Error processing Android activity record: {e}")

    def _process_android_steps_record(self, user: str, row):
        """Process Android step count record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_STEPS, record)
        except Exception as e:
            self.logger.error(f"Error processing Android steps record: {e}")

    def _process_android_accelerometer_record(self, user: str, row):
        """Process Android accelerometer record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_ACCELEROMETER, record)
        except Exception as e:
            self.logger.error(f"Error processing Android accelerometer record: {e}")

    def _process_android_app_usage_record(self, user: str, row):
        """Process Android app usage record (wrapper key lowercased, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update({k.lower(): v for k, v in payload.items()})
            self.add_record(self.config.collections.ANDROID_APP_USAGE, record)
        except Exception as e:
            self.logger.error(f"Error processing Android app_usage record: {e}")

    def _process_android_calllog_record(self, user: str, row):
        """Process Android call log record (raw array wrapped under ``calls``, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if payload is not None:
                record['calls'] = payload
            self.add_record(self.config.collections.ANDROID_CALLLOG, record)
        except Exception as e:
            self.logger.error(f"Error processing Android calllog record: {e}")

    def _process_android_smslog_record(self, user: str, row):
        """Process Android SMS log record (raw array wrapped under ``messages``, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if payload is not None:
                record['messages'] = payload
            self.add_record(self.config.collections.ANDROID_SMSLOG, record)
        except Exception as e:
            self.logger.error(f"Error processing Android smslog record: {e}")

    def _process_android_notification_record(self, user: str, row):
        """Process Android notification record (flat schema, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_NOTIFICATION, record)
        except Exception as e:
            self.logger.error(f"Error processing Android notification record: {e}")

    def _process_android_services_started_record(self, user: str, row):
        """Process Android services_started lifecycle record (scalar payload dropped, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            _, record = self._android_base_record(user, row)
            self.add_record(self.config.collections.ANDROID_SERVICES_STARTED, record)
        except Exception as e:
            self.logger.error(f"Error processing Android services_started record: {e}")

    def _process_android_running_services_record(self, user: str, row):
        """Process Android running_services heartbeat record (flat schema; wrapper already lowercase, see docs/ANDROID_SCHEMA_DESIGN.md)."""
        try:
            payload, record = self._android_base_record(user, row)
            if isinstance(payload, dict):
                record.update(payload)
            self.add_record(self.config.collections.ANDROID_RUNNING_SERVICES, record)
        except Exception as e:
            self.logger.error(f"Error processing Android running_services record: {e}")

    def _process_android_unknown_event_record(self, user: str, row, data_type):
        """Process Android event with an unrecognized data_type.

        Special-cased: payload is kept nested under ``data`` since the shape is
        unknown and flattening could collide with reserved field names. See
        docs/ANDROID_SCHEMA_DESIGN.md for the investigation workflow when this
        collection has entries.
        """
        try:
            payload, record = self._android_base_record(user, row)
            record['data_type'] = int(data_type)
            record['data'] = payload
            self.add_record(self.config.collections.ANDROID_UNKNOWN_EVENTS, record)
        except Exception as e:
            self.logger.error(f"Error processing Android unknown event record (data_type={data_type}): {e}")

    def _process_event_by_id(self, user: str, row, event_id):
        """Process event based on event_id."""
        try:
            # Core sensor events
            if event_id in [152, 151]:  # Location events
                self._process_location_record(user, row)
            elif event_id == 16:  # Activity events
                self._process_activity_record(user, row)
            elif event_id == 21:  # Steps events
                self._process_steps_record(user, row)
            elif event_id in [11, 111]:  # Battery events
                self._process_battery_record(user, row)
            elif event_id in [18, 181]:  # WiFi events
                self._process_wifi_record(user, row)
            elif event_id == 19:  # Bluetooth events
                self._process_bluetooth_record(user, row)
            elif event_id == 13:  # Brightness events
                self._process_brightness_record(user, row)
            elif event_id == 14:  # Lock/Unlock events
                self._process_lock_unlock_record(user, row)
            elif event_id == 447:  # Accelerometer events
                self._process_accelerometer_record(user, row)
            elif event_id == 23:  # Call log events
                self._process_calllog_record(user, row)
            elif event_id == 442:  # Garmin heart rate events
                self._process_garmin_hr_record(user, row)
            elif event_id == 443:  # Garmin stress events
                self._process_garmin_stress_record(user, row)
            elif event_id == 501:  # App usage events
                self._process_app_usage_record(user, row)
            elif event_id == 502:  # EMA response events
                self._process_ema_response_record(user, row)
            elif event_id == 503:  # EMA status events
                self._process_ema_status_record(user, row)
            elif event_id == 504:  # Notification events
                self._process_notification_record(user, row)
            else:
                # Unknown event_id - store in generic collection
                self._process_unknown_event_record(user, row, event_id)
                
        except Exception as e:
            self.logger.error(f"Error processing event_id {event_id}: {e}")
    
    def _process_location_record(self, user: str, row):
        """Process location record from iOS database."""
        try:
            # Row structure: [uuid1, uuid2, timestamp, event_id, event_data]
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                event_id = row[3]
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'latitude': float(event_data.get('latitude', 0)),
                    'longitude': float(event_data.get('longitude', 0)),
                    'accuracy': float(event_data.get('accuracy', 0)),
                    'altitude': float(event_data.get('altitude', 0)),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_LOCATION, record)
                
        except Exception as e:
            self.logger.error(f"Error processing location record: {e}")
    
    def _process_activity_record(self, user: str, row):
        """Process activity record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = row[4].decode("utf-8") if isinstance(row[4], bytes) else str(row[4])
                
                # Parse activity data (format: "activity1 activity2,confidence")
                split_event = event_data.split(',')
                activities = split_event[0].split(' ')[:-1] if len(split_event) > 0 else []
                confidence = split_event[1] if len(split_event) > 1 else 0
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[3],
                    'activity': activities,
                    'confidence': confidence,
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_ACTIVITY, record)
                
        except Exception as e:
            self.logger.error(f"Error processing activity record: {e}")
    
    def _process_steps_record(self, user: str, row):
        """Process steps record from iOS database."""
        try:
            if len(row) >= 4:
                start_timestamp = self._handle_timestamp_format(row[2])
                event_data = row[4].decode("utf-8") if isinstance(row[4], bytes) else str(row[4])
                
                # Parse steps data (format: "end_timestamp,steps,distance,floors_ascended,floors_descended")
                split_event = event_data.split(',')
                
                record = {
                    'uid': user,
                    'timestamp': start_timestamp,
                    'start_timestamp': start_timestamp,
                    'event_id': row[3],
                    'end_timestamp': self._handle_timestamp_format(split_event[0]) if len(split_event) > 0 else start_timestamp,
                    'steps': int(split_event[1]) if len(split_event) > 1 else 0,
                    'distance': float(split_event[2]) if len(split_event) > 2 else 0,
                    'floors_ascended': float(split_event[3]) if len(split_event) > 3 else 0,
                    'floors_descended': float(split_event[4]) if len(split_event) > 4 else 0,
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_STEPS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing steps record: {e}")
    
    def _process_battery_record(self, user: str, row):
        """Process battery record from iOS database."""
        try:
            # Row structure: [uuid1, uuid2, timestamp, event_id, event_data]
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                event_id = row[3]
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'processed_at': datetime.now().timestamp()
                }
                
                # Add battery fields if they exist
                if 'battery_left' in event_data:
                    record['battery_left'] = int(event_data.get('battery_left', 0))
                if 'battery_state' in event_data:
                    record['battery_state'] = int(event_data.get('battery_state', 0))
                
                self.add_record(self.config.collections.IOS_BATTERY, record)
                
        except Exception as e:
            self.logger.error(f"Error processing battery record: {e}")
    
    def _process_wifi_record(self, user: str, row):
        """Process WiFi record from iOS database."""
        try:
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                event_id = row[3]
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'processed_at': datetime.now().timestamp()
                }
                
                # Handle different WiFi event types
                if event_id == 18:
                    record['bssid'] = event_data.get('bssid', '')
                    record['ssid'] = event_data.get('ssid', '')
                elif event_id == 181:
                    record['wifi_enabled'] = int(event_data.get('wifi_enabled', 0))
                    if 'wifi_connected' in event_data:
                        record['wifi_connected'] = int(event_data.get('wifi_connected', 0))
                
                self.add_record(self.config.collections.IOS_WIFI, record)
                
        except Exception as e:
            self.logger.error(f"Error processing WiFi record: {e}")
    
    def _process_bluetooth_record(self, user: str, row):
        """Process Bluetooth record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[3],
                    'bt_address': event_data.get('bt_address', ''),
                    'bt_rssi': int(event_data.get('bt_rssi', 0)),
                    'bt_name': event_data.get('bt_name', ''),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_BLUETOOTH, record)
                
        except Exception as e:
            self.logger.error(f"Error processing Bluetooth record: {e}")
    
    def _process_brightness_record(self, user: str, row):
        """Process brightness record from iOS database."""
        try:
            # Row structure: [uuid1, uuid2, timestamp, event_id, event_data]
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                event_id = row[3]
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'brightness': float(event_data.get('brightness', 0)),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_BRIGHTNESS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing brightness record: {e}")
    
    def _process_lock_unlock_record(self, user: str, row):
        """Process lock/unlock record from iOS database."""
        try:
            # Row structure: [uuid1, uuid2, timestamp, event_id, event_data]
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                event_id = row[3]
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'lock_state': int(event_data.get('LockState', 0)),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_LOCK_UNLOCK, record)
                
        except Exception as e:
            self.logger.error(f"Error processing lock/unlock record: {e}")
    
    def _process_accelerometer_record(self, user: str, row):
        """Process accelerometer record from iOS database."""
        try:
            if len(row) >= 4:
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': self._handle_timestamp_format(event_data.get('timestamp', row[2])),
                    'event_id': row[2],
                    'x': float(event_data.get('x', 0)),
                    'y': float(event_data.get('y', 0)),
                    'z': float(event_data.get('z', 0)),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_ACCELEROMETER, record)
                
        except Exception as e:
            self.logger.error(f"Error processing accelerometer record: {e}")
    
    def _process_calllog_record(self, user: str, row):
        """Process call log record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[2],
                    'call_timestamp': self._handle_timestamp_format(event_data.get('timestamp', 0)),
                    'callId': str(event_data.get('callId', '')),
                    'callType': str(event_data.get('callType', '')),
                    'duration': float(event_data.get('duration', 0)),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.IOS_CALLLOG, record)
                
        except Exception as e:
            self.logger.error(f"Error processing call log record: {e}")
    
    def _process_garmin_hr_record(self, user: str, row):
        """Process Garmin heart rate record from iOS database."""
        try:
            if len(row) >= 4:
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': self._handle_timestamp_format(event_data.get('timestamp', row[2])),
                    'event_id': row[2],
                    'heart_rate': float(event_data.get('heart_rate', 0)),
                    'status': str(event_data.get('status', '')),
                    'device': str(event_data.get('device', '')),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.GARMIN_HR, record)
                
        except Exception as e:
            self.logger.error(f"Error processing Garmin HR record: {e}")
    
    def _process_garmin_stress_record(self, user: str, row):
        """Process Garmin stress record from iOS database."""
        try:
            if len(row) >= 4:
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': self._handle_timestamp_format(event_data.get('timestamp', row[2])),
                    'event_id': row[2],
                    'stress': float(event_data.get('stress', 0)),
                    'status': str(event_data.get('status', '')),
                    'device': str(event_data.get('device', '')),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.GARMIN_STRESS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing Garmin stress record: {e}")
    
    def _process_app_usage_record(self, user: str, row):
        """Process app usage record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[2],
                    'appName': str(event_data.get('appName', '')),
                    'status': str(event_data.get('status', '')),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.APP_USAGE_LOGS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing app usage record: {e}")
    
    def _process_ema_response_record(self, user: str, row):
        """Process EMA response record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[2],
                    'ema_id': str(event_data.get('ema_id', '')),
                    'questions': event_data.get('questions', {}),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.EMA_RESPONSE, record)
                
        except Exception as e:
            self.logger.error(f"Error processing EMA response record: {e}")
    
    def _process_ema_status_record(self, user: str, row):
        """Process EMA status record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[2],
                    'ema_id': str(event_data.get('ema_id', '')),
                    'status': str(event_data.get('status', '')),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.EMA_STATUS_EVENTS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing EMA status record: {e}")
    
    def _process_notification_record(self, user: str, row):
        """Process notification record from iOS database."""
        try:
            if len(row) >= 4:
                timestamp = self._handle_timestamp_format(row[2])
                event_data = json.loads(row[4].decode("utf-8")) if isinstance(row[4], bytes) else row[4]
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': row[2],
                    'notification_id': str(event_data.get('notification_id', '')),
                    'status': str(event_data.get('status', '')),
                    'expectedScheduledTime': event_data.get('expectedScheduledTime', ''),
                    'type': event_data.get('type', ''),
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.NOTIFICATION_EVENTS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing notification record: {e}")
    
    def _process_unknown_event_record(self, user: str, row, event_id):
        """Process unknown event record from iOS database."""
        try:
            # Row structure: [uuid1, uuid2, timestamp, event_id, event_data]
            if len(row) >= 5:
                timestamp = self._handle_timestamp_format(row[2])
                
                record = {
                    'uid': user,
                    'timestamp': timestamp,
                    'event_id': event_id,
                    'raw_data': str(row[4]) if len(row) > 4 else '',
                    'processed_at': datetime.now().timestamp()
                }
                
                self.add_record(self.config.collections.UNKNOWN_EVENTS, record)
                
        except Exception as e:
            self.logger.error(f"Error processing unknown event record: {e}")
    
    def _handle_timestamp_format(self, timestamp):
        """Handle different timestamp formats."""
        str_time = str(timestamp)
        if len(str_time.split(".")[0]) == 10:
            return float(timestamp)
        else:
            return float(timestamp) / 1000
    
    # Garmin data handling methods
    def _handle_garmin_accelerometer(self, user: str, row):
        """Handle Garmin accelerometer data."""
        try:
            record = {
                'uid': user,
                'x': row.x,
                'y': row.y,
                'z': row.z,
                'event_id': 447,
                'timestamp': row.timestamp + row.micros / 1_000_000,
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_ACCELEROMETER, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin accelerometer: {e}")
            return None
    
    def _handle_garmin_ibi(self, user: str, row):
        """Handle Garmin IBI (Inter-Beat Interval) data."""
        try:
            record = {
                'uid': user,
                'timestamp': row.timestamp + row.millis / 1_000,
                'bbi': row.bbi,
                'event_id': 441,
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_IBI, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin IBI: {e}")
            return None
    
    def _handle_garmin_hr(self, user: str, row):
        """Handle Garmin heart rate data."""
        try:
            record = {
                'uid': user,
                'event_id': 442,
                'timestamp': row.timestamp,
                'heart_rate': float(row.bpm),
                'status': str(row.status),
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_HR, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin heart rate: {e}")
            return None
    
    def _handle_garmin_respiration(self, user: str, row):
        """Handle Garmin respiration data."""
        try:
            record = {
                'uid': user,
                'event_id': 444,
                'respiration_timestamp': row.timestamp,
                'respiration': float(row.breathsPerMinute),
                'status': str(row.respirationStatus),
                'timestamp': row.timestamp,
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_RESPIRATION, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin respiration: {e}")
            return None
    
    def _handle_garmin_steps(self, user: str, row):
        """Handle Garmin steps data."""
        try:
            record = {
                'uid': user,
                'event_id': 445,
                'timestamp': row.startTimestamp,
                'start_timestamp': row.startTimestamp,
                'steps_timestamp': row.endTimestamp,
                'steps': float(row.stepCount),
                'total_steps': float(row.totalSteps),
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_STEPS, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin steps: {e}")
            return None
    
    def _handle_garmin_stress(self, user: str, row):
        """Handle Garmin stress data."""
        try:
            record = {
                'uid': user,
                'event_id': 443,
                'timestamp': row.timestamp,
                'heart_rate': float(row.stressScore),
                'status': str(row.stressStatus),
                'average_stress_intensity': float(row.averageStressIntensity),
                'body_battery': float(row.bodyBattery),
                'body_battery_status': str(row.bodyBatteryStatus),
                'processed_at': datetime.now().timestamp()
            }
            return self.config.collections.GARMIN_STRESS, record
        except Exception as e:
            self.logger.error(f"Error processing Garmin stress: {e}")
            return None
    

    
    def generate_daily_summaries(self, date: Optional[str] = None, force_user: Optional[str] = None) -> bool:
        """
        Generate daily summaries for all users or a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format (if None, processes today from midnight to now)
            force_user: Force processing for specific user (bypasses login check)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if date is None:
                # For cron jobs, process today from midnight to now
                end_time = datetime.now()
                target_date = end_time.date()
                # Use midnight timestamp for consistency (same as manual runs)
                start_timestamp = int(datetime.combine(target_date, datetime.min.time()).timestamp())
                end_timestamp = int(end_time.timestamp())
            else:
                # For manual runs, process the entire day
                target_date = datetime.strptime(date, "%Y-%m-%d").date()
                start_timestamp = int(datetime.combine(target_date, datetime.min.time()).timestamp())
                end_timestamp = int(datetime.combine(target_date, datetime.max.time()).timestamp())
            
            self.logger.info(f"Generating summaries for {target_date} (from {start_timestamp} to {end_timestamp})")
            
            # Get users - only those who have logged in (have device_login_time)
            if force_user:
                # Force processing for specific user
                users = [{'uid': force_user}]
                self.logger.info(f"Force processing for user: {force_user}")
            else:
                # Only process users who have logged in (have device_login_time)
                users = list(self.db['users'].find({
                    '$or': [
                        {'ios_login_time': {'$exists': True}},
                        {'android_login_time': {'$exists': True}},
                        {'garmin_login_time': {'$exists': True}}
                    ]
                }, {'uid': 1}))
                self.logger.info(f"Found {len(users)} users with login time")
            
            for user_doc in users:
                uid = user_doc['uid']
                self._generate_user_daily_summary(uid, start_timestamp, end_timestamp, target_date)
            
            self.logger.info(f"Successfully generated summaries for {target_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"Exception generating daily summaries: {e}")
            return False
    
    def generate_summaries_for_period(self, days_back: int = 7, force_user: Optional[str] = None) -> bool:
        """
        Generate daily summaries for the last N days and today up to now.
        
        Args:
            days_back: Number of days to go back (default: 7)
            force_user: Force processing for specific user (bypasses login check)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Generating summaries for last {days_back} days and today...")
            
            success = True
            for i in range(days_back):
                days_ago = i
                if days_ago == 0:
                    # Today - generate from midnight to now (use cronjob logic)
                    self.logger.info(f"Generating summary for today (midnight to now)...")
                    success &= self.generate_daily_summaries(force_user=force_user)  # No date = cronjob mode
                else:
                    # Previous days - generate full day (midnight to 11:59 PM)
                    target_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                    self.logger.info(f"Generating summary for {target_date}...")
                    success &= self.generate_daily_summaries(date=target_date, force_user=force_user)
            
            if success:
                self.logger.info(f"Successfully generated summaries for last {days_back} days")
            else:
                self.logger.error("Failed to generate some summaries")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Exception generating summaries for period: {e}")
            return False
    
    def generate_user_plots(self, uid: str, date_str: str) -> Dict[str, str]:
        """
        Generate plots for a specific user and date on-the-fly.
        
        Args:
            uid: User ID
            date_str: Date in MM-DD-YY format
            
        Returns:
            Dictionary with plot HTML strings
        """
        try:
            # Parse date
            current_date = datetime.strptime(date_str, "%m-%d-%y")
            start_timestamp = int(current_date.timestamp())
            end_timestamp = int((current_date + timedelta(days=1)).timestamp())
            
            # Generate daily plot
            daily_plot_html = self._generate_daily_plot(uid, current_date)
            
            # Generate weekly trends
            weekly_trends_html = self._generate_weekly_trends(uid, current_date)
            
            # Log for debugging
            self.logger.info(f"Generated plots for {uid} on {date_str}")
            self.logger.info(f"Daily plot length: {len(daily_plot_html)}")
            self.logger.info(f"Weekly trends length: {len(weekly_trends_html)}")
            
            return {
                'daily_plot': daily_plot_html,
                'weekly_trends': weekly_trends_html
            }
            
        except Exception as e:
            self.logger.error(f"Error generating plots for {uid} on {date_str}: {e}")
            # Return a simple test plot to verify the system works
            test_plot = """
            <div id="test-plot" style="width:100%; height:400px;">
                <script>
                    var data = [{
                        x: [1, 2, 3, 4],
                        y: [10, 11, 12, 13],
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: 'Test Data'
                    }];
                    
                    var layout = {
                        title: 'Test Plot - Plotly is working!',
                        xaxis: {title: 'X Axis'},
                        yaxis: {title: 'Y Axis'}
                    };
                    
                    Plotly.newPlot('test-plot', data, layout);
                </script>
            </div>
            """
            return {
                'daily_plot': f"<p>Error generating plot: {str(e)}</p>{test_plot}",
                'weekly_trends': f"<p>Error generating trends: {str(e)}</p>"
            }
    
    def _generate_daily_plot(self, uid: str, current_date: datetime) -> str:
        """Generate daily plot showing location, HR, and stress data."""
        try:
            # Create a fixed timeline for 24 hours with 1-second intervals
            fixed_times = [current_date + timedelta(seconds=i) for i in range(86400)]
            
            # Get location availability (30-minute windows)
            location_availability = self._get_location_availability(uid, current_date)
            location_values = [0] * len(fixed_times)
            
            for i in range(len(location_availability)):
                if location_availability[i] == 1:
                    for j in range(i * 1800, (i + 1) * 1800):
                        if j < len(location_values):
                            location_values[j] = 1
            
            # Get HR data
            hr_values, hr_times = self._get_hr_data(uid, current_date)
            
            # Get stress availability
            stress_availability = self._get_stress_availability(uid, current_date)
            stress_values = [0] * len(fixed_times)
            
            for i in range(len(stress_availability)):
                if stress_availability[i] == 1:
                    for j in range(i * 1800, (i + 1) * 1800):
                        if j < len(stress_values):
                            stress_values[j] = 2
            
            # Create plot
            fig = go.Figure()
            
            # Add location availability plot
            fig.add_trace(go.Scatter(
                x=fixed_times, 
                y=location_values, 
                mode='lines', 
                name='Location Availability',
                line=dict(color='blue', width=2)
            ))
            
            # Add HR data plot
            if hr_times and hr_values:
                fig.add_trace(go.Scatter(
                    x=hr_times, 
                    y=hr_values, 
                    mode='markers', 
                    name='Heart Rate',
                    yaxis='y2',
                    marker=dict(color='red', size=4)
                ))
            
            # Add stress data plot
            fig.add_trace(go.Scatter(
                x=fixed_times, 
                y=stress_values, 
                mode='lines', 
                name='Garmin On',
                line=dict(color='green', width=2)
            ))

            # Android phone metrics (only added when the user has android data for the day).
            android_steps, android_step_times = self._get_android_steps_timeline(uid, current_date)
            if android_step_times:
                fig.add_trace(go.Bar(
                    x=android_step_times,
                    y=android_steps,
                    name='Android Steps',
                    yaxis='y2',
                    marker=dict(color='purple'),
                    opacity=0.6
                ))

            android_battery, android_battery_times = self._get_android_battery_timeline(uid, current_date)
            if android_battery_times:
                fig.add_trace(go.Scatter(
                    x=android_battery_times,
                    y=android_battery,
                    mode='lines',
                    name='Android Battery (%)',
                    yaxis='y2',
                    line=dict(color='orange', width=1, dash='dot')
                ))

            # Generate x-axis tick values and labels
            tick_vals = [current_date + timedelta(hours=i) for i in range(25)]
            tick_labels = [f"{i:02}:00" for i in range(24)] + ["00:00"]
            
            # Update layout
            fig.update_layout(
                title=f'Daily Activity for {uid} on {current_date.strftime("%m-%d-%y")}',
                xaxis=dict(
                    title='Time',
                    tickmode='array',
                    tickvals=tick_vals,
                    ticktext=tick_labels,
                    range=[fixed_times[0], fixed_times[-1]]
                ),
                yaxis=dict(
                    title='Availability',
                    side='left',
                    range=[0, 3]
                ),
                yaxis2=dict(
                    title='HR (BPM) / Steps / Battery (%)',
                    overlaying='y',
                    side='right'
                ),
                legend=dict(
                    x=0,
                    y=1.1,
                    orientation='h'
                ),
                height=500,
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
            return fig.to_html(full_html=False, include_plotlyjs=False)
            
        except Exception as e:
            self.logger.error(f"Error generating daily plot: {e}")
            return f"<p>Error generating daily plot: {str(e)}</p>"
    
    def _generate_weekly_trends(self, uid: str, current_date: datetime) -> str:
        """Generate weekly trends showing daily summaries."""
        try:
            # Get 7 days of data
            end_date = current_date
            start_date = end_date - timedelta(days=6)
            
            dates = []
            hr_durations = []
            stress_durations = []
            location_durations = []
            android_steps = []
            android_screen_events = []

            # Get daily summaries
            daily_summaries = list(self.db[self.config.collections.DAILY_SUMMARY].find({
                'uid': uid,
                'date': {
                    '$gte': int(start_date.timestamp()),
                    '$lte': int(end_date.timestamp())
                }
            }).sort('date', 1))

            for summary in daily_summaries:
                date_obj = datetime.fromtimestamp(summary['date'])
                dates.append(date_obj.strftime('%m-%d'))

                # Get location duration from nested structure (already iOS+android combined)
                location_data = summary.get('location', {})
                location_durations.append(location_data.get('duration_hours', 0))

                # Get Garmin durations
                hr_durations.append(summary.get('garmin_wear_duration', 0))
                stress_durations.append(summary.get('garmin_on_duration', 0))

                # Get android phone activity rollups (zero for iOS-only users / older summaries)
                android = summary.get('android_activity', {}) or {}
                android_steps.append(int(android.get('steps', 0) or 0))
                android_screen_events.append(int(android.get('screen_events', 0) or 0))

            # Create plot — two y-axes: hours on the left for duration bars, raw counts on the right
            # for android steps / screen events (so the order-of-magnitude difference doesn't squash them).
            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=dates, y=location_durations,
                name='Location Duration (h)', marker_color='blue'
            ))
            fig.add_trace(go.Bar(
                x=dates, y=hr_durations,
                name='Heart Rate Duration (h)', marker_color='red'
            ))
            fig.add_trace(go.Bar(
                x=dates, y=stress_durations,
                name='Stress Duration (h)', marker_color='green'
            ))

            # Android count-based traces — only add when at least one day has data
            if any(android_steps):
                fig.add_trace(go.Scatter(
                    x=dates, y=android_steps,
                    name='Android Steps',
                    mode='lines+markers',
                    yaxis='y2',
                    line=dict(color='purple', width=2)
                ))
            if any(android_screen_events):
                fig.add_trace(go.Scatter(
                    x=dates, y=android_screen_events,
                    name='Android Screen Events',
                    mode='lines+markers',
                    yaxis='y2',
                    line=dict(color='orange', width=2, dash='dot')
                ))

            fig.update_layout(
                title=f'Weekly Trends for {uid}',
                xaxis_title='Date',
                yaxis=dict(title='Duration (Hours)'),
                yaxis2=dict(title='Android Counts', overlaying='y', side='right'),
                barmode='group',
                height=400,
                margin=dict(l=50, r=50, t=80, b=50),
                legend=dict(orientation='h', y=1.1)
            )

            return fig.to_html(full_html=False, include_plotlyjs=False)
            
        except Exception as e:
            self.logger.error(f"Error generating weekly trends: {e}")
            return f"<p>Error generating weekly trends: {str(e)}</p>"
    
    def _get_location_availability(self, uid: str, current_date: datetime) -> List[int]:
        """Get location availability for 30-minute windows (1 if either iOS or android has GPS data)."""
        start_time = int(current_date.timestamp())
        end_time = int((current_date + timedelta(days=1)).timestamp())

        location_available = []
        start_window = start_time

        while start_window < end_time:
            end_window = start_window + 1800  # 30 minutes
            ts_window = {'$gte': start_window, '$lt': end_window}

            ios_count = self.db[self.config.collections.IOS_LOCATION].count_documents({
                'uid': uid, 'event_id': 152, 'timestamp': ts_window
            })
            android_count = 0
            if ios_count == 0:
                # Only query android collection when iOS has nothing, to keep cost low for iOS-only users
                android_count = self.db[self.config.collections.ANDROID_LOCATION].count_documents({
                    'uid': uid, 'timestamp': ts_window
                })

            location_available.append(1 if (ios_count > 0 or android_count > 0) else 0)
            start_window = end_window

        return location_available
    
    def _get_hr_data(self, uid: str, current_date: datetime) -> Tuple[List[float], List[datetime]]:
        """Get heart rate data for the day."""
        start_time = int(current_date.timestamp())
        end_time = int((current_date + timedelta(days=1)).timestamp())
        
        hr_data = list(self.db[self.config.collections.GARMIN_HR].find({
            'uid': uid,
            'timestamp': {'$gte': start_time, '$lt': end_time}
        }).sort('timestamp', 1))
        
        hr_values = []
        hr_times = []
        
        for record in hr_data:
            hr_values.append(float(record.get('heart_rate', 0)))
            hr_times.append(datetime.fromtimestamp(record['timestamp']))
        
        return hr_values, hr_times
    
    def _get_android_steps_timeline(self, uid: str, current_date: datetime) -> Tuple[List[int], List[datetime]]:
        """Return (steps_per_interval, interval_times) for android step data on a given day.

        Each android_steps doc represents a reporting interval (with ``steps`` =
        steps reported in that interval). Returns a sparse timeline anchored on
        each doc's ``timestamp``, suitable for a Plotly bar/scatter trace.
        """
        start_time = int(current_date.timestamp())
        end_time = int((current_date + timedelta(days=1)).timestamp())

        try:
            docs = list(self.db[self.config.collections.ANDROID_STEPS].find({
                'uid': uid,
                'timestamp': {'$gte': start_time, '$lt': end_time}
            }, {'timestamp': 1, 'steps': 1}).sort('timestamp', 1))
        except Exception as e:
            self.logger.error(f"Error querying android steps timeline for {uid}: {e}")
            return [], []

        steps = [int(d.get('steps', 0) or 0) for d in docs]
        times = [datetime.fromtimestamp(d['timestamp']) for d in docs]
        return steps, times

    def _get_android_battery_timeline(self, uid: str, current_date: datetime) -> Tuple[List[int], List[datetime]]:
        """Return (battery_level_pct, sample_times) for android battery data on a given day."""
        start_time = int(current_date.timestamp())
        end_time = int((current_date + timedelta(days=1)).timestamp())

        try:
            docs = list(self.db[self.config.collections.ANDROID_BATTERY].find({
                'uid': uid,
                'timestamp': {'$gte': start_time, '$lt': end_time}
            }, {'timestamp': 1, 'level': 1}).sort('timestamp', 1))
        except Exception as e:
            self.logger.error(f"Error querying android battery timeline for {uid}: {e}")
            return [], []

        levels = [int(d.get('level', 0) or 0) for d in docs]
        times = [datetime.fromtimestamp(d['timestamp']) for d in docs]
        return levels, times

    def _get_stress_availability(self, uid: str, current_date: datetime) -> List[int]:
        """Get stress availability for 30-minute windows."""
        start_time = int(current_date.timestamp())
        end_time = int((current_date + timedelta(days=1)).timestamp())
        
        stress_available = []
        start_window = start_time
        
        while start_window < end_time:
            end_window = start_window + 1800  # 30 minutes
            count = self.db[self.config.collections.GARMIN_STRESS].count_documents({
                'uid': uid,
                'timestamp': {'$gte': start_window, '$lt': end_window}
            })
            stress_available.append(1 if count > 0 else 0)
            start_window = end_window
        
        return stress_available
    
    def _generate_user_daily_summary(self, uid: str, start_timestamp: int, 
                                   end_timestamp: int, target_date: datetime.date):
        """Generate daily summary for a specific user.

        Phone metrics (``location.*``) are populated from whichever platform has
        data: iOS via ``ios_location``, android via ``android_location``. Both are
        also broken out into ``location_ios`` / ``location_android`` sub-documents
        for per-platform breakdowns. Android phone activity counts live under
        ``android_activity`` (steps, battery samples, screen events, etc.).
        """
        try:
            # Get location data — iOS and android tracked separately, summed for the
            # device-agnostic "phone location" view that the core dashboard reads.
            ios_distance, ios_duration = self._get_location_info(uid, start_timestamp, end_timestamp)
            android_distance, android_duration = self._get_android_location_info(
                uid, start_timestamp, end_timestamp
            )

            # Get Garmin data
            garmin_wear_duration, garmin_on_duration = self._get_garmin_info(uid, start_timestamp, end_timestamp)

            # Get EMA data
            ema_info = self._get_ema_info(uid, start_timestamp, end_timestamp)

            # Get android phone activity rollups (steps, battery, screen, ...)
            android_activity = self._get_android_phone_activity(uid, start_timestamp, end_timestamp)

            # Create summary document
            summary = {
                'uid': uid,
                'date': start_timestamp,
                'date_str': target_date.strftime("%Y-%m-%d"),
                'location': {
                    # Device-agnostic phone location: sum because a single user is
                    # expected to upload from at most one phone OS per day.
                    'distance_traveled': ios_distance + android_distance,
                    'duration_hours': ios_duration + android_duration,
                },
                'location_ios': {
                    'distance_traveled': ios_distance,
                    'duration_hours': ios_duration,
                },
                'location_android': {
                    'distance_traveled': android_distance,
                    'duration_hours': android_duration,
                },
                'garmin_wear_duration': garmin_wear_duration,
                'garmin_on_duration': garmin_on_duration,
                'android_activity': android_activity,
                'ema': ema_info,
                'generated_at': datetime.now().timestamp()
            }
            
            # Save to database (upsert to avoid duplicates)
            # Use the standardized timestamp (midnight of the day) to prevent duplicates
            self.db[self.config.collections.DAILY_SUMMARY].update_one(
                {'uid': uid, 'date': start_timestamp},
                {'$set': summary},
                upsert=True
            )
            
            self.logger.info(f"Generated daily summary for {uid} on {target_date}")
            
        except Exception as e:
            self.logger.error(f"Error generating daily summary for {uid}: {e}")
    
    def _get_location_info(self, uid: str, start_timestamp: int, end_timestamp: int) -> tuple:
        """Get location information for a user."""
        try:
            # Get GPS records (event_id: 152 for location data)
            gps_records = list(self.db[self.config.collections.IOS_LOCATION].find({
                'uid': uid, 
                'event_id': 152,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }).sort('timestamp', 1))
            
            if not gps_records:
                return 0.0, 0.0
            
            # Calculate distance traveled
            distance_traveled = self._calculate_distance_traveled(gps_records)
            
            # Calculate duration using the same method as backend_scripts
            gps_count = 0
            previous_time = 0
            gps_minutes = 0
            
            for gps in gps_records:
                if gps['timestamp'] - previous_time < 15 * 60:  # 15 minutes threshold
                    gps_minutes += float(gps['timestamp'] - previous_time) / 60
                    gps_count += 1
                previous_time = gps['timestamp']
            
            duration_hours = float(gps_minutes) / 60
            
            return distance_traveled, duration_hours
            
        except Exception as e:
            self.logger.error(f"Error getting location info for {uid}: {e}")
            return 0.0, 0.0
    
    def _calculate_distance_traveled(self, gps_records: List[Dict]) -> float:
        """Calculate total distance traveled from GPS records (iOS schema: lowercase ``latitude``/``longitude``)."""
        return self._calculate_distance_from_records(gps_records, 'latitude', 'longitude')

    def _calculate_distance_from_records(self, gps_records: List[Dict], lat_key: str, lon_key: str) -> float:
        """Distance from any GPS-like records, parameterized by lat/lon field names.

        Used for both iOS (lowercase keys, see ``_calculate_distance_traveled``) and
        android (UPPERCASE keys per the flat android schema in
        ``docs/ANDROID_SCHEMA_DESIGN.md``).
        """
        try:
            if len(gps_records) < 2:
                return 0.0

            total_distance = 0.0
            for i in range(1, len(gps_records)):
                prev = gps_records[i-1]
                curr = gps_records[i]

                try:
                    dist = distance.distance(
                        (prev.get(lat_key, 0), prev.get(lon_key, 0)),
                        (curr.get(lat_key, 0), curr.get(lon_key, 0))
                    ).meters
                    total_distance += dist
                except Exception:
                    continue

            return total_distance

        except Exception as e:
            self.logger.error(f"Error calculating distance: {e}")
            return 0.0

    def _get_android_location_info(self, uid: str, start_timestamp: int, end_timestamp: int) -> tuple:
        """Get android location info (distance traveled, duration in hours) for a user.

        Mirrors :py:meth:`_get_location_info` but reads from ``android_location``
        and uses UPPERCASE ``LATITUDE``/``LONGITUDE`` fields per the flat android
        schema (see ``docs/ANDROID_SCHEMA_DESIGN.md``). Returns ``(0.0, 0.0)`` when
        the user has no android GPS data for the window.
        """
        try:
            gps_records = list(self.db[self.config.collections.ANDROID_LOCATION].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }).sort('timestamp', 1))

            if not gps_records:
                return 0.0, 0.0

            distance_traveled = self._calculate_distance_from_records(
                gps_records, 'LATITUDE', 'LONGITUDE'
            )

            # Duration: same 15-minute-gap heuristic as the iOS path
            previous_time = 0
            gps_minutes = 0.0
            for gps in gps_records:
                if gps['timestamp'] - previous_time < 15 * 60:
                    gps_minutes += float(gps['timestamp'] - previous_time) / 60
                previous_time = gps['timestamp']

            return distance_traveled, float(gps_minutes) / 60

        except Exception as e:
            self.logger.error(f"Error getting android location info for {uid}: {e}")
            return 0.0, 0.0

    def _get_android_phone_activity(self, uid: str, start_timestamp: int, end_timestamp: int) -> Dict:
        """Get android phone-activity rollups for the daily summary.

        Returns a dict with: ``steps``, ``battery_samples``, ``screen_events``,
        ``app_usage_uploads``, ``wifi_scans``, ``running_services_pings``.
        All counts are scoped to ``[start_timestamp, end_timestamp)``.
        """
        try:
            ts_window = {'$gte': start_timestamp, '$lt': end_timestamp}

            # Steps: sum across all android_steps docs (each doc is one reporting interval)
            steps_total = 0
            for doc in self.db[self.config.collections.ANDROID_STEPS].find(
                {'uid': uid, 'timestamp': ts_window}, {'steps': 1}
            ):
                steps_total += int(doc.get('steps', 0) or 0)

            battery_samples = self.db[self.config.collections.ANDROID_BATTERY].count_documents(
                {'uid': uid, 'timestamp': ts_window}
            )
            screen_events = self.db[self.config.collections.ANDROID_SCREEN_EVENT].count_documents(
                {'uid': uid, 'timestamp': ts_window}
            )
            app_usage_uploads = self.db[self.config.collections.ANDROID_APP_USAGE].count_documents(
                {'uid': uid, 'timestamp': ts_window}
            )
            wifi_scans = self.db[self.config.collections.ANDROID_WIFI].count_documents(
                {'uid': uid, 'timestamp': ts_window}
            )
            running_services_pings = self.db[self.config.collections.ANDROID_RUNNING_SERVICES].count_documents(
                {'uid': uid, 'timestamp': ts_window}
            )

            return {
                'steps': steps_total,
                'battery_samples': battery_samples,
                'screen_events': screen_events,
                'app_usage_uploads': app_usage_uploads,
                'wifi_scans': wifi_scans,
                'running_services_pings': running_services_pings,
            }

        except Exception as e:
            self.logger.error(f"Error getting android phone activity for {uid}: {e}")
            return {
                'steps': 0, 'battery_samples': 0, 'screen_events': 0,
                'app_usage_uploads': 0, 'wifi_scans': 0, 'running_services_pings': 0,
            }
    
    def _get_garmin_info(self, uid: str, start_timestamp: int, end_timestamp: int) -> tuple:
        """Get Garmin device information for a user."""
        try:
            # Get Garmin heart rate records (indicates device is worn)
            # Only count records where heart rate value is > 0
            garmin_hr_records = list(self.db[self.config.collections.GARMIN_HR].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            # Filter for records with heart rate > 0 in Python (more efficient than non-indexed query)
            garmin_hr_count = sum(1 for record in garmin_hr_records if record.get('heart_rate', 0) > 0)
            
            # Get Garmin stress records (indicates device is on and monitoring)
            garmin_stress_count = self.db[self.config.collections.GARMIN_STRESS].count_documents({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            })
            
            # Calculate durations using the same method as backend_scripts:
            # - Garmin wear duration: based on heart rate records (6-minute intervals)
            # - Garmin on duration: based on stress records (6-minute intervals)
            garmin_wear_duration = float(garmin_hr_count) / (6 * 60)  # Convert to hours
            garmin_on_duration = float(garmin_stress_count) / (6 * 60)  # Convert to hours
            
            self.logger.info(f"Garmin info for {uid}: {garmin_stress_count} stress records = {garmin_on_duration:.2f} hours")
            
            return garmin_wear_duration, garmin_on_duration
            
        except Exception as e:
            self.logger.error(f"Error getting Garmin info for {uid}: {e}")
            return 0.0, 0.0
    
    def _get_sensor_info(self, uid: str, start_timestamp: int, end_timestamp: int) -> float:
        """Get sensor information for a user."""
        try:
            # Get various sensor data types
            activity_records = list(self.db['activity_data'].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            steps_records = list(self.db['steps_data'].find({
                'uid': uid,
                'start_timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            battery_records = list(self.db['battery_data'].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            # Calculate total sensor activity duration
            # This is a simplified calculation - in practice you might want more sophisticated logic
            total_records = len(activity_records) + len(steps_records) + len(battery_records)
            
            # Assume each record represents some time period (e.g., 1 minute)
            total_duration_minutes = total_records
            
            return total_duration_minutes / 60  # Convert to hours
            
        except Exception as e:
            self.logger.error(f"Error getting sensor info for {uid}: {e}")
            return 0.0
    
    def _get_ema_info(self, uid: str, start_timestamp: int, end_timestamp: int) -> Dict:
        """Get EMA information for a user."""
        try:
            # Get EMA responses
            ema_responses = list(self.db['ema_data'].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            # Get scheduled EMAs
            scheduled_emas = list(self.db['ema_schedule'].find({
                'uid': uid,
                'timestamp': {'$gte': start_timestamp, '$lt': end_timestamp}
            }))
            
            return {
                'responses': ema_responses,
                'scheduled': scheduled_emas,
                'response_count': len(ema_responses),
                'scheduled_count': len(scheduled_emas)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting EMA info for {uid}: {e}")
            return {'responses': [], 'scheduled': [], 'response_count': 0, 'scheduled_count': 0}
    

    
    def generate_plots(self, user: str, date: str) -> bool:
        """
        Generate plots for a specific user and date.
        
        Args:
            user: User ID
            date: Date in YYYY-MM-DD format
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Generating plots for {user} on {date}")
            
            # Create plots directory
            plots_dir = Path(self.config.paths.static_dir) / user
            plots_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate different types of plots
            self._generate_location_plot(user, date, plots_dir)
            self._generate_sensor_plot(user, date, plots_dir)
            self._generate_ema_plot(user, date, plots_dir)
            
            self.logger.info(f"Successfully generated plots for {user} on {date}")
            return True
            
        except Exception as e:
            self.logger.error(f"Exception generating plots for {user}: {e}")
            return False
    
    def _generate_location_plot(self, user: str, date: str, plots_dir: Path):
        """Generate location plot."""
        try:
            # This would use a plotting library like matplotlib or plotly
            # For now, we'll create a placeholder
            plot_content = f"""
            <html>
            <head><title>Location Data - {user} - {date}</title></head>
            <body>
            <h1>Location Data for {user} on {date}</h1>
            <p>Location visualization would be generated here.</p>
            </body>
            </html>
            """
            
            plot_file = plots_dir / f"{date}.html"
            with open(plot_file, 'w') as f:
                f.write(plot_content)
                
        except Exception as e:
            self.logger.error(f"Error generating location plot for {user}: {e}")
    
    def _generate_sensor_plot(self, user: str, date: str, plots_dir: Path):
        """Generate sensor plot."""
        try:
            plot_content = f"""
            <html>
            <head><title>Sensor Data - {user} - {date}</title></head>
            <body>
            <h1>Sensor Data for {user} on {date}</h1>
            <p>Sensor visualization would be generated here.</p>
            </body>
            </html>
            """
            
            plot_file = plots_dir / "sensor" / f"{date}.html"
            plot_file.parent.mkdir(exist_ok=True)
            with open(plot_file, 'w') as f:
                f.write(plot_content)
                
        except Exception as e:
            self.logger.error(f"Error generating sensor plot for {user}: {e}")
    
    def _generate_ema_plot(self, user: str, date: str, plots_dir: Path):
        """Generate EMA plot."""
        try:
            plot_content = f"""
            <html>
            <head><title>EMA Data - {user} - {date}</title></head>
            <body>
            <h1>EMA Data for {user} on {date}</h1>
            <p>EMA visualization would be generated here.</p>
            </body>
            </html>
            """
            
            plot_file = plots_dir / "ema" / f"{date}.html"
            plot_file.parent.mkdir(exist_ok=True)
            with open(plot_file, 'w') as f:
                f.write(plot_content)
                
        except Exception as e:
            self.logger.error(f"Error generating EMA plot for {user}: {e}")
    



def process_all_data():
    """Process all data for all users."""
    processor = DataProcessor()
    
    # Get all users
    users = processor.db['users'].find({}, {'uid': 1})
    
    for user_doc in users:
        uid = user_doc['uid']
        # Process phone data
        processor.process_phone_data(uid)
        # Process Garmin data
        processor.process_garmin_data(uid)
    
    # Generate daily summaries
    processor.generate_daily_summaries()


def generate_all_summaries():
    """Generate daily summaries for all users."""
    processor = DataProcessor()
    processor.generate_daily_summaries()


def process_garmin_files():
    """Process all Garmin FIT files."""
    # Set the STUDY_CONFIG_FILE environment variable if not already set
    if 'STUDY_CONFIG_FILE' not in os.environ:
        # Try to find the config file in common locations
        from pathlib import Path
        
        # Look for config file in current directory or parent directories
        current_dir = Path.cwd()
        config_file = None
        
        # Check current directory
        if (current_dir / "config" / "study_config.json").exists():
            config_file = current_dir / "config" / "study_config.json"
        # Check parent directories
        else:
            for parent in current_dir.parents:
                if (parent / "config" / "study_config.json").exists():
                    config_file = parent / "config" / "study_config.json"
                    break
        
        if config_file:
            os.environ['STUDY_CONFIG_FILE'] = str(config_file)
            print(f"Set STUDY_CONFIG_FILE to: {config_file}")
            
            # Reload the configuration with the new file
            from study_framework_core.core.config import set_config_file
            set_config_file(str(config_file))
        else:
            print("Warning: Could not find study_config.json. Using default configuration.")
    
    processor = DataProcessor()
    
    # Get all users
    users = processor.db['users'].find({}, {'uid': 1})
    
    for user_doc in users:
        uid = user_doc['uid']
        # Use the proper method that handles archiving and user-specific processing
        processor.process_garmin_data(uid)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Study Framework Data Processing')
    parser.add_argument('--action', choices=['process_data', 'generate_summaries', 'process_garmin'], 
                       required=True, help='Action to perform')
    parser.add_argument('--user', help='Specific user to process')
    parser.add_argument('--date', help='Specific date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.action == 'process_data':
        if args.user:
            processor = DataProcessor()
            processor.process_phone_data(args.user)
        else:
            process_all_data()
    elif args.action == 'generate_summaries':
        if args.date:
            processor = DataProcessor()
            processor.generate_daily_summaries(args.date)
        else:
            # For cronjobs, process last 7 days + today
            processor = DataProcessor()
            processor.generate_summaries_for_period(days_back=7)
    elif args.action == 'process_garmin':
        process_garmin_files()
