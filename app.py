from flask import Flask, render_template, jsonify, request
import vertica_python
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import time
import os
import pytz
from cryptography.fernet import Fernet
from flask_caching import Cache
import psutil
from functools import wraps
import gc
import traceback
from pathlib import Path
import requests
from defusedxml.ElementTree import fromstring

MONITORED_PACKAGES = [
    "pandas",
    "pydantic",
    "numpy",
    "requests",
    "flask",
    "sqlalchemy",
    "pytest",
    "django",
    "tensorflow",
    "pytorch",
    "scikit-learn",
    "matplotlib",
    "seaborn",
    "beautifulsoup4",
    "fastapi",
    "celery",
    "redis",
    "psycopg2-binary",
    "boto3",
    "pillow",
    "opencv-python"
]


# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-please-change')

# Add Cache Configuration
cache_config = {
    "CACHE_TYPE": "SimpleCache",  # Simple cache for single worker/process
    "CACHE_DEFAULT_TIMEOUT": 300,  # 5 minutes default timeout
    "CACHE_THRESHOLD": 1000,  # Maximum number of items the cache will store
    "CACHE_KEY_PREFIX": "dashboard_"  # Add prefix for easier identification
}

app.config.from_mapping(cache_config)
cache = Cache(app)

# Initialize last cleanup time
last_cleanup_time = time.time()
CLEANUP_INTERVAL = 300  # 5 minutes between cleanup checks
MEMORY_THRESHOLD = 500  # MB - adjust based on your server capacity

# Create logs directory structure
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Configure logging with both file and console handlers
def setup_logging():
    logger = logging.getLogger('dashboard')
    logger.setLevel(logging.DEBUG)
    
    # Format for detailed logging
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] '
        '[%(filename)s:%(lineno)d] '
        '%(message)s - '
        'Function: %(funcName)s'
    )
    
    # Daily rotating file handler for general logs
    daily_handler = TimedRotatingFileHandler(
        log_dir / 'dashboard.log',
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    daily_handler.setFormatter(formatter)
    daily_handler.setLevel(logging.INFO)
    
    # Error log file handler
    error_handler = RotatingFileHandler(
        log_dir / 'error.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    
    # Console handler for development
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    logger.handlers.clear()
    
    # Add all handlers
    logger.addHandler(daily_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

@app.before_request
def log_request_info():
    """Log information about each incoming request"""
    logger.debug(
        'Request: %s %s\nHeaders: %s\nBody: %s',
        request.method,
        request.url,
        dict(request.headers),
        request.get_data().decode('utf-8')
    )

@app.after_request
def log_response_info(response):
    """Log information about each response"""
    logger.debug(
        'Response: Status: %s\nHeaders: %s',
        response.status,
        dict(response.headers)
    )
    return response

def get_memory_usage():
    """Get current memory usage of the application"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # Convert to MB

def should_cleanup_cache():
    """Check if cache cleanup is needed based on memory usage"""
    memory_usage = get_memory_usage()
    return memory_usage > MEMORY_THRESHOLD

def cleanup_cache():
    """Perform cache cleanup and garbage collection"""
    try:
        start_time = time.time()
        initial_memory = get_memory_usage()
        
        # Clear the cache
        cache.clear()
        
        # Force garbage collection
        gc.collect()
        
        end_memory = get_memory_usage()
        duration = time.time() - start_time
        
        logger.info(
            f"Cache cleanup completed in {duration:.2f} seconds. "
            f"Memory reduced from {initial_memory:.2f}MB to {end_memory:.2f}MB"
        )
    except Exception as e:
        logger.error(f"Error during cache cleanup: {str(e)}")

def monitor_cache(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        global last_cleanup_time
        
        # Check if cleanup is needed
        current_time = time.time()
        if current_time - last_cleanup_time > CLEANUP_INTERVAL:
            if should_cleanup_cache():
                cleanup_cache()
                last_cleanup_time = current_time
        
        return f(*args, **kwargs)
    return decorated_function

# Load the secret key
def load_key():
    """Load the secret key from file"""
    try:
        logger.debug("Attempting to load secret key")
        with open("secret.key", "rb") as key_file:
            key = key_file.read()
        logger.debug("Secret key loaded successfully")
        return key
    except Exception as e:
        logger.error("Error loading secret key: %s", str(e), exc_info=True)
        raise

# Decrypt the environment variables
def decrypt_env():
    """Decrypt environment variables using the secret key"""
    try:
        start_time = time.time()
        logger.debug("Starting environment decryption")
        
        key = load_key()
        f = Fernet(key)
        
        config = {}
        with open('.env', 'r') as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                    
                try:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        key_name, encrypted_value = parts
                        key_name = key_name.strip()
                        encrypted_value = encrypted_value.strip()
                        if key_name and encrypted_value:
                            decrypted_value = f.decrypt(encrypted_value.encode()).decode()
                            config[key_name] = decrypted_value
                except Exception as e:
                    logger.error(f"Error processing line '{line}': {e}")
                    continue
        
        duration = time.time() - start_time
        logger.debug("Environment decryption completed in %.3f seconds", duration)
        return config
    except Exception as e:
        logger.error(f"Error in decrypt_env: {str(e)}", exc_info=True)
        raise

# Get decrypted database configuration
DB_CONFIG = decrypt_env()

def get_db_connection():
    """Create a Vertica database connection."""
    start_time = time.time()
    try:
        logger.debug("Attempting database connection to %s:%s", 
                    DB_CONFIG['HOST'], DB_CONFIG['PORT'])
        
        conn_info = {
            'host': DB_CONFIG['HOST'],
            'port': int(DB_CONFIG['PORT']),
            'user': DB_CONFIG['USER'],
            'password': DB_CONFIG['PASSWORD'],
            'database': DB_CONFIG['DATABASE'],
            'tlsmode': 'disable'
        }
        
        conn = vertica_python.connect(**conn_info)
        duration = time.time() - start_time
        logger.info("Database connection established in %.2f seconds", duration)
        return conn
    except Exception as e:
        logger.error("Database connection error: %s\n%s", 
                    str(e), traceback.format_exc())
        raise

@cache.memoize(timeout=300)
def get_grouped_data():
    """Get grouped DAG data from database."""
    start_time = time.time()
    query_start_time = None
    try:
        logger.debug("Starting get_grouped_data function")
        start_memory = get_memory_usage()
        
        conn = get_db_connection()
        cursor = conn.cursor('dict')
        
        query = """
            SELECT 
                SUBJECT_AREA,
                DAG_NAME,
                STATUS,
                DAG_START_TIME,
                DAG_END_TIME,
                MODIFIED_TS,
                ELAPSED_TIME
            FROM public.dag_data
            ORDER BY 
                SUBJECT_AREA,
                CASE 
                    WHEN MODIFIED_TS IS NULL THEN 1 
                    ELSE 0 
                END,
                MODIFIED_TS DESC
        """
        
        logger.debug("Executing query for DAG data")
        query_start_time = time.time()
        cursor.execute(query)
        query_duration = time.time() - query_start_time
        
        rows = cursor.fetchall()
        logger.info("Retrieved %d records in %.2f seconds", 
                   len(rows), query_duration)
        
        processing_start = time.time()
        grouped_data = {}
        
        for row in rows:
            subject_area = row['SUBJECT_AREA']
            if subject_area not in grouped_data:
                grouped_data[subject_area] = []
            
            grouped_data[subject_area].append({
                'dag_name': row['DAG_NAME'],
                'status': row['STATUS'].lower() if row['STATUS'] else 'yet_to_start',
                'dag_start_time': row['DAG_START_TIME'],
                'dag_end_time': row['DAG_END_TIME'],
                'modified_ts': row['MODIFIED_TS'],
                'elapsed_time': row['ELAPSED_TIME']
            })
        
        end_memory = get_memory_usage()
        total_duration = time.time() - start_time
        processing_duration = time.time() - processing_start
        
        logger.info(
            "Data processing completed:\n"
            "Total time: %.2f seconds\n"
            "Query time: %.2f seconds\n"
            "Processing time: %.2f seconds\n"
            "Memory usage: %.2f MB\n"
            "Subject areas: %d\n"
            "Total records: %d",
            total_duration,
            query_duration,
            processing_duration,
            end_memory - start_memory,
            len(grouped_data),
            len(rows)
        )
        
        return grouped_data
        
    except Exception as e:
        logger.error(
            "Error in get_grouped_data:\n"
            "Error: %s\n"
            "Traceback: %s\n"
            "Query duration: %.2f seconds",
            str(e),
            traceback.format_exc(),
            time.time() - query_start_time if query_start_time else 0
        )
        return {}
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logger.debug("Database connection closed")

@app.route('/')
@monitor_cache
@cache.cached(timeout=60)
def index():
    start_time = time.time()
    try:
        logger.debug("Processing index route request")
        grouped_data = get_grouped_data()
        
        # Convert timestamps to EST for each subject area
        est = pytz.timezone('America/New_York')
        conversion_start = time.time()
        
        for subject_area in grouped_data.values():
            for item in subject_area:
                if item['modified_ts']:
                    modified_ts = item['modified_ts'].astimezone(est)
                    item['modified_ts'] = modified_ts.strftime('%Y-%m-%d %H:%M:%S')
                if item['dag_start_time']:
                    start_time_ts = item['dag_start_time'].astimezone(est)
                    item['dag_start_time'] = start_time_ts.strftime('%Y-%m-%d %H:%M:%S')
                if item['dag_end_time']:
                    end_time_ts = item['dag_end_time'].astimezone(est)
                    item['dag_end_time'] = end_time_ts.strftime('%Y-%m-%d %H:%M:%S')
        
        conversion_time = time.time() - conversion_start
        logger.debug("Timestamp conversion completed in %.2f seconds", conversion_time)
        
        if not grouped_data:
            logger.warning("No data available for dashboard")
        else:
            logger.info(
                "Dashboard data retrieved successfully: %d subject areas",
                len(grouped_data)
            )
        
        current_time = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S')
        total_time = time.time() - start_time
        
        logger.info("Total index route processing time: %.2f seconds", total_time)
        
        return render_template('index.html', 
                             grouped_data=grouped_data,
                             last_update=current_time)
    except Exception as e:
        logger.error(
            "Error in index route:\n"
            "Error: %s\n"
            "Traceback: %s",
            str(e),
            traceback.format_exc()
        )
        return render_template('error.html', 
                             error_title='Dashboard Error',
                             error='Failed to load dashboard data'), 500
@app.route('/package_parser', methods=['GET'])
def package_parser():
    try:
        logger.debug("Processing package_parser route request")
        start_time = time.time()
        
        packages = []
        notifications = []
        est = pytz.timezone('US/Eastern')
        
        for package_name in MONITORED_PACKAGES:
            try:
                package_info = get_package_info(package_name)
                if package_info:
                    packages.append(package_info)
                    
                    # Check if package was recently updated (within last 24 hours)
                    if is_recently_updated(package_info.get('last_update')):
                        notifications.append({
                            'package_name': package_name,
                            'new_version': package_info['version'],
                            'update_time': package_info['last_update']
                        })
            except Exception as e:
                logger.error(f"Error processing package {package_name}: {str(e)}")
                continue
        
        duration = time.time() - start_time
        logger.info(f"Retrieved information for {len(packages)} packages in {duration:.2f} seconds")
        
        return render_template('package_parser.html', 
                             packages=packages,
                             notifications=notifications)
                             
    except Exception as e:
        logger.error(
            "Error in package_parser route:\n"
            f"Error: {str(e)}\n"
            f"Traceback: {traceback.format_exc()}"
        )
        return render_template('error.html',
                             error_title='Package Parser Error',
                             error='Failed to load package information'), 500

def get_package_info(package_name):
    """Get package information from PyPI"""
    try:
        url = f"https://pypi.org/pypi/{package_name}/json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        info = data['info']
        releases = data['releases']
        
        # Get latest version info
        latest_version = info['version']
        latest_release = releases.get(latest_version, [{}])[0]
        
        # Convert upload time to EST
        upload_time = datetime.strptime(
            latest_release.get('upload_time', ''), 
            '%Y-%m-%dT%H:%M:%S'
        )
        est = pytz.timezone('US/Eastern')
        upload_time = upload_time.replace(tzinfo=pytz.utc).astimezone(est)
        
        return {
            'name': package_name,
            'version': latest_version,
            'last_update': upload_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'status': get_package_status(upload_time),
            'has_update': is_recently_updated(upload_time),
            'link': f"https://pypi.org/project/{package_name}/"
        }
    except requests.RequestException as e:
        logger.error(f"Request error for {package_name}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error processing {package_name}: {str(e)}")
        return None

def is_recently_updated(timestamp):
    """Check if package was updated in the last 24 hours"""
    if not timestamp:
        return False
        
    try:
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %Z')
            
        now = datetime.now(pytz.UTC)
        if not timestamp.tzinfo:
            timestamp = pytz.UTC.localize(timestamp)
            
        # Check if update was within last 24 hours
        return (now - timestamp).total_seconds() < 86400  # 24 hours in seconds
    except Exception as e:
        logger.error(f"Error checking update time: {str(e)}")
        return False

def get_package_status(update_time):
    """Determine package status based on last update time"""
    try:
        now = datetime.now(pytz.UTC)
        if not update_time.tzinfo:
            update_time = pytz.UTC.localize(update_time)
            
        days_since_update = (now - update_time).days
        
        if days_since_update < 1:
            return 'Recent'
        elif days_since_update < 30:
            return 'Active'
        elif days_since_update < 180:
            return 'Stable'
        else:
            return 'Inactive'
    except Exception as e:
        logger.error(f"Error determining package status: {str(e)}")
        return 'Unknown'

@app.route('/api/notifications')
def get_notifications():
    """API endpoint for getting package update notifications"""
    try:
        notifications = []
        for package_name in MONITORED_PACKAGES:
            try:
                package_info = get_package_info(package_name)
                if package_info and is_recently_updated(package_info.get('last_update')):
                    notifications.append({
                        'package_name': package_name,
                        'new_version': package_info['version'],
                        'update_time': package_info['last_update']
                    })
            except Exception as e:
                logger.error(f"Error checking updates for {package_name}: {str(e)}")
                continue
                
        return jsonify({'notifications': notifications})
    except Exception as e:
        logger.error(f"Error getting notifications: {str(e)}")
        return jsonify({'error': 'Failed to get notifications'}), 500
    
                    
@app.route('/dag_status')
@monitor_cache
@cache.cached(timeout=30, query_string=True)
def dag_status():
    try:
        start_time = time.time()
        logger.debug("Processing dag_status route request")
        
        subject_area = request.args.get('subject_area')
        status = request.args.get('status', '').lower()
        
        logger.debug("Request parameters - Subject Area: %s, Status: %s",
                    subject_area, status)
        
        if not subject_area or not status:
            logger.warning("Missing parameters in dag_status request")
            return jsonify({'error': 'Missing required parameters'}), 400

        grouped_data = get_grouped_data()
        
        if subject_area not in grouped_data:
            logger.info("No data found for subject area: %s", subject_area)
            return jsonify([])

        filtered_data = [
            {
                'dag_name': item['dag_name'],
                'status': item['status'],
                'dag_start_time': item['dag_start_time'],
                'dag_end_time': item['dag_end_time'],
                'modified_ts': item['modified_ts'],
                'elapsed_time': item['elapsed_time']
            }
            for item in grouped_data[subject_area]
            if item['status'].lower() == status
        ]

        duration = time.time() - start_time
        logger.info("Retrieved %d records for status request in %.2f seconds",
                   len(filtered_data), duration)
        return jsonify(filtered_data)

    except Exception as e:
        logger.error(
            "Error processing status request:\n"
            "Error: %s\n"
            "Traceback: %s",
            str(e),
            traceback.format_exc()
        )
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/logs/status')
def logs_status():
    """Get logging status and recent logs"""
    try:
        # Get last 100 lines from the log file
        log_file = log_dir / 'dashboard.log'
        error_file = log_dir / 'error.log'
        
        recent_logs = []
        if log_file.exists():
            with open(log_file, 'r') as f:
                recent_logs = f.readlines()[-100:]
        
        recent_errors = []
        if error_file.exists():
            with open(error_file, 'r') as f:
                recent_errors = f.readlines()[-50:]
        
        return jsonify({
            'status': 'active',
            'log_directory': str(log_dir),
            'log_files': {
                'dashboard.log': str(log_file),
                'error.log': str(error_file)
            },
            'recent_logs': recent_logs,
            'recent_errors': recent_errors
        })
    except Exception as e:
        logger.error("Error getting logs status: %s", str(e))
        return jsonify({'error': 'Failed to get logs status'}), 500

@app.route('/cache/stats')
def cache_stats():
    """Get cache statistics"""
    try:
        memory_usage = get_memory_usage()
        process = psutil.Process(os.getpid())
        
        stats = {
            'memory_usage_mb': round(memory_usage, 2),
            'cpu_percent': process.cpu_percent(),
            'thread_count': process.num_threads(),
            'last_cleanup': datetime.fromtimestamp(last_cleanup_time).strftime('%Y-%m-%d %H:%M:%S'),
            'cache_cleanup_interval': f"{CLEANUP_INTERVAL/60} minutes",
            'memory_threshold_mb': MEMORY_THRESHOLD
        }
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        return jsonify({'error': 'Failed to get cache statistics'}), 500

if __name__ == '__main__':
    try:
        # Log startup information
        logger.info("Starting application")
        logger.info("Environment: %s", os.environ.get('FLASK_ENV', 'production'))
        
        # Log initial memory usage
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024
        logger.info("Initial memory usage: %.2f MB", memory_usage)
        
        # Get the environment from an environment variable, default to 'development'
        env = os.environ.get('FLASK_ENV', 'development')
        
        # Set debug mode based on environment
        debug_mode = env == 'development'
        
        # Run the application
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=debug_mode
        )
    except Exception as e:
        logger.critical("Failed to start application: %s", str(e), exc_info=True)
        raise
