from flask import Flask, render_template, jsonify, request
import vertica_python
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
from cryptography.fernet import Fernet

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-please-change')

# Configure logging with rotation
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO if not app.debug else logging.DEBUG)
handler = RotatingFileHandler('app.log', maxBytes=10000, backupCount=3)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(handler)

# Add a stream handler for console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(console_handler)

# Load the secret key
def load_key():
    """Load the secret key from file"""
    with open("secret.key", "rb") as key_file:
        return key_file.read()

# Decrypt the environment variables
def decrypt_env():
    """Decrypt environment variables using the secret key"""
    try:
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
                    
        return config
    except Exception as e:
        logger.error(f"Error in decrypt_env")
        raise

# Get decrypted database configuration
DB_CONFIG = decrypt_env()

def get_db_connection():
    """Create a Vertica database connection."""
    try:
        conn_info = {
            'host': DB_CONFIG['HOST'],
            'port': int(DB_CONFIG['PORT']),
            'user': DB_CONFIG['USER'],
            'password': DB_CONFIG['PASSWORD'],
            'database': DB_CONFIG['DATABASE'],
            'tlsmode': 'disable'
        }
        
        conn = vertica_python.connect(**conn_info)
        return conn
    except Exception as e:
        logger.error(f"Database connection error")
        raise

def get_grouped_data():
    """Get grouped DAG data from database."""
    try:
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
        
        logger.info("Fetching DAG data")  # Generic message instead of showing query
        cursor.execute(query)
        
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} records")  # Only log count, not data
        
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
        
        logger.info(f"Processed data for {len(grouped_data)} subject areas")  # Log only counts
        return grouped_data
        
    except Exception as e:
        logger.error("Error fetching grouped data")  # Generic error message
        logger.error(str(e))  # Log only the error message, not the full traceback
        return {}
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



@app.route('/')
def index():
    try:
        grouped_data = get_grouped_data()
        
        if not grouped_data:
            logger.warning("No data available")
        else:
            logger.info("Dashboard data retrieved successfully")
        
        return render_template('index.html', 
                             grouped_data=grouped_data,
                             last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.error("Error loading dashboard")
        return render_template('error.html', 
                             error_title='Dashboard Error',
                             error='Failed to load dashboard data'), 500


@app.route('/dag_status')
def dag_status():
    try:
        subject_area = request.args.get('subject_area')
        status = request.args.get('status', '').lower()
        
        if not subject_area or not status:
            logger.warning("Missing parameters in dag_status request")
            return jsonify({'error': 'Missing required parameters'}), 400

        grouped_data = get_grouped_data()
        
        if subject_area not in grouped_data:
            logger.info(f"No data found for requested subject area")
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

        logger.info(f"Retrieved {len(filtered_data)} records for status request")
        return jsonify(filtered_data)

    except Exception as e:
        logger.error("Error processing status request")
        return jsonify({'error': 'Internal server error'}), 500
    
@app.after_request
def add_security_headers(response):
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response

if __name__ == '__main__':
    
    # Get the environment from an environment variable, default to 'development'
    env = os.environ.get('FLASK_ENV', 'development')
    
    # Set debug mode based on environment
    debug_mode = env == 'development'
    
    # Run the application with environment-appropriate debug setting
    app.run(host='0.0.0.0', port=5000, debug=debug_mode)
