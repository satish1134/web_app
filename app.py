from flask import Flask, render_template, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging
import os
from cryptography.fernet import Fernet

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-please-change')

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
                # Skip empty lines or comments
                if not line or line.startswith('#'):
                    continue
                    
                try:
                    # Split only on the first occurrence of '='
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        key_name, encrypted_value = parts
                        key_name = key_name.strip()
                        encrypted_value = encrypted_value.strip()
                        # Only process if we have both key and value
                        if key_name and encrypted_value:
                            decrypted_value = f.decrypt(encrypted_value.encode()).decode()
                            config[key_name] = decrypted_value
                except Exception as e:
                    logger.error(f"Error processing line '{line}': {e}")
                    continue
                    
        return config
    except Exception as e:
        logger.error(f"Error in decrypt_env: {e}")
        raise


# Get decrypted database configuration
DB_CONFIG = decrypt_env()

def get_db_connection():
    """Create a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['DATABASE'],
            user=DB_CONFIG['USER'],
            password=DB_CONFIG['PASSWORD'],
            host=DB_CONFIG['HOST'],
            port=DB_CONFIG['PORT'],
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def init_db():
    """Initialize the database connection."""
    try:
        with app.app_context():
            db = get_db_connection()
            # Test the connection
            cursor = db.cursor()
            cursor.execute("SELECT 1 FROM public.dag_data LIMIT 1")
            logger.info("Database connection successful")
            cursor.close()
            db.close()
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

def get_grouped_data():
    """Get grouped DAG data from database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                subject_area,
                dag_name,
                status,
                dag_start_time,
                dag_end_time,
                modified_ts,
                elapsed_time
            FROM public.dag_data
            ORDER BY subject_area, modified_ts DESC
        """)
        
        rows = cursor.fetchall()
        grouped_data = {}
        
        for row in rows:
            subject_area = row['subject_area']
            if subject_area not in grouped_data:
                grouped_data[subject_area] = []
            
            grouped_data[subject_area].append({
                'dag_name': row['dag_name'],
                'status': row['status'],
                'dag_start_time': row['dag_start_time'].strftime('%Y-%m-%d %H:%M:%S') if row['dag_start_time'] else None,
                'dag_end_time': row['dag_end_time'].strftime('%Y-%m-%d %H:%M:%S') if row['dag_end_time'] else None,
                'modified_ts': row['modified_ts'].strftime('%Y-%m-%d %H:%M:%S') if row['modified_ts'] else None,
                'elapsed_time': str(row['elapsed_time']) if row['elapsed_time'] else None
            })
        
        return grouped_data
    except Exception as e:
        logger.error(f"Error fetching grouped data: {e}")
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
        return render_template('index.html', 
                             grouped_data=grouped_data,
                             last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.error(f"Error in index route: {e}")
        return render_template('error.html', 
                             error_title='Dashboard Error',
                             error='Failed to load dashboard data'), 500

@app.route('/dag_status')
def dag_status():
    """Return DAG status data as JSON."""
    try:
        subject_area = request.args.get('subject_area')
        status = request.args.get('status', '').lower()
        
        if not subject_area or not status:
            return jsonify({'error': 'Missing required parameters'}), 400

        grouped_data = get_grouped_data()
        
        if subject_area not in grouped_data:
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

        return jsonify(filtered_data)

    except Exception as e:
        logger.error(f"Error in dag_status route: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/update_status', methods=['POST'])
def update_status():
    """Update DAG status."""
    try:
        data = request.get_json()
        
        if not data or not all(key in data for key in ['dag_name', 'status', 'subject_area']):
            return jsonify({'error': 'Missing required fields'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE public.dag_data 
            SET status = %s, modified_ts = %s
            WHERE dag_name = %s AND subject_area = %s
        """, (
            data['status'],
            datetime.now(),
            data['dag_name'],
            data['subject_area']
        ))
        
        conn.commit()
        return jsonify({'message': 'Status updated successfully'})

    except Exception as e:
        logger.error(f"Error updating status: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', 
                         error_title='Page Not Found (404)',
                         error='The requested page could not be found.'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', 
                         error_title='Internal Server Error (500)',
                         error='An internal server error occurred. Please try again later.'), 500

@app.after_request
def add_security_headers(response):
    """Add security headers to all responses."""
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response

if __name__ == '__main__':
    try:
        init_db()
        logger.info('Database connection initialized successfully')
    except Exception as e:
        logger.error(f'Failed to initialize database connection: {e}')
    
    # Run the application
    app.run(debug=True)
