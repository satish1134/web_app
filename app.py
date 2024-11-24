<<<<<<< HEAD
from flask import Flask, render_template, request, redirect, url_for
import os
from data_processing import parse_csv_file, group_by_subject_area

app = Flask(__name__, static_url_path='/static')

current_directory = os.path.dirname(os.path.abspath(__file__))
print (current_directory)
data_file_path = os.path.join(current_directory, 'latest_update.txt')
print (data_file_path)

# Function to parse CSV file and group data by subject area
def get_grouped_data():
    csv_file_path = os.path.join(current_directory, 'data.csv')
    parsed_data = parse_csv_file(csv_file_path)
    grouped_data = group_by_subject_area(parsed_data)
    return grouped_data

# Route for the home page
@app.route('/')
def index():
    grouped_data = get_grouped_data()
    
    # Define data status
    data_status = {}
    for subject_area, data in grouped_data.items():
        # Check if any DAG in running status
        if any(row['STATUS'].lower() == 'running' for row in data):
            data_status[subject_area] = 'running'
        # Check if any DAG in failed status
        elif any(row['STATUS'].lower() == 'failed' for row in data):
            data_status[subject_area] = 'failed'
        else:
            data_status[subject_area] = 'success'
    
    # Read latest updates from file
    latest_updates = []
    with open(data_file_path, 'r') as file:
        latest_updates = file.readlines()
    
    # Render the index.html template with the processed data, data status, and latest updates
    return render_template('index.html', grouped_data=grouped_data, data_status=data_status, latest_updates=latest_updates)

# Route for the login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Check login credentials
        if request.form['username'] == 'admin' and request.form['password'] == 'admin':
            # Redirect to add_update route upon successful login
            return redirect(url_for('add_update'))
        else:
            return 'Invalid credentials. Please try again.'
    return render_template('login.html')

# Route for adding a new update message
# Route for adding a new update message
@app.route('/add_update', methods=['GET', 'POST'])
def add_update():
    if request.method == 'POST':
        # Get form data
        date = request.form['date']
        subject_area = request.form['subject_area']
        dag_name = request.form['dag_name']
        description = request.form['description']
        eta = request.form['eta']
        
        # Construct the message
        new_message = f"Date: {date}, Subject Area: {subject_area}, DAG Name: {dag_name}, Description: {description}, ETA: {eta}\n"

        try:
            # Save the new message to the text file
            with open(data_file_path, 'w') as file:
                file.write(new_message)
            
            # Print success message to console
            print("Message successfully written to file.")
        except Exception as e:
            # Print error to console
            print(f"Error occurred while writing to file: {e}")
        
        # Redirect to the home page after saving the message
        return redirect(url_for('index'))
        
    return render_template('add_update.html')



@app.route('/dag_status')
def dag_status():
    subject_area = request.args.get('subject_area')
    
    if subject_area is None:
        return 'Error: No subject area provided', 400
    
    grouped_data = get_grouped_data()
    dag_data = [row for row in grouped_data.get(subject_area, [])]
    
    html_content = "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>DAG Status</title>"
    html_content += "<link href='https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/3.6.0/mdb.min.css' rel='stylesheet'></head><body>"
    html_content += "<style>.table {border-collapse: separate; border-spacing: 0 10px; width: 100%;} .table th, .table td {border: 2px solid #000; padding: 8px;} .table th {background-color: #f2f2f2;}</style>"
    html_content += "<table class='table'><thead><tr><th>DAG Name</th><th>Start Date</th><th>End Date</th><th>Elapsed Time</th><th>Status</th></tr></thead><tbody>"
    for dag in dag_data:
        row_color = ''
        if dag['STATUS'].lower() == 'success':
            row_color = 'table-success'
        elif dag['STATUS'].lower() == 'failed':
            row_color = 'table-danger'
        elif dag['STATUS'].lower() == 'running':
            row_color = 'table-warning'
        html_content += f"<tr class='{row_color}'><td>{dag['DAG_NAME']}</td><td>{dag['DAG_START_TIME']}</td><td>{dag['DAG_END_TIME']}</td><td>{dag['ELAPSED_TIME']}</td><td>{dag['STATUS']}</td></tr>"
    html_content += "</tbody></table></body></html>"
    
    return html_content

# Route for the home page
@app.route('/home')
def home():
    return redirect(url_for('index'))

if __name__ == '__main__':
=======
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
    """Render the main dashboard page."""
    try:
        grouped_data = get_grouped_data()
        return render_template('index.html', grouped_data=grouped_data)
    except Exception as e:
        logger.error(f"Error in index route: {e}")
        return render_template('error.html', error="Failed to load dashboard data")

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


@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors."""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return render_template('500.html'), 500

@app.after_request
def add_security_headers(response):
    response.headers['X-Frame-Options'] = 'DENY'
    return response

if __name__ == '__main__':
    try:
        init_db()
        logger.info('Database connection initialized successfully')
    except Exception as e:
        logger.error(f'Failed to initialize database connection: {e}')
    
    # Run the application
>>>>>>> master
    app.run(debug=True)
