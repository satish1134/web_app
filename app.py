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
    app.run(debug=True)
