from flask import Flask, render_template, request
import os
from data_processing import parse_csv_file, group_by_subject_area

app = Flask(__name__, static_url_path='/static')

current_directory = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def index():
    csv_file_path = os.path.join(current_directory, 'data.csv')
    parsed_data = parse_csv_file(csv_file_path)
    grouped_data = group_by_subject_area(parsed_data)
    
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
    
    # Render the index.html template with the processed data and data status
    return render_template('index.html', grouped_data=grouped_data, data_status=data_status)


@app.route('/dag_status')
def dag_status():
    subject_area = request.args.get('subject_area')
    
    if subject_area is None:
        # If subject_area is not provided, return an error response
        return 'Error: No subject area provided', 400
    
    # Read data from the CSV file and filter by the subject area
    csv_file_path = os.path.join(current_directory, 'data.csv')
    parsed_data = parse_csv_file(csv_file_path)
    dag_data = [row for row in parsed_data if row['SUBJECT_AREA'] == subject_area]
    
    # Generate HTML content for the DAG status data
    html_content = "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>DAG Status</title>"
    html_content += "<link href='https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/3.6.0/mdb.min.css' rel='stylesheet'></head><body>"
    html_content += "<style>.table {border-collapse: separate; border-spacing: 0 10px; width: 100%;} .table th, .table td {border: 2px solid #000; padding: 8px;} .table th {background-color: #f2f2f2;}</style>"
    html_content += "<table class='table'><thead><tr><th>DAG Name</th><th>Start Date</th><th>End Date</th><th>Elapsed Time</th><th>Status</th></tr></thead><tbody>"
    for dag in dag_data:
        # Set row color based on status
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


@app.route('/about_us')
def about_us():
    return render_template('about_us.html')

if __name__ == '__main__':
    app.run(debug=True)
