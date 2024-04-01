from flask import Flask, make_response, request
import os
from data_processing import parse_csv_file, group_by_subject_area
from html_generation import generate_html_for_subject_areas

app = Flask(__name__, static_url_path='/static')

current_directory = os.path.dirname(os.path.abspath(__file__))
print("Flask application directory:", current_directory)

@app.route('/')
def index():
    csv_file_path = os.path.join(current_directory, 'data.csv')
    parsed_data = parse_csv_file(csv_file_path)
    grouped_data = group_by_subject_area(parsed_data)
    html_content = generate_html_for_subject_areas(grouped_data)
    
    response = make_response(html_content)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    
    return response

@app.route('/dag_status')
def dag_status():
    subject_area = request.args.get('subject_area')
    
    # Read data from the CSV file and filter by the subject area
    csv_file_path = os.path.join(current_directory, 'data.csv')
    parsed_data = parse_csv_file(csv_file_path)
    dag_data = [row for row in parsed_data if row['SUBJECT_AREA'] == subject_area]
    
    # Generate HTML content for the DAG status data
    html_content = "<table class='table'><thead><tr><th>DAG Name</th><th>Start Date</th><th>End Date</th><th>Elapsed Time</th><th>Status</th></tr></thead><tbody>"
    for dag in dag_data:
        # Set row color based on status
        row_color = 'success' if dag['STATUS'].lower() == 'success' else 'danger'
        html_content += f"<tr class='{row_color}'><td>{dag['DAG_NAME']}</td><td>{dag['DAG_START_TIME']}</td><td>{dag['DAG_END_TIME']}</td><td>{dag['ELAPSED_TIME']}</td><td>{dag['STATUS']}</td></tr>"
    html_content += "</tbody></table>"
    
    return html_content

if __name__ == '__main__':
    app.run(debug=True)
