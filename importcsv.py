import csv

def parse_csv_file(csv_file_path):
    data = []
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def group_by_subject_area(data):
    grouped_data = {}
    for row in data:
        subject_area = row['SUBJECT_AREA']
        if subject_area not in grouped_data:
            grouped_data[subject_area] = []
        grouped_data[subject_area].append(row)
    return grouped_data

def generate_html_for_subject_areas(grouped_data):
    html_content = "<div id='subjectAreas'>\n"
    for subject_area, data in grouped_data.items():
        latest_modified = max(data, key=lambda x: x['MODIFIED_TS'])['MODIFIED_TS']
        html_content += f"<div class='subjectAreaBox' data-subject-area='{subject_area}'>\n"
        html_content += f"<h3>{subject_area}</h3>\n"
        html_content += f"Last Modified: {latest_modified}\n"
        html_content += "</div>\n"
    html_content += "</div>"
    return html_content

# Example usage
if __name__ == "__main__":
    csv_file_path = 'your_file.csv'  # Update with the path to your CSV file
    parsed_data = parse_csv_file(csv_file_path)
    grouped_data = group_by_subject_area(parsed_data)
    html_content = generate_html_for_subject_areas(grouped_data)
    print(html_content)