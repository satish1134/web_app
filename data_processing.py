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