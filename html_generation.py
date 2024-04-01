def generate_html_for_subject_areas(grouped_data):
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Subject Areas</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <link rel="stylesheet" type="text/css" href="/static/styles.css">
        <script>
            function showDagStatus(subjectArea) {
                var data = JSON.parse('[[DATA_FOR_SUBJECT_AREA]]'); // Replace with the actual data

                var table = '<table class="table"><thead><tr><th>DAG Name</th><th>Start Date</th><th>End Date</th><th>Elapsed Time</th><th>Status</th></tr></thead><tbody>';
                for (var i = 0; i < data.length; i++) {
                    table += '<tr>';
                    table += '<td>' + data[i].DAG_NAME + '</td>';
                    table += '<td>' + data[i].DAG_START_TIME + '</td>';
                    table += '<td>' + data[i].DAG_END_TIME + '</td>';
                    table += '<td>' + data[i].ELAPSED_TIME + '</td>';
                    table += '<td>' + data[i].STATUS + '</td>';
                    table += '</tr>';
                }
                table += '</tbody></table>';

                var popup = window.open('', '', 'width=600,height=400');
                popup.document.body.innerHTML = '<h1>DAG Status for ' + subjectArea + '</h1>' + table;
            }
        </script>
    </head>
    <body>

    <div class="container">
        <div class="row">
    """

    for subject_area, data in grouped_data.items():
        latest_modified = max(data, key=lambda x: x['MODIFIED_TS'])['MODIFIED_TS']
        html_content += f"""
            <div class="col-md-4">
                <div class="subjectAreaBox">
                    <h3>{subject_area}</h3>
                    <p>Last Modified: {latest_modified}</p>
                    <button class="open-popup" onclick="showDagStatus('{subject_area}')">></button>
                </div>
            </div>
        """

    html_content += """
        </div>
    </div>

    </body>
    </html>
    """

    return html_content
