import os
import csv
import io
from flask import Flask, request, jsonify, send_file, render_template, abort
from flask_cors import CORS
from clickhouse_driver import Client
from werkzeug.utils import secure_filename

app = Flask(__name__, static_folder='../frontend', template_folder='../frontend')
CORS(app)

# Configuration
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
OUTPUT_FOLDER = os.path.join(os.getcwd(), 'output')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def get_clickhouse_client(config):
    """
    Creates and returns a ClickHouse Client using the configuration.
    Expects config to contain: host, port, database, user, jwt_token, secure flag (optional)
    """
    host = config.get("host")
    port = int(config.get("port", 9000))
    database = config.get("database", "default")
    user = config.get("user", "default")
    jwt_token = config.get("jwt_token")
    secure = config.get("secure", False)

    # Pass the JWT token via headers for authentication if provided
    headers = {}
    if jwt_token:
        headers["Authorization"] = f"Bearer {jwt_token}"
    
    try:
        client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            secure=secure,
            settings={"use_numpy": True},
            http_headers=headers
        )
        return client
    except Exception as e:
        raise Exception(f"Error creating ClickHouse client: {str(e)}")


@app.route('/')
def index():
    # Serve the frontend index.html
    return app.send_static_file('index.html')


@app.route('/api/get_tables', methods=['POST'])
def get_tables():
    """
    Expects JSON:
    {
        "host": "localhost",
        "port": "9000",
        "database": "default",
        "user": "default",
        "jwt_token": "your_jwt_token",
        "secure": false
    }
    Returns list of table names.
    """
    data = request.json
    try:
        client = get_clickhouse_client(data)
        # Get tables from system.tables in the database
        query = "SELECT name FROM system.tables WHERE database = %(database)s"
        result = client.execute(query, {"database": data.get("database", "default")})
        tables = [row[0] for row in result]
        return jsonify({"status": "success", "tables": tables})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route('/api/get_columns', methods=['POST'])
def get_columns():
    """
    Expects JSON:
    {
        "host": "localhost",
        "port": "9000",
        "database": "default",
        "user": "default",
        "jwt_token": "your_jwt_token",
        "secure": false,
        "table": "your_table_name"
    }
    Returns list of columns with name and type.
    """
    data = request.json
    table = data.get("table")
    if not table:
        return jsonify({"status": "error", "message": "Table name is required."}), 400

    try:
        client = get_clickhouse_client(data)
        query = f"DESCRIBE TABLE {table}"
        result = client.execute(query)
        columns = [{"name": row[0], "type": row[1]} for row in result]
        return jsonify({"status": "success", "columns": columns})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route('/api/ingest_clickhouse_to_flatfile', methods=['POST'])
def ingest_clickhouse_to_flatfile():
    """
    Ingests data from ClickHouse to a Flat File (CSV).

    Expects JSON:
    {
        "host": "localhost",
        "port": "9000",
        "database": "default",
        "user": "default",
        "jwt_token": "your_jwt_token",
        "secure": false,
        "table": "your_table_name",
        "columns": ["col1", "col2"],
        "output_file": "output.csv",
        "delimiter": ","
    }
    Returns record count.
    """
    data = request.json
    table = data.get("table")
    columns = data.get("columns")
    output_file = data.get("output_file", "output.csv")
    delimiter = data.get("delimiter", ",")
    
    if not table or not columns:
        return jsonify({"status": "error", "message": "Table name and selected columns are required."}), 400

    try:
        client = get_clickhouse_client(data)
        col_string = ", ".join(columns)
        query = f"SELECT {col_string} FROM {table}"
        rows = client.execute(query, with_column_types=True)
        col_names = [col[0] for col in rows[1]]
        data_rows = rows[0]

        output_path = os.path.join(OUTPUT_FOLDER, secure_filename(output_file))
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            writer.writerow(col_names)
            for row in data_rows:
                writer.writerow(row)

        return jsonify({"status": "success", "record_count": len(data_rows), "output_file": output_file})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route('/api/ingest_flatfile_to_clickhouse', methods=['POST'])
def ingest_flatfile_to_clickhouse():
    """
    Upload a CSV file and ingest it into ClickHouse (creates a new table).
    
    Expects form-data with:
    - file: CSV File
    - host, port, database, user, jwt_token, secure (as form fields)
    - table: target table name to be created
    - columns: comma-separated list of column names (matching CSV header order)
    - column_types: comma-separated list of ClickHouse column types corresponding to the columns
    - delimiter: CSV file delimiter (optional, default is comma)
    """
    try:
        host = request.form.get("host")
        port = request.form.get("port", 9000)
        database = request.form.get("database", "default")
        user = request.form.get("user", "default")
        jwt_token = request.form.get("jwt_token")
        secure = request.form.get("secure", "false").lower() == "true"
        table = request.form.get("table")
        columns = request.form.get("columns")  # e.g., "col1,col2,col3"
        column_types = request.form.get("column_types")  # e.g., "String,Int32,String"
        delimiter = request.form.get("delimiter", ",")

        if not all([host, table, columns, column_types]):
            return jsonify({"status": "error", "message": "Missing required fields."}), 400

        columns_list = [col.strip() for col in columns.split(",")]
        types_list = [typ.strip() for typ in column_types.split(",")]

        if len(columns_list) != len(types_list):
            return jsonify({"status": "error", "message": "Number of columns and types must match."}), 400

        # Save uploaded file
        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "No file part in the request."}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"status": "error", "message": "No selected file."}), 400

        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Connect to ClickHouse
        config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "jwt_token": jwt_token,
            "secure": secure
        }
        client = get_clickhouse_client(config)

        # Create table dynamically (drop if exists for this demo)
        schema_cols = ", ".join([f"{name} {typ}" for name, typ in zip(columns_list, types_list)])
        create_query = f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({schema_cols}) ENGINE = MergeTree() ORDER BY tuple()"
        client.execute(create_query)

        # Read CSV file and insert rows
        record_count = 0
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            rows = []
            for row in reader:
                # Filter and order by the columns_list
                ordered_row = [row.get(col) for col in columns_list]
                rows.append(ordered_row)
            # Insert rows in one batch (for large files, consider batching)
            if rows:
                insert_query = f"INSERT INTO {table} ({', '.join(columns_list)}) VALUES"
                client.execute(insert_query, rows)
                record_count = len(rows)

        return jsonify({"status": "success", "record_count": record_count, "table": table})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route('/api/preview_data', methods=['POST'])
def preview_data():
    """
    Preview first 100 rows of data from a ClickHouse table based on selected columns.
    
    Expects JSON:
    {
        "host": "...",
        "port": "...",
        "database": "...",
        "user": "...",
        "jwt_token": "...",
        "secure": false,
        "table": "...",
        "columns": ["col1", "col2"],
    }
    """
    data = request.json
    table = data.get("table")
    columns = data.get("columns")
    
    if not table or not columns:
        return jsonify({"status": "error", "message": "Table name and columns are required."}), 400

    try:
        client = get_clickhouse_client(data)
        col_string = ", ".join(columns)
        query = f"SELECT {col_string} FROM {table} LIMIT 100"
        result = client.execute(query, with_column_types=True)
        col_names = [col[0] for col in result[1]]
        rows = result[0]
        # Format rows as list of dictionaries
        preview = [dict(zip(col_names, row)) for row in rows]
        return jsonify({"status": "success", "preview": preview})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route('/api/join_tables', methods=['POST'])
def join_tables():
    """
    (Bonus)
    Join multiple ClickHouse tables.
    
    Expects JSON:
    {
        "host": "...",
        "port": "...",
        "database": "...",
        "user": "...",
        "jwt_token": "...",
        "secure": false,
        "tables": ["table1", "table2"],
        "join_condition": "table1.id = table2.id",
        "columns": ["table1.col1", "table2.col2"],
        "output_file": "joined_output.csv",
        "delimiter": ","
    }
    """
    data = request.json
    tables = data.get("tables")
    join_condition = data.get("join_condition")
    columns = data.get("columns")
    output_file = data.get("output_file", "joined_output.csv")
    delimiter = data.get("delimiter", ",")

    if not (tables and join_condition and columns):
        return jsonify({"status": "error", "message": "Tables, join_condition and columns are required."}), 400

    try:
        client = get_clickhouse_client(data)
        col_string = ", ".join(columns)
        tables_string = " JOIN ".join(tables)
        query = f"SELECT {col_string} FROM {tables_string} ON {join_condition}"
        result = client.execute(query, with_column_types=True)
        col_names = [col[0] for col in result[1]]
        rows = result[0]

        output_path = os.path.join(OUTPUT_FOLDER, secure_filename(output_file))
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            writer.writerow(col_names)
            for row in rows:
                writer.writerow(row)

        return jsonify({"status": "success", "record_count": len(rows), "output_file": output_file})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


if __name__ == '__main__':
    # Run on port 5000 by default
    app.run(debug=True)
