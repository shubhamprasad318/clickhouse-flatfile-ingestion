`# 🔄 ClickHouse ↔ Flat File Ingestion Tool`

`A user-friendly web-based interface that enables **bidirectional data ingestion** between **ClickHouse** and **flat files (CSV)**.`    
`Built with **Flask (Python)** for the backend and modern **HTML/CSS/JavaScript** for the frontend.`  
`---`  
`## 🚀 Features`

`- ✅ Connect to ClickHouse with custom credentials`  
`- 📦 Export data from any ClickHouse table into a CSV file`  
`- 📥 Import data from a CSV file into a ClickHouse table`  
`- 👁️ Preview first 100 rows of selected table and columns`  
`- 💡 Clean, modern, and responsive UI`  
`---`  
`## 🛠️ Tech Stack`  
`- **Frontend:** HTML5, CSS3, JavaScript`  
`- **Backend:** Flask (Python)`  
`- **Database:**  Datasets: Use ClickHouse example datasets like uk_price_paid and ontime (https://clickhouse.com/docs/getting-started/example-datasets).`   
`---`

`## 📂 Project Structure`

clickhouse-flatfile-ingestion/

├── backend/

  	 ├── app.py   

├── requirements.txt

 └── uploaded\_files/            \# For temp CSV uploads

├── frontend/

│   └── index.html

├── output\_files/                 \# For exported CSVs

├── prompts.txt                   \# List of AI prompts you used

├── README.md

`---`

`## ⚙️ Setup Instructions`

`1. **Clone the repository**`

```` ```bash ````  
`git clone https://github.com/shubhamprasad318/clickhouse-ingestion-tool.git`  
`cd clickhouse-ingestion-tool`

2. **Create a virtual environment (optional)**

`python -m venv venv`  
`source venv/bin/activate  # or venv\Scripts\activate (on Windows)`

3. **Install dependencies**

`pip install -r requirements.txt`

Make sure ClickHouse is running and accessible.

4. **Run the application**

`python app.py`

5. **Access it on**

`http://localhost:5000`

---

## **🧪 Sample .env or Configuration**

Set your default ClickHouse details in the frontend or as default values:

env  
Copy code  
`CLICKHOUSE_HOST=localhost`  
`CLICKHOUSE_PORT=9000`  
`CLICKHOUSE_DB=default`  
`CLICKHOUSE_USER=default`  
`CLICKHOUSE_JWT=your_token_here`  
`CLICKHOUSE_SECURE=false`

---

## **📬 API Endpoints**

| Method | Endpoint | Description |
| ----- | ----- | ----- |
| POST | `/api/get_tables` | Fetch all ClickHouse tables |
| POST | `/api/get_columns` | Get columns for a specific table |
| POST | `/api/preview_data` | Preview 100 rows of selected columns |
| POST | `/api/ingest_clickhouse_to_flatfile` | Export selected data to CSV |
| POST | `/api/ingest_flatfile_to_clickhouse` | Import CSV data into ClickHouse |

All POST APIs accept JSON payloads (except the CSV upload, which uses `multipart/form-data`).

---

## **🧠 Future Improvements**

* Add pagination and search to preview table

* Support Parquet and JSON formats

* Add authentication to the frontend

* Upload/download file progress bar

---

## **👨‍💻 Author**

**Shubham Prasad**  
 📧 shubhamprasad3758@gmail.com  
 🔗 [LinkedIn](https://www.linkedin.com/in/shubham-prasad-320495196/)

---

## **🙌 Contributions**

Feel free to fork the project, create issues, or submit PRs to improve the tool\!

`---`

