
import re
import pymongo
import sqlite3
from datetime import datetime

# ---------------------------------------------------------------------------
# Task 1 & 2: Extract and Transform Data
# ---------------------------------------------------------------------------
def extract_and_transform(log_file):
    """
    Reads the log file, extracts email addresses and dates, and transforms the date into
    a standardized format (YYYY-MM-DD HH:MM:SS). Returns a list of dictionaries.
    """
    # Regex pattern to capture email and date
    pattern = re.compile(
        r'From (\S+) (\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})'
    )
    transformed_data = []

    with open(log_file, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                email, date_str = match.groups()
                # Attempt to parse the date using the expected format.
                # Handle cases with variable spacing in the day field.
                try:
                    date_obj = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
                except ValueError:
                    date_obj = datetime.strptime(date_str, "%a %b  %d %H:%M:%S %Y")
                # Convert date to standard format
                formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                transformed_data.append({"email": email, "date": formatted_date})
    return transformed_data

# ---------------------------------------------------------------------------
# Task 3: Save Data to MongoDB
# ---------------------------------------------------------------------------
def save_to_mongodb(data):
    """
    Connects to MongoDB and inserts the provided data into the 'user_history' collection
    in the 'log_analysis' database.
    """
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["log_analysis"]
    collection = db["user_history"]
    # Clear existing data if needed (optional)
    collection.delete_many({})
    collection.insert_many(data)
    print("Data successfully inserted into MongoDB.")

# ---------------------------------------------------------------------------
# Task 4: Transfer Data from MongoDB to SQLite
# ---------------------------------------------------------------------------
def transfer_to_sqlite():
    """
    Fetches data from MongoDB and inserts it into an SQLite database table named 'user_history'.
    The table includes a primary key and NOT NULL constraints.
    """
    # Connect to MongoDB and retrieve records
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["log_analysis"]
    collection = db["user_history"]
    records = list(collection.find())

    # Connect to (or create) SQLite database and table
    conn = sqlite3.connect("user_history.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            date TEXT NOT NULL
        )
    """)
    # Clear existing data (optional)
    cursor.execute("DELETE FROM user_history")

    # Insert data into SQLite table
    insert_data = [(rec['email'], rec['date']) for rec in records]
    cursor.executemany("INSERT INTO user_history (email, date) VALUES (?, ?)", insert_data)
    conn.commit()
    conn.close()
    print("Data successfully transferred to SQLite.")

# ---------------------------------------------------------------------------
# Task 5: Run SQL Queries for Analysis (10 Queries)
# ---------------------------------------------------------------------------
def run_sql_queries():
    """
    Connects to the SQLite database and executes 10 SQL queries to analyze the email log data.
    Prints the results of each query.
    """
    conn = sqlite3.connect("user_history.db")
    cursor = conn.cursor()

    queries = {
        "1. Unique Emails": "SELECT DISTINCT email FROM user_history;",
        "2. Emails per Day": "SELECT date(date) as day, COUNT(*) FROM user_history GROUP BY day;",
        "3. First and Last Email per User": "SELECT email, MIN(date) AS first_email, MAX(date) AS last_email FROM user_history GROUP BY email;",
        "4. Emails per Domain": "SELECT substr(email, instr(email, '@') + 1) as domain, COUNT(*) FROM user_history GROUP BY domain;",
        "5. Total Number of Emails": "SELECT COUNT(*) FROM user_history;",
        "6. Emails Count by Month": "SELECT strftime('%Y-%m', date) AS month, COUNT(*) FROM user_history GROUP BY month;",
        "7. Email Frequency Ranking": "SELECT email, COUNT(*) as count FROM user_history GROUP BY email ORDER BY count DESC;",
        "8. Most Active Day": "SELECT date(date) as day, COUNT(*) as count FROM user_history GROUP BY day ORDER BY count DESC LIMIT 1;",
        "9. Emails by Day of Week": "SELECT strftime('%w', date) as weekday, COUNT(*) FROM user_history GROUP BY weekday;",
        "10. Average Emails per Day": "SELECT AVG(count) FROM (SELECT COUNT(*) as count FROM user_history GROUP BY date(date));"
    }

    print("\nSQL Query Results:")
    for desc, query in queries.items():
        print(f"\n{desc}:")
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        except Exception as e:
            print(f"Error executing query: {e}")

    conn.close()

# ---------------------------------------------------------------------------
# Main Execution Flow
# ---------------------------------------------------------------------------
def main():
    log_file = "mbox.txt"  # Ensure this file exists in the same directory
    print("Extracting and transforming data from the log file...")
    data = extract_and_transform(log_file)
    print(f"Extracted {len(data)} records.")

    print("\nSaving data to MongoDB...")
    save_to_mongodb(data)

    print("\nTransferring data from MongoDB to SQLite...")
    transfer_to_sqlite()

    print("\nRunning SQL queries for analysis...")
    run_sql_queries()

if __name__ == "__main__":
    main()