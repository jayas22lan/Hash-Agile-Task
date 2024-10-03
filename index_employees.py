import csv
from datetime import datetime
from elasticsearch import Elasticsearch

# Connect to Elasticsearch instance
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}], request_timeout=60)

# Delete the index if it exists
if es.indices.exists(index='employee_index'):
    es.indices.delete(index='employee_index')
    print("Deleted existing 'employee_index'.")

# Define index mappings
index_mapping = {
    "mappings": {
        "properties": {
            "Employee ID": {"type": "keyword"},
            "Full Name": {"type": "text"},
            "Job Title": {"type": "text"},
            "Department": {"type": "text"},
            "Business Unit": {"type": "text"},
            "Gender": {"type": "keyword"},
            "Ethnicity": {"type": "keyword"},
            "Age": {"type": "integer"},
            "Hire Date": {"type": "date", "format": "MM/dd/yyyy"},
            "Annual Salary": {"type": "text"},  # Salary stored as string because of formatting ($)
            "Bonus %": {"type": "text"},        # Bonus stored as string due to percentage
            "Country": {"type": "text"},
            "City": {"type": "text"},
            "Exit Date": {"type": "date", "format": "MM/dd/yyyy", "null_value": "NULL"}
        }
    }
}
# Create a new index
es.indices.create(index='employee_index', body=index_mapping)
print("Created 'employee_index'.")

# Function to parse date and handle empty fields
def parse_date(date_str):
    if date_str:
        # Convert the date to the format MM/dd/yyyy
        return datetime.strptime(date_str.strip(), '%m/%d/%Y').strftime('%m/%d/%Y')
    return None  # Handle missing date as None

# Open the CSV file and index each row
with open('employee_data.csv', encoding='ISO-8859-1') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Parse dates before indexing
        row['Hire Date'] = parse_date(row['Hire Date'])
        row['Exit Date'] = parse_date(row['Exit Date'])

        # Index each document (employee record) into Elasticsearch
        es.index(index='employee_index', document=row)

print("Data indexed successfully!")

result = es.search(index="employee_index", body={"query": {"match": {"Ethnicity":"Asian"}}})
print(result)
