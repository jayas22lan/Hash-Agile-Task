import pandas as pd
import csv
from elasticsearch import Elasticsearch, NotFoundError

# Initialize the Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200,'scheme':'http'}],request_timeout=60)

# Load CSV data with proper file handling
def load_csv(file_path):
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            return pd.read_csv(file)
    except UnicodeDecodeError:
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            return pd.read_csv(file)

# Clean the data (drop NaN, ensure types are correct)
def clean_data(data):
    print(data.columns)  # To see all column names in the dataset
    # Ensure the correct column names are used
    if 'employee_id' in data.columns:
        data['employee_id'] = data['employee_id'].astype(str)
    elif 'Employee ID' in data.columns:
        data['Employee ID'] = data['Employee ID'].astype(str)
    else:
        raise KeyError("The column for employee ID is not found in the dataset.")
    
    # Remove rows with missing values (optional, based on data)
    data = data.dropna()  
    return data

# 1. Create Collection with Mapping
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        mapping = {
            "mappings": {
                "properties": {
                    "employee_id": {"type": "keyword"},
                    "Name": {"type": "text"},
                    "Department": {"type": "keyword"},
                    "Gender": {"type": "keyword"}
                }
            }
        }
        es.indices.create(index=p_collection_name, body=mapping)
        print(f"Collection '{p_collection_name}' created with custom mapping.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

# 2. Index Data (excluding the provided column)
def indexData(p_collection_name, p_exclude_column, csv_data):
    for _, emp in csv_data.iterrows():
        emp_data = emp.to_dict()
        emp_data.pop(p_exclude_column, None)  # Remove excluded column
        es.index(index=p_collection_name, document=emp_data)  # Use 'document' instead of 'body'
    print(f"Data indexed into collection '{p_collection_name}' excluding column '{p_exclude_column}'.")

# 3. Search by Column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    return res['hits']['hits']

# 4. Get Employee Count
def getEmpCount(p_collection_name):
    res = es.count(index=p_collection_name)
    return res['count']

# 5. Delete Employee by ID
def delEmpById(p_collection_name, p_employee_id):
    query = {
        "query": {
            "match": {
                "employee_id": p_employee_id
            }
        }
    }
    try:
        es.delete_by_query(index=p_collection_name, body=query)
        print(f"Employee with ID '{p_employee_id}' deleted.")
    except NotFoundError:
        print(f"Employee with ID '{p_employee_id}' not found.")

# 6. Get Department Facet
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    return res['aggregations']['department_count']['buckets']

# Execution of functions with CSV input
csv_file_path = 'employee_data.csv'
employee_data = load_csv(csv_file_path)

# Clean the employee data before indexing
employee_data = clean_data(employee_data)

v_nameCollection = 'jai_hash'  # Replace <Your Name>
v_phoneCollection = '2706'    # Replace <Your Phone last four digits>

# Create Collections
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Get Employee Count
print(f"Employee count in {v_nameCollection}: {getEmpCount(v_nameCollection)}")

# Index Data excluding specific columns from CSV
indexData(v_nameCollection, 'Department', employee_data)
indexData(v_phoneCollection, 'Gender', employee_data)

# Delete Employee by ID
delEmpById(v_nameCollection, 'E02003')

# Get updated Employee Count
print(f"Updated employee count in {v_nameCollection}: {getEmpCount(v_nameCollection)}")

# Search by Column
print(f"Search results for Department 'IT' in {v_nameCollection}: {searchByColumn(v_nameCollection, 'Department', 'IT')}")
print(f"Search results for Gender 'Male' in {v_nameCollection}: {searchByColumn(v_nameCollection, 'Gender', 'Male')}")
print(f"Search results for Department 'IT' in {v_phoneCollection}: {searchByColumn(v_phoneCollection, 'Department', 'IT')}")

# Get Department Facet
print(f"Department facet in {v_nameCollection}: {getDepFacet(v_nameCollection)}")
print(f"Department facet in {v_phoneCollection}: {getDepFacet(v_phoneCollection)}")
