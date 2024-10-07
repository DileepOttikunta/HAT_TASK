import subprocess
import requests
import pysolr
import pandas as pd
from datetime import datetime

# Function to create a Solr collection
def create_collection(collection_name):
    try:
        # Check if the collection already exists
        response = requests.get(f'http://localhost:8983/solr/admin/cores?action=STATUS&core={collection_name}')
        if collection_name in response.json()['status']:
            print(f"Collection {collection_name} already exists.")
            return

        # Full path to solr.cmd
        solr_path = "C:/Users/DILEEP/OneDrive/Desktop/solr-9.7.0/solr-9.7.0/bin/solr.cmd"
        command = f"{solr_path} create -c {collection_name}"
        subprocess.run(command, shell=True, check=True)
        print(f"Collection {collection_name} created successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating collection: {e}")
    except Exception as e:
        print(f"Error checking collection status: {e}")

# Function to index data into Solr
def index_data(collection_name, exclude_column):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}')
    try:
        # Read the employee data CSV with the proper encoding
        df = pd.read_csv('employee_data.csv', encoding='ISO-8859-1')

        # Drop the excluded column
        df.drop(columns=[exclude_column], inplace=True)

        # Convert 'Hire_Date' to ISO 8601 format if it exists
        if 'Hire_Date' in df.columns:
            df['Hire_Date'] = pd.to_datetime(df['Hire_Date'], format='%m/%d/%Y', errors='coerce') \
                              .dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Convert the DataFrame to a list of dictionaries and send to Solr
        data = df.to_dict(orient='records')
        solr.add(data)
        solr.commit()
        print(f"Data indexed in {collection_name} excluding {exclude_column}.")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please check if 'employee_data.csv' exists.")
    except UnicodeDecodeError as e:
        print(f"Encoding error while reading CSV: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to search Solr by column
def search_by_column(collection_name, column_name, column_value):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}')
    try:
        results = solr.search(f"{column_name}:{column_value}")
        for result in results:
            print(result)
    except Exception as e:
        print(f"Search error: {e}")

# Function to get the employee count
def get_emp_count(collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}')
    results = solr.search("*:*")
    print(f"Employee count: {results.hits}")

# Function to delete employee by ID
def del_emp_by_id(collection_name, employee_id):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}')
    solr.delete(id=employee_id)
    solr.commit()
    print(f"Employee {employee_id} deleted.")

# Function to get department facets
def get_dep_facet(collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}')
    try:
        facet_query = solr.search('*:*', **{
            'facet': 'true',
            'facet.field': 'Department',
            'facet.limit': -1,
        })
        for facet in facet_query.facets['facet_fields']['Department']:
            print(facet)
    except Exception as e:
        print(f"Facet error: {e}")

# Main task execution
def execute_task():
    v_nameCollection = 'Hash_Dileep'
    v_phoneCollection = 'Hash_1234'

    # Creating collections
    create_collection(v_nameCollection)
    create_collection(v_phoneCollection)

    # Getting employee count before indexing
    get_emp_count(v_nameCollection)

    # Indexing data excluding columns
    index_data(v_nameCollection, 'Department')
    index_data(v_phoneCollection, 'Gender')

    # Deleting an employee by ID
    del_emp_by_id(v_nameCollection, 'E02003')

    # Getting employee count after deletion
    get_emp_count(v_nameCollection)

    # Searching by column
    search_by_column(v_nameCollection, 'Department', 'IT')
    search_by_column(v_nameCollection, 'Gender', 'Male')
    search_by_column(v_phoneCollection, 'Department', 'IT')

    # Getting department facets
    get_dep_facet(v_nameCollection)
    get_dep_facet(v_phoneCollection)

# Execute the task
execute_task()
