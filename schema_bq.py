import os
from google.cloud import bigquery

# Authenticate with the service account key
client = bigquery.Client.from_service_account_json(r'C:\Users\nishu.prasad\Documents\ARVEST\secret_jsonkey.json')

def get_table_metadata(client, project_id, dataset_id, table_id):
    # Construct the full table ID
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Get the table schema (column-level attributes)
    table = client.get_table(table_ref)
    
    # Extract column-level metadata (name, type, description)
    columns = []
    for schema_field in table.schema:
        columns.append({
            "name": schema_field.name,
            "type": schema_field.field_type,
            "description": schema_field.description if schema_field.description else ""
        })
    
    return columns

def generate_tags_for_columns(columns):
    tags = []
    for column in columns:
        if 'email' in column['name'].lower():
            tags.append("PII")
        elif 'number_of_strikes' in column['name'].lower():
            tags.append("Identifier")
        elif column['type'] == "STRING" and "address" in column['description'].lower():
            tags.append("Sensitive Data")
        else:
            tags.append("No Tag Defined")
    return tags
	
def main():
    # BigQuery parameters
    project_id = 'dmgcp-del-171'
    dataset_id = 'testdata'
    table_id = 'Employee'
    
    # Step 1: Get BigQuery metadata (pass authenticated client)
    columns = get_table_metadata(client, project_id, dataset_id, table_id)
    
    # Step 2: Generate tags based on column attributes
    for column in columns:
        tags = generate_tags_for_columns([column])
        print(f"Column: {column['name']} | Tags: {tags}")
        print("-" * 20) # Separator for readability

if __name__ == "__main__":
    main()
