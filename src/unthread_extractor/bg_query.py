import argparse
from google.cloud import bigquery
from google.oauth2 import service_account

def run_sql(service_account_file: str, sql_file: str):
    """
    Run a SQL command on BigQuery using a service account JSON file.

    Args:
        service_account_file (str): Path to service account JSON key.
        sql_file (str): Path to a SQL file containing the query/command.

    Returns:
        bigquery.table.RowIterator | None: Query results (for SELECT queries).
    """
    # Read SQL from file
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    # Load service account credentials
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    
    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    print(f"Running SQL from {sql_file}:\n{sql}\n")
    query_job = client.query(sql)
    results = query_job.result()  # Waits for job to finish

    # Return results if it's a SELECT query
    if sql.strip().lower().startswith("select"):
        return results
    else:
        print("Query executed successfully.")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL on BigQuery using a service account.")
    parser.add_argument("--key", required=True, help="Path to service account JSON file")
    parser.add_argument("--file", required=True, help="Path to SQL file containing the query/command")
    args = parser.parse_args()

    result = run_sql(args.key, args.file)

    if result:
        for row in result:
            print(dict(row))