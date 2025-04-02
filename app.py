import time
import requests

# GraphQL API Endpoint
GRAPHQL_API_URL = "https://your-api-endpoint/graphql"
HEADERS = {"Authorization": "Bearer YOUR_ACCESS_TOKEN", "Content-Type": "application/json"}

# GraphQL Query to Submit the Job
SUBMIT_JOB_QUERY = """
mutation {
  startBulkExtractionJob(input: { query: "YOUR_GRAPHQL_QUERY" }) {
    jobId
  }
}
"""

# Function to submit the job
def submit_job():
    response = requests.post(GRAPHQL_API_URL, json={"query": SUBMIT_JOB_QUERY}, headers=HEADERS)
    data = response.json()
    return data['data']['startBulkExtractionJob']['jobId']

# Function to check job status
def check_job_status(job_id):
    STATUS_QUERY = f"""
    query {{
      jobStatus(jobId: "{job_id}") {{
        status
        fileLocation
      }}
    }}
    """
    
    while True:
        response = requests.post(GRAPHQL_API_URL, json={"query": STATUS_QUERY}, headers=HEADERS)
        data = response.json()
        status_info = data['data']['jobStatus']
        
        status = status_info['status']
        print(f"Job Status: {status}")
        
        if status == "COMPLETED":
            return status_info['fileLocation']
        elif status == "ERROR":
            print("Job failed. Retrying...")
            return None
        elif status == "RUNNING":
            time.sleep(60)  # Wait for a minute before checking again

# Function to download the extracted file
def download_file(file_url, output_path="extracted_data.csv"):
    response = requests.get(file_url, headers=HEADERS)
    if response.status_code == 200:
        with open(output_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully: {output_path}")
    else:
        print("Failed to download file.")

# Main Workflow
def main():
    job_id = submit_job()
    print(f"Submitted job with ID: {job_id}")
    
    while True:
        file_location = check_job_status(job_id)
        if file_location:
            download_file(file_location)
            break
        else:
            job_id = submit_job()  # Retry submission if previous job failed

if __name__ == "__main__":
    main()
