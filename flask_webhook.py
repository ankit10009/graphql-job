import time
import requests
from flask import Flask, request, jsonify
import logging

# Configure logging
logging.basicConfig(filename="webhook_requests.log", level=logging.INFO, format='%(asctime)s - %(message)s')

# GraphQL API Endpoint
GRAPHQL_API_URL = "https://your-api-endpoint/graphql"
HEADERS = {"Authorization": "Bearer YOUR_ACCESS_TOKEN", "Content-Type": "application/json"}

# Webhook Server Configuration
WEBHOOK_PORT = 5000
app = Flask(__name__)

@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    if request.method == "POST":
        data = request.json
        logging.info(f"Received POST request: {data}")
        
        if data and data.get("status") == "COMPLETED":
            file_location = data.get("fileLocation")
            if file_location:
                download_file(file_location)
                return jsonify({"message": "File download initiated."}), 200
        return jsonify({"message": "Notification received."}), 200
    
    elif request.method == "GET":
        logging.info("Received GET request for webhook verification.")
        return jsonify({"message": "Webhook is active."}), 200

# Function to submit the job
def submit_job():
    SUBMIT_JOB_QUERY = """
    mutation {
      startBulkExtractionJob(input: { query: "YOUR_GRAPHQL_QUERY", webhookUrl: "http://your-server-address:5000/webhook" }) {
        jobId
      }
    }
    """
    response = requests.post(GRAPHQL_API_URL, json={"query": SUBMIT_JOB_QUERY}, headers=HEADERS)
    data = response.json()
    return data['data']['startBulkExtractionJob']['jobId']

# Function to download the extracted file
def download_file(file_url, output_path="extracted_data.csv"):
    response = requests.get(file_url, headers=HEADERS)
    if response.status_code == 200:
        with open(output_path, "wb") as file:
            file.write(response.content)
        logging.info(f"File downloaded successfully: {output_path}")
    else:
        logging.error("Failed to download file.")

# Main Workflow
def main():
    job_id = submit_job()
    logging.info(f"Submitted job with ID: {job_id}")

if __name__ == "__main__":
    # Start webhook listener in a separate thread
    from threading import Thread
    server_thread = Thread(target=lambda: app.run(host="0.0.0.0", port=WEBHOOK_PORT, debug=True, use_reloader=False))
    server_thread.start()
    
    # Submit job
    main()
