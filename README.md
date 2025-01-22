# Simple Data Pipeline

This project demonstrates a simple end-to-end data pipeline built in Python. The pipeline:
1. Fetches data from a public API (e.g., product data from a mock e-commerce API).
2. Processes the data using Pandas (e.g., filtering and transforming it).
3. Uploads the processed data as a `.txt` file to a public SFTP server.

## Features
- **API Integration**: Fetch product data from an API.
- **Data Processing**: Use Pandas to filter and clean data.
- **SFTP Upload**: Upload the processed data to a public SFTP server.
- **File Verification**: Automatically list files on the SFTP server to confirm the upload.
- **Robust Logging**:
  - Logs the status of every stage of the pipeline (e.g., fetching, processing, uploading).
  - Includes clear visual indicators such as (✅, ❌, 🚀, 🏁) to highlight success, failure, progress or completion.
  - Generates detailed logs to a `pipeline.log` file for future debugging and monitoring.
- **Error Handling**:
  - Handles common errors like:
    - API connection failures (e.g., network issues, invalid endpoints).
    - Data processing errors (e.g., empty or malformed data).
    - SFTP upload failures (e.g., authentication issues, invalid host/port).
  - Retries failed SFTP uploads up to 3 times before exiting gracefully.
  - Provides clear error messages in the logs for quick troubleshooting.

## Technologies Used
- Python
- Libraries: `pandas`, `paramiko`, `requests`

## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/tuobaar/simple_data_pipeline.git
   cd simple_data_pipeline

2. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   
3. Run the script:
   ```bash
   python main.py
4. Verify the upload:

   Check the list of files printed in the terminal.
   Alternatively, log in to the public SFTP server at https://demo.wftpserver.com using the following credentials:
   - Username: demo
   - Password: demo

## Environment Variables

This project uses a `.env` file to store environment variables, such as API endpoints and SFTP credentials. The `.env` file is included in this repository for testing purposes only.

### Example `.env` File:
```plaintext
# Public SFTP server (demo.wftpserver.com) Server Credentials
SFTP_HOST=demo.wftpserver.com
SFTP_PORT=2222
SFTP_USER=demo
SFTP_PASSWORD=demo

# Remote Path for the Uploaded File
REMOTE_FILE_PATH=/upload/processed_data.txt   # Save file with .txt extension

# API Endpoint/URL (Example: Fake Store API for demonstration purposes)
API_URL=https://fakestoreapi.com/products  # Public mock e-commerce API
```

### Important Notes:
The .env file included in this repository is for testing and demonstration purposes only.
Do not use this .env file in production or include sensitive credentials (e.g., private SFTP servers or APIs) in the file.
If you're using this project in your environment, create your own .env file with your specific credentials.

## Future Enhancements
- Add AWS Lambda deployment for serverless execution.
- Replace the public SFTP server with an Amazon S3 bucket or a private SFTP server.
- Add unit tests for better coverage and robustness.
- Add logging integration with centralized systems like AWS CloudWatch or ELK stack.

## License
This project is licensed under the MIT License.