# Data Pipeline Example

This project demonstrates a simple end-to-end data pipeline built in Python. The pipeline:
1. Fetches data from a public API (e.g., product data from a mock e-commerce API).
2. Processes the data using Pandas (e.g., filtering and transforming it).
3. Uploads the processed data as a `.txt` file to a public SFTP server.

## Features
- **API Integration**: Fetch product data from an API.
- **Data Processing**: Use Pandas to filter and clean data.
- **SFTP Upload**: Upload the processed data to a public SFTP server.
- **File Verification**: Automatically list files on the SFTP server to confirm the upload.

## Technologies Used
- Python
- Libraries: `pandas`, `paramiko`, `requests`

## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/data-pipeline-example.git
   cd data-pipeline-example

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

## Public SFTP Server Details
- Hostname: demo.wftpserver.com
- Port: 2222
- Username: demo
- Password: demo
- Upload Directory: /upload

## Future Enhancements
- Add AWS Lambda deployment for serverless execution.
- Replace the public SFTP server with an Amazon S3 bucket or a private SFTP server.
- Add logging for better monitoring and debugging.

## License
This project is licensed under the MIT License.