import requests  # For making API requests
import pandas as pd  # For data processing and manipulation
import paramiko  # For SFTP file transfer
import logging  # For logging
from io import StringIO  # For in-memory file handling


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)


# ----------------------------
# Step 1: Fetch Data from API
# ----------------------------
def fetch_data(api_url):
    """
    Fetches data from a specified API endpoint.

    Args:
        api_url (str): The URL of the API to fetch data from.

    Returns:
        list: A list of dictionaries containing the data from the API.
    """
    try:
        response = requests.get(api_url)  # Make a GET request to the API
        response.raise_for_status()  # Raise an exception for HTTP errors
        logging.info("✅ Data fetched successfully from API!")
        return response.json()  # Parse and return the JSON response as Python objects
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch data from API: {e}")
        return None  # Return None if the request fails


# ---------------------------------
# Step 2: Process the Data (Pandas)
# ---------------------------------
def process_data(data):
    """
    Processes the raw data into a filtered and structured TXT format.

    Args:
        data (list): Raw data as a list of dictionaries.

    Returns:
        StringIO: An in-memory file object containing the processed TXT data.
    """
    try:
        # Convert the data into a Pandas DataFrame
        df = pd.DataFrame(data)
        logging.info("🔄 Original Data:")
        logging.info(df.head().to_string())  # Log first few rows of the DataFrame

        # Filter: Example - Only include products with a price greater than 50
        df_filtered = df[df['price'] > 50]
        logging.info("🔄 Filtered Data (Price > 50):")
        logging.info(df_filtered.head().to_string())  # Log first few rows of filtered data

        # Save the filtered data to an in-memory TXT file (tab-separated)
        txt_buffer = StringIO()
        df_filtered.to_csv(txt_buffer, sep="\t", index=False)  # Save as TSV (tab-separated values)
        logging.info("✅ Data processed and saved to TXT format!")
        return txt_buffer  # Return the in-memory TXT buffer
    except Exception as e:
        logging.error(f"❌ Error processing data: {e}")
        return None


# ----------------------------------------
# Step 3: Upload the TXT to an SFTP Server
# ----------------------------------------
def upload_to_sftp(txt_buffer, sftp_host, sftp_port, sftp_user, sftp_password, remote_file_path):
    """
    Uploads the processed TXT data to an SFTP server and provides instructions to verify the upload.

    Args:
        txt_buffer (StringIO): An in-memory file object containing the TXT data.
        sftp_host (str): Hostname or IP of the SFTP server.
        sftp_port (int): Port number of the SFTP server (default is 22).
        sftp_user (str): Username for SFTP authentication.
        sftp_password (str): Password for SFTP authentication.
        remote_file_path (str): Path where the TXT file will be uploaded on the SFTP server.
    """
    try:
        # Reset the file pointer to the beginning of the buffer
        txt_buffer.seek(0)

        # Connect to the SFTP server
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Upload the file to the remote server
        with sftp.open(remote_file_path, 'w') as remote_file:
            remote_file.write(txt_buffer.getvalue())  # Write the TXT data to the remote file
            logging.info(f"✅ File successfully uploaded to: {remote_file_path}")

        # List the files in the upload directory
        logging.info("\n📂 Files currently in the /upload directory:")
        files = sftp.listdir('/upload')  # Get the list of files in the /upload directory
        for file in files:
            logging.info(f" - {file}")  # Print each file in the directory

        # Close the SFTP connection
        sftp.close()
        transport.close()

        # Print manual verification instructions
        print("\n🔍 To manually verify the uploaded file:")
        print("1. Visit the SFTP server's web interface: https://demo.wftpserver.com")
        print("2. Use the following credentials:")
        print("   - Username: demo")
        print("   - Password: demo")
        print("3. Navigate to the `/upload` directory to view the uploaded file.")

    except paramiko.SSHException as e:
        logging.error(f"❌ SFTP connection failed: {e}")
    except Exception as e:
        logging.error(f"❌ An error occurred during file upload: {e}")


# ---------------------------
# Step 4: Main Pipeline Logic
# ---------------------------
if __name__ == "__main__":
    # Public SFTP server (demo.wftpserver.com) details
    SFTP_HOST = "demo.wftpserver.com"
    SFTP_PORT = 2222
    SFTP_USER = "demo"
    SFTP_PASSWORD = "demo"
    REMOTE_FILE_PATH = "/upload/processed_data.txt"  # Save file with .txt extension

    # API URL (Example: Fake Store API for demonstration purposes)
    API_URL = "https://fakestoreapi.com/products"  # Public mock e-commerce API

    logging.info("🚀 Starting the data pipeline...")

    # Step 1: Fetch the data
    raw_data = fetch_data(API_URL)
    if raw_data:
        # Step 2: Process the data
        txt_buffer = process_data(raw_data)
        if txt_buffer:
            # Step 3: Upload to the public SFTP server
            upload_to_sftp(txt_buffer, SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, REMOTE_FILE_PATH)

    logging.info("🏁 Data pipeline completed!")

