import os  # For operating system-related tasks, such as reading environment variables
import time  # For adding delays between retries during SFTP uploads
import logging  # For logging pipeline progress and errors
import requests  # For making API requests
import pandas as pd  # For data processing and manipulation
import paramiko  # For connecting to and transferring files via SFTP
from dotenv import load_dotenv  # For securely loading credentials from environment variables
from io import StringIO  # For handling in-memory text streams (e.g., saving processed data)

# Load environment variables from the .env file
load_dotenv()

# Read variables from the environment
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
REMOTE_FILE_PATH = os.getenv("REMOTE_FILE_PATH")
API_URL = os.getenv("API_URL")

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
        list: A list of dictionaries containing the data from the API, or None if an error occurs.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        logging.info("✅ Data fetched successfully from API!")
        return response.json()  # Parse and return the JSON response as Python objects
    except requests.exceptions.ConnectionError:
        logging.error("❌ Failed to connect to the API. Check your network connection or API URL/Endpoint.")
    except requests.exceptions.HTTPError as e:
        logging.error(f"❌ HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch data from API: {e}")
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred while fetching data: {e}")
    return None  # Return None in case of an error


# ---------------------------------
# Step 2: Process the Data (Pandas)
# ---------------------------------
def process_data(data):
    """
    Processes the raw data into a filtered and structured TXT format.

    Args:
        data (list): Raw data as a list of dictionaries.

    Returns:
        StringIO: An in-memory file object containing the processed TXT data, or None if an error occurs.
    """

    try:
        # Validate that data is not empty
        if not data:
            raise ValueError("No data provided for processing")

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
    except ValueError as e:
        logging.error(f"❌ ValueError: {e}")
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred while processing data: {e}")
    return None


# ----------------------------------------
# Step 3: Upload the TXT to an SFTP Server
# ----------------------------------------
def upload_to_sftp(txt_buffer, sftp_host, sftp_port, sftp_user, sftp_password, remote_file_path, retries=3, delay=5):
    """
    Uploads the processed TXT data to an SFTP server with retry logic.

    Args:
        txt_buffer (StringIO): In-memory buffer containing TXT data to upload.
        sftp_host (str): Hostname of the SFTP server.
        sftp_port (int): Port of the SFTP server.
        sftp_user (str): Username for SFTP authentication.
        sftp_password (str): Password for SFTP authentication.
        remote_file_path (str): Remote path where the file will be uploaded.
        retries (int): Number of retry attempts for failed uploads.
        delay (int): Delay (in seconds) between retry attempts.
    """

    transport = None

    for attempt in range(1, retries + 1):
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

            # Print manual verification instructions only when the upload is successful
            logging.info("\n🔍 To manually verify the uploaded file:")
            logging.info("1. Visit the SFTP server's web interface: https://demo.wftpserver.com")
            logging.info("2. Use the following credentials:")
            logging.info(f"   - Username: {sftp_user}")
            logging.info(f"   - Password: {sftp_password}")
            logging.info("3. Navigate to the `/upload` directory to view the uploaded file.")

            return "upload_successful"  # Explicit success return value  # Exit the function if the upload is successful

        except Exception as e:
            logging.error(f"❌ Upload attempt {attempt} failed: {e}")
            if attempt < retries:
                logging.info(f"🔄 Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"❌ All {retries} attempts failed. Giving up.")
                logging.error(f"❌ Failed to upload data to SFTP server after {retries} retries")
                return "upload_failed"  # Explicit failure return value

        finally:
            # Ensure transport is only closed if it was successfully created
            if transport:
                transport.close()
                logging.info("✅ Transport connection closed.")


# ---------------------------
# Step 4: Main Pipeline Logic
# ---------------------------
if __name__ == "__main__":

    logging.info("🚀 Starting the data pipeline...")

    # Step 1: Fetch the data
    raw_data = fetch_data(API_URL)
    if raw_data:
        # Step 2: Process the data
        txt_buffer = process_data(raw_data)
        if txt_buffer:
            # Step 3: Upload to the public SFTP server
            upload = upload_to_sftp(txt_buffer, SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, REMOTE_FILE_PATH)

            if upload == "upload_successful":
                logging.info("🏁 ✅ All data pipeline processes completed successfully!")
            elif upload == "upload_failed":
                logging.info("🏁 ❌ Data pipeline completed without uploading data to SFTP server!")
    else:
        logging.error("❌ Data pipeline failed at the Fetch Data stage.")
