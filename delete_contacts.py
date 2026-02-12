import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
import time

# --- Configuration ---
# Set up basic logging to see the script's progress and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from the .env file
load_dotenv()

# Get API Token and Location ID from environment variables
API_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
LOCATION_ID = os.getenv("PSF_LOCATION_ID")
CSV_FILE_PATH = "contacts_to_delete.csv" # The path to your CSV file

# HighLevel API Configuration
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Version": API_VERSION,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- Helper Functions ---

def search_contact_by_email(email):
    """
    Searches for a contact by email using the HighLevel API, based on the new documentation.
    
    Args:
        email (str): The email address of the contact to search for.
        
    Returns:
        str: The contact ID if found, otherwise None.
    """
    search_url = f"{BASE_URL}/contacts/search"
    
    # *** UPDATED PAYLOAD BASED ON NEW DOCUMENTATION ***
    payload = {
        "locationId": LOCATION_ID,
        "pageLimit": 1, # This is a required field 
        "filters": [ # Use the 'filters' array to search 
            {
                "field": "email", # Field name for email is 'email' 
                "operator": "eq", # 'eq' (equals) is a supported operator for email 
                "value": email
            }
        ]
    }
    
    try:
        response = requests.post(search_url, headers=HEADERS, json=payload)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        
        data = response.json()
        contacts = data.get("contacts", [])
        
        if contacts:
            contact_id = contacts[0].get("id")
            logging.info(f"Found contact for email '{email}' with ID: {contact_id}")
            return contact_id
        else:
            logging.warning(f"No contact found for email: {email}")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error searching for contact '{email}': {e}")
        # Enhanced logging to show the server's response
        if e.response:
            logging.error(f"Response Body: {e.response.text}")
        return None

def delete_contact_by_id(contact_id):
    """
    Deletes a contact by their contact ID using the HighLevel API.
    
    Args:
        contact_id (str): The ID of the contact to delete.
        
    Returns:
        bool: True if deletion was successful, otherwise False.
    """
    delete_url = f"{BASE_URL}/contacts/{contact_id}"
    
    try:
        response = requests.delete(delete_url, headers=HEADERS)
        response.raise_for_status()
        
        if response.status_code == 200 and response.json().get("succeded"):
            logging.info(f"Successfully deleted contact with ID: {contact_id}")
            return True
        else:
            logging.error(f"Failed to delete contact with ID: {contact_id}. Status: {response.status_code}, Body: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error deleting contact ID '{contact_id}': {e}")
        if e.response:
            logging.error(f"Response Body: {e.response.text}")
        return False

# --- Main Execution ---

def main():
    """
    Main function to read CSV, find contacts, and delete them.
    """
    if not API_TOKEN or not LOCATION_ID:
        logging.error("API_TOKEN and LOCATION_ID must be set in the .env file. Exiting.")
        return

    if not os.path.exists(CSV_FILE_PATH):
        logging.error(f"CSV file not found at path: {CSV_FILE_PATH}. Exiting.")
        return

    deleted_contacts_emails = []
    not_found_contacts_emails = []

    logging.info(f"Starting contact deletion process from file: {CSV_FILE_PATH}")

    try:
        df = pd.read_csv(CSV_FILE_PATH)
        
        if 'Email' not in df.columns:
            logging.error("CSV file must contain an 'Email' column. Exiting.")
            return

        for index, row in df.iterrows():
            email = row['Email']
            if pd.isna(email):
                logging.warning(f"Skipping row {index + 2} due to empty email.")
                continue

            logging.info(f"Processing email: {email}")
            
            contact_id = search_contact_by_email(email)
            
            # Rate-limiting delay
            time.sleep(1) 
            
            if contact_id:
                was_deleted = delete_contact_by_id(contact_id)
                if was_deleted:
                    deleted_contacts_emails.append(email)
                # Rate-limiting delay
                time.sleep(1)
            else:
                not_found_contacts_emails.append(email)
        
    except Exception as e:
        logging.critical(f"A critical error occurred: {e}")

    finally:
        logging.info("--- Deletion Process Summary ---")
        
        print("\n" + "="*40)
        print("          Deletion Summary Report")
        print("="*40 + "\n")
        
        print(f"Successfully Deleted Contacts ({len(deleted_contacts_emails)}):")
        if deleted_contacts_emails:
            for email in deleted_contacts_emails:
                print(f"  - {email}")
        else:
            print("  - None")
        
        print("\n" + "-"*40 + "\n")
        
        print(f"Contacts Not Found in HighLevel ({len(not_found_contacts_emails)}):")
        if not_found_contacts_emails:
            for email in not_found_contacts_emails:
                print(f"  - {email}")
        else:
            print("  - None")
            
        print("\n" + "="*40)
        logging.info("Script finished.")


if __name__ == "__main__":
    main()