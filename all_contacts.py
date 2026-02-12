import requests
import os
import json
import time
import csv
from dotenv import load_dotenv

# Load environment variables from .env file
print("INFO: Loading environment variables...")
load_dotenv()
print("INFO: Environment variables loaded.")

# --- Configuration ---
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
PSF_LOCATION_ID = os.getenv("PSF_LOCATION_ID")
API_VERSION = "2021-07-28"
BASE_URL = "https://services.leadconnectorhq.com"
SEARCH_ENDPOINT = "/contacts/search"
PAGE_LIMIT = 100  # Number of contacts to fetch per API call
CSV_FILENAME = "all_contacts_export.csv" # Changed filename to reflect the update
CSV_HEADERS = ["email", "phone", "fn", "ln", "zip", "ct", "st", "country"]

# --- Helper Function to Make API Calls ---
def search_contacts_api(payload):
    """
    Makes a POST request to the /contacts/search endpoint.

    Args:
        payload (dict): The request body for the API call.

    Returns:
        dict: The JSON response from the API, or None if an error occurs.
    """
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except ValueError as json_err: # Includes JSONDecodeError
        print(f"JSON decode error: {json_err}")
        print(f"Response content: {response.text}")
    return None

# --- Main Export Function ---
def export_all_contacts_to_csv_title_cased():
    """
    Fetches all contacts from the HighLevel API for a given location
    using page-based pagination and writes them to a CSV file,
    title-casing the first and last names.
    """
    print(f"Starting contact export for Location ID: {PSF_LOCATION_ID}")
    print(f"Contacts will be saved to: {CSV_FILENAME} (with title-cased names)")

    page_number = 1
    total_contacts_fetched = 0
    
    if PSF_ACCESS_TOKEN == "YOUR_ACCESS_TOKEN" or PSF_LOCATION_ID == "YOUR_LOCATION_ID":
        print("\nERROR: Please replace 'YOUR_ACCESS_TOKEN' and 'YOUR_LOCATION_ID' in the script with your actual credentials.")
        return

    try:
        with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(CSV_HEADERS)  # Write header row

            while True:
                payload = {
                    "locationId": PSF_LOCATION_ID,
                    "pageLimit": PAGE_LIMIT,
                    "page": page_number
                }

                print(f"\nFetching contacts with payload: {payload}")
                response_data = search_contacts_api(payload)

                if not response_data:
                    print("Failed to fetch data from API. Aborting.")
                    break

                contacts = response_data.get("contacts", [])
                
                if not contacts:
                    print(f"No more contacts found on page {page_number}.")
                    break
                
                api_total_contacts = response_data.get("total")
                if page_number == 1 and api_total_contacts is not None: # Log total only once
                    print(f"API reports a total of {api_total_contacts} contacts for this location.")

                print(f"Fetched {len(contacts)} contacts in this batch (Page {page_number}). Processing for CSV...")
                total_contacts_fetched += len(contacts)

                for contact in contacts:
                    # Get first name and last name, defaulting to empty string if None
                    fn_lower = contact.get("firstNameLowerCase", "")
                    ln_lower = contact.get("lastNameLowerCase", "")

                    # Ensure they are strings before calling .title()
                    # If they were None, they are now "", and "".title() is ""
                    first_name_title_cased = fn_lower.title() if isinstance(fn_lower, str) else ""
                    last_name_title_cased = ln_lower.title() if isinstance(ln_lower, str) else ""
                    
                    row = [
                        contact.get("email", ""),
                        contact.get("phone", ""),
                        first_name_title_cased,  # Use title-cased first name
                        last_name_title_cased,   # Use title-cased last name
                        contact.get("postalCode", ""),
                        contact.get("city", ""), # City as is from API
                        contact.get("state", ""),
                        contact.get("country", "")
                    ]
                    csv_writer.writerow(row)
                
                if len(contacts) < PAGE_LIMIT:
                    print("Fetched fewer contacts than page limit, assuming this is the last page.")
                    break
                
                page_number += 1
                # time.sleep(0.5) # Optional delay

        print(f"\nSuccessfully exported {total_contacts_fetched} contacts to {CSV_FILENAME}")

    except IOError:
        print(f"Error writing to CSV file: {CSV_FILENAME}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Run the Export ---
if __name__ == "__main__":
    export_all_contacts_to_csv_title_cased()