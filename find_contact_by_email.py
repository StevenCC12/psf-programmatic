import requests
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration ---
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
PSF_LOCATION_ID = os.getenv("PSF_LOCATION_ID") 
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"
EMAIL_TO_FIND = "erik@ewcinvest.com" # The email we are searching for

def find_ghl_contact_by_email():
    """
    Uses the GHL search endpoint to find a contact by their email address,
    scoped to a specific locationId and with a page limit.
    """
    if not all([PSF_ACCESS_TOKEN, PSF_LOCATION_ID]):
        print("CRITICAL ERROR: PSF_ACCESS_TOKEN or PSF_LOCATION_ID is not set in your .env file.")
        return

    print(f"INFO: Searching for contact with email: {EMAIL_TO_FIND}")
    print(f"INFO: Scoping search to Location ID: {PSF_LOCATION_ID}")

    endpoint = f"{GHL_API_BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # FINAL CORRECTION: Use pageLimit as explicitly requested by the error message.
    payload = {
        "query": EMAIL_TO_FIND,
        "locationId": PSF_LOCATION_ID,
        "pageLimit": 10 # Using the exact parameter name from the error message.
    }

    print(f"\nINFO: Making API call to: POST {endpoint}")

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        
        print(f"\nSUCCESS! API Search Successful! Status Code: {response.status_code}")
        
        data = response.json()
        contacts = data.get("contacts", [])

        if contacts:
            print(f"Found {len(contacts)} matching contact(s).")
            print("--- Contact Details ---")
            for contact in contacts:
                print(f"  Name: {contact.get('name')}")
                print(f"  Email: {contact.get('email')}")
                print(f"  ID: {contact.get('id')}  <-- THIS IS THE ID YOU NEED")
                print("-" * 20)
        else:
            print("INFO: The API call was successful, but no contacts were found with that email address in this location.")

    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error during API test: {http_err}")
        print(f"  Status Code: {response.status_code}")
        print(f"  Response Text: {response.text}")
    except Exception as err:
        print(f"ERROR: An unexpected error occurred: {err}")

if __name__ == "__main__":
    find_ghl_contact_by_email()