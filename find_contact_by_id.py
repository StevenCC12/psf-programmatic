import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration ---
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"
CONTACT_ID_TO_FIND = "JTqZLQFMBFYNrYO11qR3"  # <-- Set the contact ID you want to search for

def find_ghl_contact_by_id():
    """
    Uses the GHL GET contact endpoint to find a contact by their contact ID.
    """
    if not PSF_ACCESS_TOKEN:
        print("CRITICAL ERROR: PSF_ACCESS_TOKEN is not set in your .env file.")
        return

    print(f"INFO: Searching for contact with ID: {CONTACT_ID_TO_FIND}")

    endpoint = f"{GHL_API_BASE_URL}/contacts/{CONTACT_ID_TO_FIND}"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }

    print(f"\nINFO: Making API call to: GET {endpoint}")

    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()

        print(f"\nSUCCESS! API Search Successful! Status Code: {response.status_code}")

        data = response.json()
        contact = data.get("contact", {})

        if contact:
            print("--- Contact Details ---")
            print(f"  Name: {contact.get('name')}")
            print(f"  Email: {contact.get('email')}")
            print(f"  ID: {contact.get('id')}")
            print(f"  Location ID: {contact.get('locationId')}")
            print(f"  Phone: {contact.get('phone')}")
            print(f"  Company: {contact.get('companyName')}")
            print("-" * 20)
        else:
            print("INFO: The API call was successful, but no contact was found with that ID.")

    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error during API test: {http_err}")
        print(f"  Status Code: {response.status_code}")
        print(f"  Response Text: {response.text}")
    except Exception as err:
        print(f"ERROR: An unexpected error occurred: {err}")

if __name__ == "__main__":
    find_ghl_contact_by_id()