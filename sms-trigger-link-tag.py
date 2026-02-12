import requests
import os
import csv
import time
from dotenv import load_dotenv

# --- 1. Setup & Configuration ---
load_dotenv()

# Configuration Variables
GHL_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
CSV_FILE_PATH = "sms-trigger-link.csv"  # The name of your input CSV
TAG_TO_ADD = "temp: dec sms"  # Change this to the tag you want to apply
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"

# Common Headers for GHL API
HEADERS = {
    "Authorization": f"Bearer {GHL_ACCESS_TOKEN}",
    "Version": "2021-07-28",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def get_contact_by_id(contact_id):
    """
    Validates that a contact exists by fetching their details.
    GET /contacts/:contactId
    """
    endpoint = f"{GHL_API_BASE_URL}/contacts/{contact_id}"
    
    try:
        response = requests.get(endpoint, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            # Depending on API version, contact data might be at root or under 'contact' key
            contact = data.get('contact', data)
            return contact
        elif response.status_code == 404:
            print(f"  [WARN] Contact ID {contact_id} not found in GHL.")
            return None
        else:
            print(f"  [ERROR] Failed to fetch contact {contact_id}. Status: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  [EXCEPTION] Error fetching contact {contact_id}: {e}")
        return None

def add_tag_to_contact(contact_id, tag):
    """
    Adds a list of tags to a specific contact.
    POST /contacts/:contactId/tags
    """
    endpoint = f"{GHL_API_BASE_URL}/contacts/{contact_id}/tags"
    
    payload = {
        "tags": [tag]
    }
    
    try:
        response = requests.post(endpoint, headers=HEADERS, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"  [SUCCESS] Tag '{tag}' added to {contact_id}.")
            return True
        else:
            print(f"  [FAIL] Could not tag {contact_id}. Status: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"  [EXCEPTION] Error tagging contact {contact_id}: {e}")
        return False

def process_csv_and_tag():
    """
    Main Logic: Reads CSV, verifies contact, adds tag.
    """
    if not GHL_ACCESS_TOKEN:
        print("CRITICAL ERROR: GHL_ACCESS_TOKEN is not set in your .env file.")
        return

    if not os.path.exists(CSV_FILE_PATH):
        print(f"CRITICAL ERROR: Could not find file: {CSV_FILE_PATH}")
        return

    print(f"INFO: Starting processing of {CSV_FILE_PATH}...")
    
    success_count = 0
    fail_count = 0

    with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # FIX: Check if fieldnames exist before looping to satisfy Pylance
        if reader.fieldnames:
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
        else:
            print("CRITICAL ERROR: CSV file appears to be empty or has no headers.")
            return
        
        if 'contactId' not in reader.fieldnames:
            print(f"CRITICAL ERROR: CSV must have a column named 'contactId'. Found: {reader.fieldnames}")
            return

        for row in reader:
            contact_id = row.get('contactId', '').strip()
            
            if not contact_id:
                continue

            print(f"\nProcessing ID: {contact_id}")

            # Step 1: Validate Contact Exists (GET Request)
            contact_data = get_contact_by_id(contact_id)
            
            if contact_data:
                contact_name = contact_data.get('name', contact_data.get('firstName', 'Unknown'))
                print(f"  [INFO] Found Contact: {contact_name}")

                # Step 2: Add Tag (POST Request)
                result = add_tag_to_contact(contact_id, TAG_TO_ADD)
                
                if result:
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1
            
            # Rate limiting safety (GHL allows 100 requests/10s, but safe is better)
            time.sleep(0.2)

    print("\n" + "="*30)
    print("PROCESSING COMPLETE")
    print(f"Successful Tags: {success_count}")
    print(f"Failed/Skipped:  {fail_count}")
    print("="*30)

if __name__ == "__main__":
    process_csv_and_tag()