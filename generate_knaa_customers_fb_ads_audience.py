import csv
import requests
import os
import time

# --- Environment Variable Loading ---
try:
    from dotenv import load_dotenv
    if load_dotenv():
        print("INFO: Loaded environment variables from .env file.")
    else:
        print("INFO: No .env file found or it was empty, relying on system environment variables.")
except ImportError:
    print("INFO: dotenv library not found, relying on system environment variables.")

# --- Configuration ---
ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
LOCATION_ID = os.getenv("PSF_LOCATION_ID")

# The exact tag we want to search for
TARGET_TAG = "knaa customer" 

# Output CSV Configuration
OUTPUT_FILENAME = "knaa_customer_facebook_audience.csv"
# Headers based on Facebook Custom Audience requirements
CSV_HEADERS = ["email", "phone", "fn", "ln", "zip", "ct", "st", "country"]

# API Details
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
PAGE_LIMIT = 100 # Max is 100 per page for standard queries usually, though docs say up to 500 might be possible [cite: 15]
DELAY_BETWEEN_API_CALLS = 0.5

def fetch_contacts_by_tag(access_token, location_id, tag_value):
    """
    Fetches contacts from CRM that have the specific tag.
    Uses the 'eq' operator for tags as per documentation.
    """
    if not all([access_token, location_id, tag_value]):
        print("ERROR: Access token, location ID, and tag value are required.")
        return None

    all_contacts = []
    page = 1
    search_endpoint = f"{BASE_URL}/contacts/search" # 
    
    headers = {
        "Authorization": f"Bearer {access_token}", 
        "Version": API_VERSION,
        "Content-Type": "application/json", 
        "Accept": "application/json"
    }
    
    print(f"INFO: Fetching contacts with tag '{tag_value}'...")

    while True:
        print(f"  Fetching page {page}...")
        
        # Filter Payload based on  for tags
        payload = {
            "locationId": location_id,
            "pageLimit": PAGE_LIMIT,
            "page": page,
            "filters": [
                {
                    "field": "tags",
                    "operator": "eq", # 'eq' finds contacts that have this specific tag
                    "value": tag_value
                }
            ]
        }

        response = None
        try:
            response = requests.post(search_endpoint, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
            
            contacts_on_page = response_data.get("contacts", [])
            
            if not contacts_on_page:
                print(f"  No more contacts found on page {page}.")
                break
                
            all_contacts.extend(contacts_on_page)
            print(f"  Fetched {len(contacts_on_page)} records on page {page}. Total found so far: {len(all_contacts)}")
            
            # If we received fewer records than the limit, we've reached the end
            if len(contacts_on_page) < PAGE_LIMIT:
                break
                
            page += 1
            time.sleep(DELAY_BETWEEN_API_CALLS)
            
            # Safety check for Standard Pagination limit 
            if len(all_contacts) >= 10000:
                print("WARNING: Reached 10,000 record limit for standard pagination. If you have more, switch to searchAfter.")
                break

        except Exception as e:
            print(f"ERROR: Fetching page {page} failed: {e}")
            if response is not None and hasattr(response, 'text'): 
                print(f"  Response: {response.text}")
            return None

    print(f"INFO: Finished fetching. Total contacts: {len(all_contacts)}")
    return all_contacts

def export_to_facebook_csv(contacts, filename):
    """
    Writes the contacts to a CSV with Facebook Audience specific headers.
    """
    if not contacts:
        print("INFO: No contacts to export.")
        return

    print(f"INFO: Writing {len(contacts)} contacts to '{filename}'...")

    with open(filename, mode='w', newline='', encoding='utf-8') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(CSV_HEADERS)

        for contact in contacts:
            # --- Field Mapping ---
            # Extracting fields based on Response Body [cite: 117]
            
            # 1. Email
            email = contact.get("email", "")
            
            # 2. Phone
            phone = contact.get("phone", "")
            
            # 3. First Name (fn) - Title Cased
            fn_raw = contact.get("firstNameLowerCase", "") # 
            fn = fn_raw.title() if fn_raw else ""
            
            # 4. Last Name (ln) - Title Cased
            ln_raw = contact.get("lastNameLowerCase", "") # 
            ln = ln_raw.title() if ln_raw else ""
            
            # 5. Zip (zip)
            zip_code = contact.get("postalCode", "") # 
            
            # 6. City (ct) - Title Cased
            city_raw = contact.get("city", "") # 
            ct = city_raw.title() if city_raw else ""
            
            # 7. State (st) - usually 2 letter code in GHL if formatted correctly, otherwise full name
            st = contact.get("state", "") # 
            
            # 8. Country
            country = contact.get("country", "") # 

            # Only write row if there is at least an email or phone (Facebook requirement usually)
            if email or phone:
                csv_writer.writerow([email, phone, fn, ln, zip_code, ct, st, country])

    print(f"SUCCESS: Export complete. File saved as: {filename}")

if __name__ == "__main__":
    if not ACCESS_TOKEN or not LOCATION_ID:
        print("CRITICAL ERROR: PSF_ACCESS_TOKEN and PSF_LOCATION_ID must be set in your .env or environment.")
    else:
        # 1. Fetch
        tagged_contacts = fetch_contacts_by_tag(ACCESS_TOKEN, LOCATION_ID, TARGET_TAG)
        
        # 2. Export
        if tagged_contacts:
            export_to_facebook_csv(tagged_contacts, OUTPUT_FILENAME)