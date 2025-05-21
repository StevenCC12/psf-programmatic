import requests
import os
import json
import time
import csv

# --- Environment Variable Loading (Optional, but recommended) ---
try:
    from dotenv import load_dotenv
    if load_dotenv():
        print("Loaded environment variables from .env file.")
    else:
        print("No .env file found or it was empty, relying on system environment variables.")
except ImportError:
    print("dotenv library not found, relying on system environment variables.")

# --- Configuration ---
ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
LOCATION_ID = os.getenv("PSF_LOCATION_ID")

# If not using environment variables, uncomment and set your actual values:
# ACCESS_TOKEN = "YOUR_PSF_ACCESS_TOKEN_HERE"
# LOCATION_ID = "YOUR_PSF_LOCATION_ID_HERE"

# API Details
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
PAGE_LIMIT = 100
DELAY_BETWEEN_API_CALLS = 0.5
DELAY_AFTER_UPDATE = 1

# CSV Configuration
CSV_FILENAME = "tofu_webinar_leads_export.csv"
CSV_HEADERS = ["email", "phone", "fn", "ln", "zip", "ct", "country"]

# Define the tags for segmentation
TAG_LEAD = "lead: amazon masterclass (swe)"
TAG_PURCHASE = "purchase: 5-day amazon challenge (swe)"
TAG_EXCLUSION = "exclusion 25-04-18: ahead, amazon-byrå, partner, ledning, not swede (pre 2025), knaa customer, status: paused"


def update_contact_zip(contact_id, new_zip_code, access_token):
    """
    Updates the postalCode for a specific contact.
    Returns True if successful, False otherwise.
    """
    if not contact_id or new_zip_code is None:
        print(f"Error: contact_id and new_zip_code are required for update. contact_id: {contact_id}, new_zip_code: {new_zip_code}")
        return False

    endpoint = f"{BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "postalCode": new_zip_code
    }

    print(f"  Attempting to update zip for contact {contact_id} to: '{new_zip_code}'...")
    try:
        response = requests.put(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        print(f"  Successfully updated zip for contact {contact_id}. Status: {response.status_code}")
        time.sleep(DELAY_AFTER_UPDATE)
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"  HTTP error updating zip for contact {contact_id}: {http_err}")
        print(f"  Status Code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"  Request error updating zip for contact {contact_id}: {req_err}")
    except Exception as e:
        print(f"  Unexpected error updating zip for contact {contact_id}: {e}")
    
    time.sleep(DELAY_BETWEEN_API_CALLS)
    return False


def process_contacts_for_export_and_clean_zip(access_token, location_id, tag_lead, tag_purchase, tag_exclusion):
    """
    Fetches contacts based on complex tag filters, cleans zip codes,
    updates contacts, and exports specified fields (with title-cased names) to a CSV file.
    """
    if not all([access_token, location_id]):
        print("Error: ACCESS_TOKEN and LOCATION_ID must be set for processing.")
        return

    print(f"Starting contact processing for Location ID: {location_id}...")
    print(f"Exporting to CSV: {CSV_FILENAME}")

    page = 1
    total_contacts_fetched = 0
    total_contacts_written_to_csv = 0
    zip_updates_attempted = 0
    zip_updates_successful = 0
    
    search_endpoint = f"{BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8') as csv_file_obj:
        csv_writer = csv.writer(csv_file_obj)
        csv_writer.writerow(CSV_HEADERS)

        while True:
            print(f"\nFetching page {page} of contacts matching complex tag filters...")
            search_payload = {
                "locationId": location_id,
                "pageLimit": PAGE_LIMIT,
                "page": page,
                "filters": [
                    {
                        "group": "OR",
                        "filters": [
                            {"group": "AND", "filters": [{"field": "tags", "operator": "contains", "value": tag_lead}, {"field": "tags", "operator": "not_contains", "value": tag_exclusion}]},
                            {"group": "AND", "filters": [{"field": "tags", "operator": "contains", "value": tag_purchase}, {"field": "tags", "operator": "not_contains", "value": tag_exclusion}]}
                        ]
                    }
                ],
                "sort": [{"field": "dateAdded", "direction": "desc"}]
            }

            try:
                response = requests.post(search_endpoint, headers=headers, json=search_payload)
                response.raise_for_status()
                response_data = response.json()
                contacts_on_page = response_data.get("contacts", [])
                
                if not contacts_on_page:
                    print(f"No more contacts found on page {page} matching criteria.")
                    break
                
                total_contacts_fetched += len(contacts_on_page)
                print(f"Found {len(contacts_on_page)} contacts on page {page}. Processing...")

                for contact in contacts_on_page:
                    contact_id = contact.get('id')
                    email = contact.get('email', '')
                    phone = contact.get('phone', '')
                    
                    # Get lowercase names from API and ensure they default to empty string if missing or None
                    fn_lower = contact.get('firstNameLowerCase') or ''
                    ln_lower = contact.get('lastNameLowerCase') or ''

                    # Convert to title case for CSV
                    fn_title_case = fn_lower.title()
                    ln_title_case = ln_lower.title()

                    original_zip = contact.get('postalCode', '') # Already defaulting to empty string
                    city = contact.get('city', '')             # Already defaulting to empty string
                    country = contact.get('country', '')       # Already defaulting to empty string

                    current_zip_for_csv = original_zip if original_zip is not None else ''

                    if isinstance(original_zip, str) and ' ' in original_zip:
                        cleaned_zip = original_zip.replace(' ', '')
                        print(f"Contact ID: {contact_id}, Email: {email} - Original Zip: '{original_zip}', Cleaned Zip: '{cleaned_zip}'")
                        if cleaned_zip != original_zip:
                            zip_updates_attempted += 1
                            if update_contact_zip(contact_id, cleaned_zip, access_token):
                                zip_updates_successful += 1
                            current_zip_for_csv = cleaned_zip 
                    
                    # Prepare row for CSV with title-cased names
                    csv_row = [email, phone, fn_title_case, ln_title_case, current_zip_for_csv, city, country]
                    csv_writer.writerow(csv_row)
                    total_contacts_written_to_csv += 1

                if len(contacts_on_page) < PAGE_LIMIT:
                    print("\nFetched all contacts matching criteria (last page was not full).")
                    break
                
                page += 1
                time.sleep(DELAY_BETWEEN_API_CALLS)

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error fetching contacts page {page}: {http_err}")
                if 'response' in locals(): print(f"Status Code: {response.status_code}, Response: {response.text}")
                break 
            except requests.exceptions.RequestException as req_err:
                print(f"Request error fetching contacts page {page}: {req_err}")
                break
            except json.JSONDecodeError as json_err:
                print(f"JSON decode error for contacts page {page}: {json_err}")
                if 'response' in locals(): print(f"Response text: {response.text}")
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                import traceback
                traceback.print_exc()
                break
            
    print(f"\n--- CSV Export and Zip Cleaning Summary ---")
    print(f"Total unique contacts fetched matching segment: {total_contacts_fetched}")
    print(f"Total contacts written to CSV ({CSV_FILENAME}): {total_contacts_written_to_csv}")
    print(f"Zip code updates attempted: {zip_updates_attempted}")
    print(f"Zip code updates successful: {zip_updates_successful}")


if __name__ == "__main__":
    if not ACCESS_TOKEN or not LOCATION_ID:
        print("ERROR: PSF_ACCESS_TOKEN and PSF_LOCATION_ID must be set.")
    else:
        process_contacts_for_export_and_clean_zip(
            ACCESS_TOKEN,
            LOCATION_ID,
            TAG_LEAD,
            TAG_PURCHASE,
            TAG_EXCLUSION
        )
    
    print("\nScript execution finished.")