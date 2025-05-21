import csv
import requests
import os
import json
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

ORIGINAL_BUYERS_CSV_FILENAME = "knaa_buyers_from_spreadsheet.csv"
EMAIL_COLUMN_NAME_IN_ORIGINAL_CSV = "email"
CRM_TAG_TO_CHECK = "knaa customer" # The exact tag string we're looking for

# New CSV for discrepancies
DISCREPANCY_CSV_FILENAME = "crm_knaa_discrepancies_export.csv"
DISCREPANCY_CSV_HEADERS = ["email", "phone", "fn", "ln", "zip", "ct", "country"]

# API Details
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
PAGE_LIMIT = 100
DELAY_BETWEEN_API_CALLS = 0.5
DELAY_AFTER_UPDATE = 1


def load_emails_from_csv(csv_filepath, email_column_header):
    emails = set()
    try:
        with open(csv_filepath, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            if email_column_header not in reader.fieldnames:
                print(f"ERROR: Email column '{email_column_header}' not found in CSV headers: {reader.fieldnames}")
                return None
            for row_num, row in enumerate(reader, 1):
                email_value = row.get(email_column_header)
                if email_value and email_value.strip():
                    emails.add(email_value.strip().lower())
                else:
                    print(f"WARNING: Empty or missing email in {csv_filepath} row {row_num + 1}.")
            print(f"INFO: Loaded {len(emails)} unique emails from {csv_filepath}")
            return emails
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at '{csv_filepath}'.")
        return None
    except Exception as e:
        print(f"ERROR: Could not read CSV file '{csv_filepath}': {e}")
        return None

def fetch_crm_contacts_with_potential_tag(access_token, location_id, tag_value_to_contain):
    """
    Fetches contacts from CRM where any tag might *contain* the tag_value_to_contain.
    This is an initial broad fetch.
    """
    if not all([access_token, location_id, tag_value_to_contain]):
        print("ERROR: Access token, location ID, and tag value are required.")
        return None
    all_contacts = []
    page = 1
    search_endpoint = f"{BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {access_token}", "Version": API_VERSION,
        "Content-Type": "application/json", "Accept": "application/json"
    }
    print(f"INFO: Broadly fetching contacts from CRM where a tag might contain '{tag_value_to_contain}'...")
    while True:
        print(f"  Fetching page {page}...")
        payload = {"locationId": location_id, "pageLimit": PAGE_LIMIT, "page": page,
                   "filters": [{"field": "tags", "operator": "contains", "value": tag_value_to_contain}]}
        try:
            response = requests.post(search_endpoint, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
            contacts_on_page = response_data.get("contacts", [])
            if not contacts_on_page:
                print(f"  No more contacts found on page {page} with a tag containing '{tag_value_to_contain}'.")
                break
            all_contacts.extend(contacts_on_page)
            print(f"  Fetched {len(contacts_on_page)} on page {page}. Total potentially matching: {len(all_contacts)}")
            if len(contacts_on_page) < PAGE_LIMIT:
                print("  Likely fetched all potentially matching contacts (last page was not full).")
                break
            page += 1
            time.sleep(DELAY_BETWEEN_API_CALLS)
        except Exception as e:
            print(f"ERROR: During broad contact fetching (page {page}): {e}")
            if 'response' in locals() and hasattr(response, 'text'): print(f"  Response: {response.text}")
            return None
    print(f"INFO: Finished broad fetching. Total contacts potentially matching tag: {len(all_contacts)}")
    return all_contacts

def update_contact_detail(contact_id, field_to_update, new_value, access_token):
    if not contact_id or field_to_update is None or new_value is None:
        print(f"ERROR: contact_id, field_to_update, and new_value are required for update. "
              f"ID: {contact_id}, Field: {field_to_update}, Value: {new_value}")
        return False
    endpoint = f"{BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}", "Version": API_VERSION,
        "Content-Type": "application/json", "Accept": "application/json"
    }
    payload = {field_to_update: new_value}
    print(f"  Attempting to update contact {contact_id}: set '{field_to_update}' to '{new_value}'...")
    try:
        response = requests.put(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        print(f"  Successfully updated contact {contact_id}. Status: {response.status_code}")
        time.sleep(DELAY_AFTER_UPDATE)
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"  HTTP error updating contact {contact_id}: {http_err}")
        if hasattr(response, 'text'): print(f"  Response: {response.text}")
    except Exception as e:
        print(f"  Unexpected error updating contact {contact_id}: {e}")
    time.sleep(DELAY_BETWEEN_API_CALLS)
    return False

def process_discrepancies_and_export(csv_emails_set, crm_contacts_with_exact_tag, exact_tag_value, access_token_for_updates):
    if csv_emails_set is None or crm_contacts_with_exact_tag is None:
        print("ERROR: Cannot process discrepancies due to earlier errors.")
        return

    discrepancies_found_count = 0
    zip_updates_attempted = 0
    zip_updates_successful = 0

    print(f"\n--- Processing Discrepancies (Contacts with EXACT tag '{exact_tag_value}') & Exporting to {DISCREPANCY_CSV_FILENAME} ---")

    with open(DISCREPANCY_CSV_FILENAME, mode='w', newline='', encoding='utf-8') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(DISCREPANCY_CSV_HEADERS)

        for contact in crm_contacts_with_exact_tag: # Iterate through the precisely filtered list
            crm_email = contact.get("email")
            contact_id = contact.get('id', 'Unknown ID')

            if crm_email and crm_email.strip():
                crm_email_lower = crm_email.strip().lower()
                if crm_email_lower not in csv_emails_set:
                    discrepancies_found_count += 1
                    print(f"\nProcessing Discrepancy #{discrepancies_found_count}: Contact ID {contact_id}, Email: {crm_email}")

                    phone_val = contact.get('phone', '')
                    fn_api = contact.get('firstNameLowerCase')
                    fn_title = (fn_api or '').title()
                    ln_api = contact.get('lastNameLowerCase')
                    ln_title = (ln_api or '').title()
                    original_zip = contact.get('postalCode', '')
                    city_api = contact.get('city')
                    city_title = (city_api or '').title()
                    country_val = contact.get('country', '')
                    current_zip_for_csv_and_crm = original_zip if original_zip is not None else ''

                    if isinstance(original_zip, str) and ' ' in original_zip:
                        cleaned_zip = original_zip.replace(' ', '').strip()
                        print(f"  Original Zip: '{original_zip}', Cleaned Zip: '{cleaned_zip}'")
                        if cleaned_zip != original_zip:
                            zip_updates_attempted += 1
                            if update_contact_detail(contact_id, "postalCode", cleaned_zip, access_token_for_updates):
                                zip_updates_successful += 1
                            current_zip_for_csv_and_crm = cleaned_zip 
                    
                    csv_row_data = [
                        crm_email, phone_val, fn_title, ln_title,
                        current_zip_for_csv_and_crm, city_title, country_val
                    ]
                    csv_writer.writerow(csv_row_data)
            elif contact_id != 'Unknown ID':
                 print(f"\nWARNING: CRM Contact ID {contact_id} (with exact tag '{exact_tag_value}') has no primary email. Not added to discrepancy CSV.")

    print(f"\n--- Discrepancy Processing Summary ---")
    print(f"Found {discrepancies_found_count} contacts in CRM with exact tag '{exact_tag_value}' (that have email) not in the original CSV.")
    print(f"These {discrepancies_found_count} contacts have been written to '{DISCREPANCY_CSV_FILENAME}'.")
    print(f"Zip code updates attempted for these discrepancy contacts: {zip_updates_attempted}")
    print(f"Zip code updates successful: {zip_updates_successful}")


if __name__ == "__main__":
    if not ACCESS_TOKEN or not LOCATION_ID:
        print("CRITICAL ERROR: PSF_ACCESS_TOKEN and PSF_LOCATION_ID must be set.")
    else:
        true_customer_emails = load_emails_from_csv(ORIGINAL_BUYERS_CSV_FILENAME, EMAIL_COLUMN_NAME_IN_ORIGINAL_CSV)

        if true_customer_emails is not None:
            # 1. Fetch contacts where any tag might *contain* CRM_TAG_TO_CHECK
            potentially_tagged_crm_contacts = fetch_crm_contacts_with_potential_tag(ACCESS_TOKEN, LOCATION_ID, CRM_TAG_TO_CHECK)

            if potentially_tagged_crm_contacts is not None:
                # 2. Filter this list in Python to find contacts with the *exact* tag
                print(f"\nINFO: API returned {len(potentially_tagged_crm_contacts)} contacts where a tag might contain '{CRM_TAG_TO_CHECK}'.")
                print("INFO: Now filtering for contacts that have the *exact* tag...")
                
                exact_match_crm_contacts = []
                for contact_from_api in potentially_tagged_crm_contacts:
                    contact_tags_list = contact_from_api.get("tags", []) 
                    if CRM_TAG_TO_CHECK in contact_tags_list: # Python's 'in' on a list checks for exact element match
                        exact_match_crm_contacts.append(contact_from_api)
                
                print(f"INFO: Found {len(exact_match_crm_contacts)} contacts with the exact tag '{CRM_TAG_TO_CHECK}'.")

                # 3. Process discrepancies using the accurately filtered list
                process_discrepancies_and_export(
                    true_customer_emails, 
                    exact_match_crm_contacts, 
                    CRM_TAG_TO_CHECK, 
                    ACCESS_TOKEN
                )

    print("\nScript execution finished.")