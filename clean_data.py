import requests
import os
import json
import time
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
UPDATE_ENDPOINT_TEMPLATE = "/contacts/{contactId}"
PAGE_LIMIT = 100

# --- Helper Function to Make API Calls (Search) ---
def search_contacts_api(payload, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during search: {http_err} - {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error during search: {req_err}")
    except ValueError as json_err:
        print(f"JSON decode error during search: {json_err} - {response.text}")
    return None

# --- Helper Function to Make API Calls (Update) ---
def update_contact_api(contact_id, update_data, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    url = f"{BASE_URL}{UPDATE_ENDPOINT_TEMPLATE.format(contactId=contact_id)}"
    try:
        response = requests.put(url, headers=headers, json=update_data)
        response.raise_for_status()
        # Check for "succeded" (with typo) or "succeeded" (correct spelling)
        if response.json().get("succeded") is True or response.json().get("succeeded") is True:
            return response.json()
        else:
            print(f"WARN: Update for contact {contact_id} reported not successful by API: {response.text}")
            return response.json() # Still return JSON for inspection
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error updating contact {contact_id}: {http_err} - {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error updating contact {contact_id}: {req_err}")
    except ValueError as json_err: # Includes JSONDecodeError
        print(f"JSON decode error updating contact {contact_id}: {json_err} - {response.text}")
    return None

# --- Main Data Cleaning and Update Function ---
def clean_and_update_contacts_v2():
    if not PSF_ACCESS_TOKEN or not PSF_LOCATION_ID:
        print("ERROR: PSF_ACCESS_TOKEN or PSF_LOCATION_ID not found in environment variables.")
        print("Please ensure your .env file is correctly set up with these values.")
        return

    print(f"INFO: Starting contact cleaning (v2) for Location ID: {PSF_LOCATION_ID}")

    page_number = 1
    total_contacts_processed = 0
    total_contacts_updated = 0
    contacts_needing_update_overall = 0

    while True:
        search_payload = {
            "locationId": PSF_LOCATION_ID,
            "pageLimit": PAGE_LIMIT,
            "page": page_number
        }

        print(f"\nINFO: Fetching contacts page {page_number}...")
        response_data = search_contacts_api(search_payload, PSF_ACCESS_TOKEN)

        if not response_data:
            print("ERROR: Failed to fetch data from search API on page {page_number}. Aborting further processing for this page.")
            break

        contacts = response_data.get("contacts", [])
        
        if not contacts:
            print("INFO: No more contacts found.")
            break
        
        api_total_contacts = response_data.get("total")
        if page_number == 1 and api_total_contacts is not None:
             print(f"INFO: API reports a total of {api_total_contacts} contacts for this location.")

        print(f"INFO: Processing {len(contacts)} contacts from page {page_number}...")

        for contact in contacts:
            total_contacts_processed += 1
            contact_id = contact.get("id")

            if not contact_id:
                print(f"WARNING: Contact found without an ID. Skipping: {contact}")
                continue

            payload_for_update = {}
            log_changes = []

            # Get original values from the search response for comparison and source
            original_fn_lower = contact.get('firstNameLowerCase')
            original_ln_lower = contact.get('lastNameLowerCase')
            original_city = contact.get('city')
            original_postal_code = contact.get('postalCode')

            # 1. Clean postalCode
            if isinstance(original_postal_code, str):
                cleaned_postal_code = original_postal_code.replace(" ", "")
                if cleaned_postal_code != original_postal_code:
                    payload_for_update["postalCode"] = cleaned_postal_code
                    log_changes.append(f"Zip '{original_postal_code}' -> '{cleaned_postal_code}'")

            # 2. Handle First Name / Last Name (splitting and title casing)
            # API fields for update are 'firstName' and 'lastName'
            fn_processed = False
            ln_processed_by_split = False

            if isinstance(original_fn_lower, str) and original_fn_lower.strip():
                name_parts = original_fn_lower.strip().split(maxsplit=1)
                
                current_target_fn = name_parts[0].title()
                payload_for_update['firstName'] = current_target_fn # Set title-cased first part
                fn_processed = True
                # Log change if different from original (hypothetical original, as we only have lowercase)
                # or simply log that it was set.
                if current_target_fn != original_fn_lower : # Check against original lowercase to see if casing changed
                    log_changes.append(f"FN '{original_fn_lower}' -> '{current_target_fn}' (title-cased)")


                if len(name_parts) > 1: # Multi-word first name, second part becomes LN
                    current_target_ln = name_parts[1].title()
                    payload_for_update['lastName'] = current_target_ln
                    ln_processed_by_split = True
                    log_changes.append(f"LN from FN split -> '{current_target_ln}' (was part of '{original_fn_lower}')")
            
            # Handle lastName if not set by split and original_ln_lower exists
            if not ln_processed_by_split and isinstance(original_ln_lower, str):
                current_target_ln = original_ln_lower.title()
                payload_for_update['lastName'] = current_target_ln
                if current_target_ln != original_ln_lower: # Check against original lowercase
                     log_changes.append(f"LN '{original_ln_lower}' -> '{current_target_ln}' (title-cased)")
            
            # Handle cases where fn_lower or ln_lower might have been empty strings
            # and we want to ensure the payload reflects an empty string for the update.
            if isinstance(original_fn_lower, str) and not original_fn_lower.strip() and not fn_processed:
                payload_for_update['firstName'] = "" # Ensure empty if original was just spaces
                log_changes.append(f"FN cleared (was spaces)")
            
            if isinstance(original_ln_lower, str) and not original_ln_lower.strip() and not ln_processed_by_split and 'lastName' not in payload_for_update:
                payload_for_update['lastName'] = "" # Ensure empty if original was just spaces
                log_changes.append(f"LN cleared (was spaces)")


            # 3. Title case city
            if isinstance(original_city, str) and original_city: # Ensure not empty
                cleaned_city = original_city.title()
                if cleaned_city != original_city:
                    payload_for_update["city"] = cleaned_city
                    log_changes.append(f"City '{original_city}' -> '{cleaned_city}'")
            
            # Perform update if any changes were made
            if payload_for_update:
                contacts_needing_update_overall +=1
                print(f"  - Contact ID {contact_id}: Changes - {'; '.join(log_changes)}")
                print(f"    INFO: Updating with payload: {payload_for_update}")
                
                update_response = update_contact_api(contact_id, payload_for_update, PSF_ACCESS_TOKEN)
                if update_response and (update_response.get("succeded") is True or update_response.get("succeeded") is True) :
                    total_contacts_updated += 1
                    print(f"    SUCCESS: Contact ID {contact_id} updated.")
                else:
                    print(f"    ERROR: Failed to update contact ID {contact_id}. Response: {update_response}")
                
                time.sleep(0.25) # Be kind to the API

        if len(contacts) < PAGE_LIMIT:
            print("INFO: Fetched fewer contacts than page limit, assuming this was the last page.")
            break
        
        page_number += 1
        # time.sleep(0.5) # Delay between fetching pages if needed

    print("\n--- Cleaning and Update Summary (v2) ---")
    print(f"Total contacts processed: {total_contacts_processed}")
    print(f"Contacts with potential changes identified: {contacts_needing_update_overall}")
    print(f"Contacts successfully updated: {total_contacts_updated}")
    print("INFO: Script finished.")

# --- Run the Cleaning and Update Process ---
if __name__ == "__main__":
    clean_and_update_contacts_v2()