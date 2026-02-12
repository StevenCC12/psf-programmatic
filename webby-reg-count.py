import requests
import os
import json
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration ---
ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
LOCATION_ID = os.getenv("PSF_LOCATION_ID") 

API_VERSION = "2021-07-28"
BASE_URL = "https://services.leadconnectorhq.com"

PRIMARY_TAG_TO_SEARCH = "lead: amazon masterclass (swe)"
WEBINAR_TAG_PREFIX = "webinar week: "
CONTACTS_PAGE_LIMIT = 100 

WEBINAR_COUNT_CUSTOM_FIELD_ID = "gWzoL0bdqlhnvbp0mBiM" 

if not WEBINAR_COUNT_CUSTOM_FIELD_ID:
    print("ERROR: You must configure WEBINAR_COUNT_CUSTOM_FIELD_ID.")
    exit()
if not LOCATION_ID:
    print("ERROR: PSF_LOCATION_ID is not configured in your .env file.")
    exit()
if not ACCESS_TOKEN:
    print("ERROR: PSF_ACCESS_TOKEN is not configured in your .env file.")
    exit()

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Version": API_VERSION,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- Helper Functions ---

def search_contacts(tag_to_filter, location_id_param, page_size=CONTACTS_PAGE_LIMIT, search_after_cursor=None):
    """
    Searches for contacts based on a tag, using the structured 'filters' array.
    Uses 'searchAfter' for cursor pagination or 'page' for initial/standard pagination.
    """
    url = f"{BASE_URL}/contacts/search"
    
    # Construct filters array based on the new documentation
    filters_payload = [
        {
            "field": "tags",
            "operator": "eq",  # Use "eq" for exact match for the tag in the array.
                               # Or "contains" if you want contacts that have this tag among others.
                               # The documentation example for "tags" with "eq" uses an array for value.
            "value": [tag_to_filter] 
        }
    ]
    
    payload = {
        "locationId": location_id_param,
        "pageLimit": page_size,
        "filters": filters_payload
    }

    if search_after_cursor:
        payload["searchAfter"] = search_after_cursor
        # Do not include 'page' when 'searchAfter' is used, as per documentation
    else:
        payload["page"] = 1 # For the first request, or if not using cursor pagination
    
    print(f"INFO: Searching contacts with payload: {json.dumps(payload, indent=2)}")
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error during search: {http_err} - {response.text}")
        # Print more details from the response if available
        try:
            error_details = http_err.response.json()
            print(f"ERROR DETAILS: {json.dumps(error_details, indent=2)}")
        except json.JSONDecodeError:
            pass # No JSON in error response
    except Exception as e:
        print(f"ERROR: Exception during search: {e}")
    return None

def update_contact_custom_fields(contact_id, custom_fields_payload):
    url = f"{BASE_URL}/contacts/{contact_id}"
    payload = {
        "customFields": custom_fields_payload
    }
    print(f"INFO: Updating contact {contact_id} with customFields: {json.dumps(custom_fields_payload)}")
    try:
        response = requests.put(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        contact_info = response.json().get('contact', {})
        print(f"SUCCESS: Updated custom fields for contact {contact_id}. Contact API ID: {contact_info.get('id', 'N/A')}")
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error updating custom fields for {contact_id}: {http_err} - {response.text}")
    except Exception as e:
        print(f"ERROR: Exception updating custom fields for {contact_id}: {e}")
    return False

# --- Main Logic ---
def main():
    all_contacts_processed_count = 0
    contacts_updated_count = 0
    current_search_after_cursor = None # Stores the cursor for the next page
    has_more_pages = True

    print(f"INFO: Starting script to update 'Webinar Registration Count' custom field.")
    print(f"INFO: Using Location ID: '{LOCATION_ID}' for search.")
    print(f"INFO: Target Custom Field Identifier: ID='{WEBINAR_COUNT_CUSTOM_FIELD_ID}'")

    while has_more_pages:
        print(f"\nINFO: Fetching page of contacts (searchAfter cursor: {current_search_after_cursor})...")
        search_result = search_contacts(
            PRIMARY_TAG_TO_SEARCH, 
            LOCATION_ID, 
            page_size=CONTACTS_PAGE_LIMIT,
            search_after_cursor=current_search_after_cursor # Pass the current cursor
        )

        print(f"DEBUG: Full API search response: {json.dumps(search_result, indent=2)}")

        if search_result is None: # Indicates an error occurred in search_contacts
            print("WARN: Failed to fetch contacts (search_contacts returned None). Ending.")
            break
        
        if "contacts" not in search_result:
            print(f"WARN: 'contacts' key not found in search response. Ending.")
            break
        
        contacts = search_result.get("contacts", [])
        
        if not contacts:
            meta_total = search_result.get("total", "N/A (not in response)") # API response shows total at root
            # The new docs also show meta.totalCount, check both if one fails.
            # meta = search_result.get("meta", {})
            # total_contacts_from_meta = meta.get("totalCount", meta.get("total"))
            print(f"INFO: API search returned {meta_total} total matching contacts in this response structure.")
            print("INFO: No contacts found in this batch (contacts array is empty).")
            has_more_pages = False # No contacts, so no more pages
            break # Exit the while loop
        
        print(f"INFO: Found {len(contacts)} contacts in this batch.")

        for contact_data in contacts: # Renamed to avoid conflict with outer 'contacts' list
            all_contacts_processed_count += 1
            contact_id = contact_data.get("id")
            contact_tags = contact_data.get("tags", [])
            existing_custom_fields_from_search = contact_data.get("customFields", []) 

            if not contact_id:
                print(f"WARN: Skipping contact due to missing ID: {contact_data}")
                continue

            print(f"\nINFO: Processing contact ID: {contact_id}")

            webinar_registration_count = 0
            for tag in contact_tags:
                if tag.startswith(WEBINAR_TAG_PREFIX):
                    webinar_registration_count += 1
            
            print(f"INFO: Contact ID: {contact_id} has {webinar_registration_count} webinar registrations.")
            new_cf_value_str = str(webinar_registration_count)
            
            custom_fields_for_put_request = []
            our_target_field_found_in_existing = False
            original_value_of_target_field = None
            
            for cf_from_search in existing_custom_fields_from_search:
                current_field_def_id = cf_from_search.get("id")
                current_field_val_for_contact = cf_from_search.get("value")

                if current_field_def_id == WEBINAR_COUNT_CUSTOM_FIELD_ID:
                    custom_fields_for_put_request.append({
                        "id": WEBINAR_COUNT_CUSTOM_FIELD_ID,
                        "field_value": new_cf_value_str
                    })
                    our_target_field_found_in_existing = True
                    original_value_of_target_field = current_field_val_for_contact
                else:
                    if current_field_def_id:
                        custom_fields_for_put_request.append({
                            "id": current_field_def_id,
                            "field_value": current_field_val_for_contact 
                        })

            if not our_target_field_found_in_existing:
                custom_fields_for_put_request.append({
                    "id": WEBINAR_COUNT_CUSTOM_FIELD_ID,
                    "field_value": new_cf_value_str
                })

            needs_api_update = False
            if our_target_field_found_in_existing:
                if str(original_value_of_target_field) != new_cf_value_str:
                    needs_api_update = True
                    print(f"INFO: Value change detected for custom field ID='{WEBINAR_COUNT_CUSTOM_FIELD_ID}'. Old: '{original_value_of_target_field}', New: '{new_cf_value_str}'.")
            else:
                needs_api_update = True
                print(f"INFO: Custom field ID='{WEBINAR_COUNT_CUSTOM_FIELD_ID}' will be added.")

            if needs_api_update:
                if update_contact_custom_fields(contact_id, custom_fields_for_put_request):
                    contacts_updated_count += 1
                time.sleep(0.5)
            else:
                print(f"INFO: Contact ID: {contact_id}. Custom field (ID='{WEBINAR_COUNT_CUSTOM_FIELD_ID}') is already up-to-date with value '{new_cf_value_str}'. No API update sent.")
        
        # Pagination logic based on searchAfter from the response
        # The initial user-provided example showed searchAfter inside each contact.
        # The new HTML doc request body section shows searchAfter as a top-level field for the *next* request.
        # Let's assume the response structure from the user's FIRST prompt is correct for where to find searchAfter.
        
        # If the API response is structured like:
        # { "contacts": [..., {"id": "last_contact_id", "searchAfter": ["cursor_val"]}], "total": X }
        # then this is how we get the cursor from the last contact.
        
        # However, the new HTML documentation for Search Contacts mentions searchAfter in the context of the *request*.
        # The *response* structure for pagination cursors often includes it in a 'meta' object or a top-level field
        # like 'nextPageToken' or 'searchAfter' that applies to the whole batch.
        # Given the previous debug output was: { "contacts": [], "total": 0, "traceId": "..." }
        # it did NOT contain a per-contact searchAfter.
        # The API doc for Search Contacts (request body) shows 'searchAfter' (array; optional)
        # Example: `[10, "ABC"]`. This should come from the *previous response*.
        # Let's check if search_result (the overall JSON response) has a top-level 'searchAfter'.

        if "searchAfter" in search_result and search_result["searchAfter"]:
            current_search_after_cursor = search_result["searchAfter"]
            print(f"INFO: Next page using 'searchAfter' cursor from response root: {current_search_after_cursor}")
            has_more_pages = True # Continue if cursor is found
        elif contacts and 'searchAfter' in contacts[-1] and contacts[-1]['searchAfter']: # Fallback to per-contact if API behaves that way
            current_search_after_cursor = contacts[-1]['searchAfter']
            print(f"INFO: Next page using 'searchAfter' cursor from last contact: {current_search_after_cursor}")
            has_more_pages = True
        else:
            # No 'searchAfter' cursor found in response root or last contact.
            # If 'page' was used, and total pages > current page, we could increment page.
            # But the goal is to use cursor pagination if available.
            # If we got fewer contacts than pageLimit, it's the last page.
            if len(contacts) < CONTACTS_PAGE_LIMIT:
                print("INFO: Last page indicated by returned contacts count being less than page limit.")
            else:
                print("INFO: No 'searchAfter' cursor found in response. Assuming no more pages.")
            has_more_pages = False
            current_search_after_cursor = None # Reset cursor

    print(f"\n--- Script Finished ---")
    print(f"Total contacts scanned: {all_contacts_processed_count}")
    print(f"Total contacts where custom field (ID='{WEBINAR_COUNT_CUSTOM_FIELD_ID}') was updated/added: {contacts_updated_count}")

if __name__ == "__main__":
    main()