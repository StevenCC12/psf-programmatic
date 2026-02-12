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

CONTACTS_PAGE_LIMIT = 100 # Number of contacts to fetch per API call

# --- Target Custom Field Details ---
TARGET_CUSTOM_FIELD_ID = "WP1s0IK166ih8iIVUhdM" # ID you found for the radio button field
TARGET_CUSTOM_FIELD_VALUE = "20 000 SEK eller mer" # The specific option you're looking for

# --- Basic Sanity Checks for Configuration ---
if not ACCESS_TOKEN:
    print("ERROR: PSF_ACCESS_TOKEN is not configured in your .env file.")
    exit()
if not LOCATION_ID:
    print("ERROR: PSF_LOCATION_ID is not configured in your .env file. It is required.")
    exit()
if not TARGET_CUSTOM_FIELD_ID:
    print("ERROR: TARGET_CUSTOM_FIELD_ID is not set in the script.")
    exit()
if not TARGET_CUSTOM_FIELD_VALUE:
    print("ERROR: TARGET_CUSTOM_FIELD_VALUE is not set in the script.")
    exit()

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Version": API_VERSION,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- Helper Functions ---

def search_contacts_by_custom_field_value(
    location_id_param, 
    custom_field_id_to_filter, 
    custom_field_target_value, 
    page_size=CONTACTS_PAGE_LIMIT, 
    search_after_cursor=None
):
    """
    Searches for contacts based on a specific custom field ID and its value.
    """
    url = f"{BASE_URL}/contacts/search"
    
    filters_payload = [
        {
            "field": f"customFields.{custom_field_id_to_filter}", # Using the format customFields.<ID>
            "operator": "eq",
            "value": custom_field_target_value
        }
    ]
    
    payload = {
        "locationId": location_id_param,
        "pageLimit": page_size,
        "filters": filters_payload
    }

    if search_after_cursor:
        payload["searchAfter"] = search_after_cursor
    else:
        payload["page"] = 1 # Initial request uses page number
    
    print(f"INFO: Searching contacts with payload: {json.dumps(payload, indent=2)}")
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error during search: {http_err} - {response.text}")
        try:
            error_details = http_err.response.json()
            print(f"ERROR DETAILS: {json.dumps(error_details, indent=2)}")
        except json.JSONDecodeError:
            pass 
    except Exception as e:
        print(f"ERROR: Exception during search: {e}")
    return None

# --- Main Logic ---
def main():
    all_matching_contacts_count = 0
    current_search_after_cursor = None 
    has_more_pages = True

    print(f"INFO: Starting script to find contacts with Custom Field ID '{TARGET_CUSTOM_FIELD_ID}' set to '{TARGET_CUSTOM_FIELD_VALUE}'.")
    print(f"INFO: Using Location ID: '{LOCATION_ID}'.")

    while has_more_pages:
        print(f"\nINFO: Fetching page of contacts (searchAfter cursor: {current_search_after_cursor})...")
        
        search_result = search_contacts_by_custom_field_value(
            LOCATION_ID,
            TARGET_CUSTOM_FIELD_ID,
            TARGET_CUSTOM_FIELD_VALUE,
            page_size=CONTACTS_PAGE_LIMIT,
            search_after_cursor=current_search_after_cursor
        )

        print(f"DEBUG: Full API search response: {json.dumps(search_result, indent=2)}")

        if search_result is None:
            print("WARN: Failed to fetch contacts (search_contacts_by_custom_field_value returned None). Ending.")
            break
        
        if "contacts" not in search_result:
            print(f"WARN: 'contacts' key not found in search response. Ending.")
            break
        
        contacts_on_this_page = search_result.get("contacts", [])
        api_total_count = search_result.get("total") # As per your debug output
        
        if not contacts_on_this_page:
            if api_total_count is not None:
                 print(f"INFO: API indicates {api_total_count} total matching contacts based on its 'total' field in response.")
            print("INFO: No more contacts found in this batch (contacts array is empty).")
            has_more_pages = False
            break 
        
        print(f"INFO: Found {len(contacts_on_this_page)} contacts in this batch.")

        for contact_data in contacts_on_this_page:
            all_matching_contacts_count += 1
            contact_id = contact_data.get("id")
            contact_name = contact_data.get("firstNameLowerCase", "") + " " + contact_data.get("lastNameLowerCase", "")
            contact_email = contact_data.get("email")
            
            print(f"\n--- Found Contact ---")
            print(f"  ID: {contact_id}")
            print(f"  Name: {contact_name.strip()}")
            print(f"  Email: {contact_email}")

            # Verify and print the specific custom field value from the response
            contact_custom_fields = contact_data.get("customFields", [])
            found_target_cf_in_response = False
            for cf in contact_custom_fields:
                if cf.get("id") == TARGET_CUSTOM_FIELD_ID:
                    print(f"  Custom Field (ID: {TARGET_CUSTOM_FIELD_ID}) Value: {cf.get('value')}")
                    found_target_cf_in_response = True
                    break
            if not found_target_cf_in_response:
                print(f"  Custom Field (ID: {TARGET_CUSTOM_FIELD_ID}) was not found in this contact's response data (odd, as filter should match).")
        
        # Pagination Logic
        # Based on your HTML doc for Search API request and typical cursor patterns.
        # The debug output '{"contacts": [], "total": 0, "traceId": "..."}' did not have searchAfter.
        # We need to see what a non-empty response with multiple pages returns for 'searchAfter'.
        # Assuming it's a top-level field in search_result for now, or in last contact.
        
        next_cursor = None
        if "searchAfter" in search_result and search_result["searchAfter"]:
            next_cursor = search_result["searchAfter"]
            print(f"INFO: Using 'searchAfter' cursor from response root for next page: {next_cursor}")
        elif contacts_on_this_page and 'searchAfter' in contacts_on_this_page[-1] and contacts_on_this_page[-1]['searchAfter']:
            next_cursor = contacts_on_this_page[-1]['searchAfter']
            print(f"INFO: Using 'searchAfter' cursor from last contact for next page: {next_cursor}")
        
        if next_cursor:
            current_search_after_cursor = next_cursor
            has_more_pages = True
        else:
            has_more_pages = False
            if len(contacts_on_this_page) == CONTACTS_PAGE_LIMIT:
                print("WARN: Received a full page of contacts, but no 'searchAfter' cursor was found for the next page. Assuming end of results.")
            else:
                print("INFO: No 'searchAfter' cursor found and/or fewer contacts than page limit. End of results.")

    print(f"\n--- Script Finished ---")
    print(f"Total matching contacts found and printed: {all_matching_contacts_count}")

if __name__ == "__main__":
    # Ensure your .env file has:
    # PSF_ACCESS_TOKEN="your_access_token"
    # PSF_LOCATION_ID="your_location_id"
    main()