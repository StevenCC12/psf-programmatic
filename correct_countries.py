import requests
import os
import json
import time
import phonenumbers # For parsing phone numbers and getting country codes
# from phonenumbers import geocoder # Not strictly needed for just region_code_for_number

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
# ACCESS_TOKEN = "YOUR_PSF_ACCESS_TOKEN_HERE" # Replace with your actual token
# LOCATION_ID = "YOUR_PSF_LOCATION_ID_HERE"  # Replace with your actual location ID


# API Details
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
PAGE_LIMIT = 100 # Max allowed by API for search, process in batches of 100
DELAY_BETWEEN_API_CALLS = 1 # Seconds to wait between most API calls
DELAY_AFTER_UPDATE = 2 # Slightly longer delay after an update operation


def update_contact_country(contact_id, new_country_code, access_token):
    """
    Updates the country for a specific contact.
    """
    if not contact_id or not new_country_code:
        print(f"Error: contact_id and new_country_code are required for update.")
        return False

    endpoint = f"{BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "country": new_country_code # API expects ISO 2-letter codes like "US", "GB"
    }

    print(f"Attempting to update contact {contact_id} country to: {new_country_code}...")
    try:
        response = requests.put(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Successfully updated contact {contact_id}. Status: {response.status_code}")
        time.sleep(DELAY_AFTER_UPDATE) # Pause after a successful update
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error updating contact {contact_id}: {http_err}")
        print(f"Status Code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error updating contact {contact_id}: {req_err}")
    except Exception as e:
        print(f"Unexpected error updating contact {contact_id}: {e}")
    
    time.sleep(DELAY_BETWEEN_API_CALLS) # Pause even if update failed before next operation
    return False


def process_contacts_to_update_country(access_token, location_id):
    """
    Fetches contacts currently set to "SE" country AND having a phone number,
    derives country from phone, and updates if different and valid.
    """
    if not all([access_token, location_id]):
        print("Error: ACCESS_TOKEN and LOCATION_ID must be set for processing.")
        return

    # Updated print message to reflect the new filter
    print(f"Starting process to update country field for contacts in Location ID: {location_id}")
    print("Filtering for contacts with Country='SE' AND a non-empty Phone number.")
    
    page = 1
    contacts_processed_count = 0
    contacts_updated_count = 0
    contacts_failed_update_count = 0
    contacts_phone_parse_failed_count = 0 # This count might be lower now due to pre-filtering
    
    search_endpoint = f"{BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    while True:
        print(f"\nFetching page {page} of contacts with country 'SE' and non-empty phone...")
        search_payload = {
            "locationId": location_id,
            "pageLimit": PAGE_LIMIT,
            "page": page,
            "filters": [
                {
                    "field": "country",
                    "operator": "eq",
                    "value": "SE" 
                },
                { # New filter added here
                    "field": "phone",
                    "operator": "exists"
                }
            ],
            "sort": [ 
                {
                    "field": "dateAdded",
                    "direction": "asc" 
                }
            ]
        }

        # ... (the rest of the try-except block for making the request and processing contacts)
        # The inner logic for parsing phone and updating country remains the same,
        # as it will now only operate on contacts that are guaranteed to have a phone number.
        # The check 'if not phone_number_str:' will still be there as a safeguard,
        # but ideally won't be triggered often due to the API filter.
        try:
            response = requests.post(search_endpoint, headers=headers, json=search_payload)
            response.raise_for_status()
            response_data = response.json()
            contacts_on_page = response_data.get("contacts", [])
            total_matching_api = response_data.get("total", 0)

            if not contacts_on_page:
                print(f"No more contacts found matching criteria on page {page} (or API returned empty list).")
                break
            
            print(f"Found {len(contacts_on_page)} contacts on page {page}. (API reports {total_matching_api} total matching criteria)")

            for contact in contacts_on_page:
                contacts_processed_count += 1
                contact_id = contact.get("id")
                current_country = contact.get("country") 
                phone_number_str = contact.get("phone") # Should always exist now due to filter
                email = contact.get("email", "N/A")

                print(f"\nProcessing Contact ID: {contact_id}, Email: {email}, Current Country: {current_country}, Phone: {phone_number_str}")

                # This check is still good as a safeguard, though the API filter should handle it
                if not phone_number_str: 
                    print(f"  - Warning: Phone number is empty for contact {contact_id} despite API filter. Skipping.")
                    contacts_phone_parse_failed_count +=1 # Count this anomaly
                    continue

                derived_country_code = None
                try:
                    parsed_phone = phonenumbers.parse(phone_number_str, None) 
                    
                    if phonenumbers.is_valid_number(parsed_phone):
                        derived_country_code = phonenumbers.region_code_for_number(parsed_phone)
                        print(f"  - Phone parsed: Valid. Derived Country Code: {derived_country_code}")
                    else:
                        print(f"  - Phone parsed: Invalid number ({phone_number_str}).")
                        contacts_phone_parse_failed_count +=1
                        
                except phonenumbers.NumberParseException as e:
                    print(f"  - Phone number parsing error for '{phone_number_str}': {e}")
                    contacts_phone_parse_failed_count +=1
                except Exception as e:
                    print(f"  - Unexpected error during phone parsing for '{phone_number_str}': {e}")
                    contacts_phone_parse_failed_count +=1

                if derived_country_code and derived_country_code != current_country:
                    print(f"  - Action: Current country is '{current_country}', derived is '{derived_country_code}'. Updating contact.")
                    if update_contact_country(contact_id, derived_country_code, access_token):
                        contacts_updated_count += 1
                    else:
                        contacts_failed_update_count += 1
                elif derived_country_code and derived_country_code == current_country:
                    print(f"  - Action: Derived country '{derived_country_code}' matches current '{current_country}'. No update needed.")
                elif not derived_country_code:
                    print(f"  - Action: Could not reliably derive country from phone. No update.")
            
            if len(contacts_on_page) < PAGE_LIMIT :
                print("\nLikely fetched all contacts matching criteria (received less than page_limit).")
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
            print(f"An unexpected error occurred during contact fetching/processing loop: {e}")
            import traceback
            traceback.print_exc()
            break
            
    print("\n--- Country Update Process Summary ---")
    print(f"Total contacts queried (Country='SE' AND Phone exists): {contacts_processed_count}") # Updated description
    print(f"Successfully updated country for: {contacts_updated_count} contacts")
    print(f"Failed to update country for: {contacts_failed_update_count} contacts")
    print(f"Failed to parse phone or derive country for: {contacts_phone_parse_failed_count} contacts")


if __name__ == "__main__":
    if not ACCESS_TOKEN or not LOCATION_ID:
        print("ERROR: PSF_ACCESS_TOKEN and PSF_LOCATION_ID must be set either as environment variables or directly in the script.")
        print("If using a .env file, ensure it's loaded and the variables are correctly named.")
    else:
        process_contacts_to_update_country(ACCESS_TOKEN, LOCATION_ID)
    
    print("\nScript execution finished.")