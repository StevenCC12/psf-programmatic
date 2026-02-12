import requests
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration ---
ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
LOCATION_ID = os.getenv("PSF_LOCATION_ID") # Required for this endpoint
API_VERSION = "2021-07-28"
BASE_URL = "https://services.leadconnectorhq.com"

# --- Target Custom Field Details ---
# We will search for a custom field that matches these properties.
TARGET_FIELD_NAME = "Zoom Personal Join Link"
# The API response shows 'fieldKey' often prefixed with 'contact.'
# We'll check for the user-provided key and the prefixed version.
TARGET_FIELD_KEY_SIMPLE = "zoom_personal_join_link"
TARGET_FIELD_KEY_PREFIXED = f"contact.{TARGET_FIELD_KEY_SIMPLE}"


HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Version": API_VERSION,
    "Accept": "application/json" # Content-Type is not needed for GET
}

def get_custom_fields(location_id_param, model_param="contact"):
    """
    Retrieves custom fields for a given location and model.
    """
    if not location_id_param:
        print("ERROR: Location ID is required but not provided/loaded.")
        return None

    url = f"{BASE_URL}/locations/{location_id_param}/customFields"
    params = {
        "model": model_param
    }
    
    print(f"INFO: Fetching custom fields from {url} with params: {params}")
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error occurred: {http_err} - {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"ERROR: Request exception occurred: {req_err}")
    except json.JSONDecodeError:
        print(f"ERROR: Failed to decode JSON response: {response.text}")
    return None

def find_target_custom_field():
    if not ACCESS_TOKEN:
        print("ERROR: PSF_ACCESS_TOKEN is not set. Please check your .env file or environment variables.")
        return
    if not LOCATION_ID:
        print("ERROR: PSF_LOCATION_ID is not set. This script requires it. Please check your .env file.")
        return

    # Update header if ACCESS_TOKEN was loaded after initial definition
    HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"
        
    custom_fields_data = get_custom_fields(LOCATION_ID, model_param="contact")

    if custom_fields_data and "customFields" in custom_fields_data:
        found_fields = []
        for field in custom_fields_data["customFields"]:
            field_id = field.get("id")
            field_name = field.get("name")
            field_key_api = field.get("fieldKey") # This is what the API returns, e.g., "contact.pincode"

            # Check conditions:
            # 1. Name matches TARGET_FIELD_NAME
            # 2. fieldKey from API matches TARGET_FIELD_KEY_SIMPLE or TARGET_FIELD_KEY_PREFIXED
            
            name_matches = (field_name == TARGET_FIELD_NAME)
            key_matches_simple = (field_key_api == TARGET_FIELD_KEY_SIMPLE)
            key_matches_prefixed = (field_key_api == TARGET_FIELD_KEY_PREFIXED)
            
            # As per your request: "name 'Webinar Registration Count' AND key 'webinar_registration_count'"
            # This means we are looking for a field that has the correct name AND one of the correct key variations.
            if name_matches and (key_matches_simple or key_matches_prefixed):
                print(f"\n--- MATCH FOUND (Name AND Key) ---")
                print(f"  ID:         {field_id}")
                print(f"  Name:       {field_name}")
                print(f"  Field Key:  {field_key_api}")
                print(f"  Data Type:  {field.get('dataType')}")
                found_fields.append(field)
            elif name_matches:
                print(f"\n--- PARTIAL MATCH (Name Matched, Key Did Not) ---")
                print(f"  ID:         {field_id}")
                print(f"  Name:       {field_name} (Matches '{TARGET_FIELD_NAME}')")
                print(f"  Field Key:  {field_key_api} (Does not match '{TARGET_FIELD_KEY_SIMPLE}' or '{TARGET_FIELD_KEY_PREFIXED}')")
                print(f"  Data Type:  {field.get('dataType')}")
                found_fields.append(field)
            elif key_matches_simple or key_matches_prefixed:
                print(f"\n--- PARTIAL MATCH (Key Matched, Name Did Not) ---")
                print(f"  ID:         {field_id}")
                print(f"  Name:       {field_name} (Does not match '{TARGET_FIELD_NAME}')")
                print(f"  Field Key:  {field_key_api} (Matches '{TARGET_FIELD_KEY_SIMPLE}' or '{TARGET_FIELD_KEY_PREFIXED}')")
                print(f"  Data Type:  {field.get('dataType')}")
                found_fields.append(field)

        if not found_fields:
            print(f"\nINFO: No custom field found matching Name='{TARGET_FIELD_NAME}' or Key='{TARGET_FIELD_KEY_SIMPLE}/{TARGET_FIELD_KEY_PREFIXED}'.")
            print("INFO: Please check the TARGET_FIELD_NAME and TARGET_FIELD_KEY values in the script, and ensure the custom field exists in your HighLevel location with the 'contact' model.")
        else:
            print(f"\nINFO: Found {len(found_fields)} potential match(es). If multiple are listed, please identify the correct one to use its ID in the main script.")
            print("INFO: The 'ID' is what you'll want to use in the main script for WEBINAR_COUNT_CUSTOM_FIELD_ID.")

    elif custom_fields_data:
        print(f"WARN: 'customFields' key not found in the response. Response received: {json.dumps(custom_fields_data, indent=2)}")
    else:
        print("INFO: Failed to retrieve custom fields.")

if __name__ == "__main__":
    # Ensure your .env file has:
    # PSF_ACCESS_TOKEN="your_token"
    # PSF_LOCATION_ID="your_location_id"
    find_target_custom_field()