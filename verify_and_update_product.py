import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration (using your preferred variable names) ---
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
PSF_LOCATION_ID = os.getenv("PSF_LOCATION_ID")
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"
PRODUCT_NAME_TO_FIND = "Lyckas på Amazon"

def verify_and_update_product_type():
    """
    Finds a specific product by name, checks its productType,
    and updates it to 'PHYSICAL' if it's not already.
    """
    if not all([PSF_ACCESS_TOKEN, PSF_LOCATION_ID]):
        print("CRITICAL ERROR: PSF_ACCESS_TOKEN or PSF_LOCATION_ID is not set in your .env file.")
        return

    # --- STEP 1: Find the product by name ---
    print(f"\n--- STEP 1: Searching for product named '{PRODUCT_NAME_TO_FIND}' ---")
    list_endpoint = f"{GHL_API_BASE_URL}/products/"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }
    params = {
        "locationId": PSF_LOCATION_ID,
        "search": PRODUCT_NAME_TO_FIND
    }

    try:
        response = requests.get(list_endpoint, headers=headers, params=params)
        response.raise_for_status()
        products = response.json().get("products", [])

        if not products:
            print(f"ERROR: No product found with the name '{PRODUCT_NAME_TO_FIND}'. Please check the name.")
            return

        # Assuming the first result is the correct one
        product_data = products[0]
        product_id = product_data.get("_id")
        current_type = product_data.get("productType")

        print(f"SUCCESS: Found product.")
        print(f"  Product ID: {product_id}")
        print(f"  Product Name: {product_data.get('name')}")
        print(f"  API reports current Product Type as: '{current_type}'")

        # --- STEP 2: Check and update the product if necessary ---
        if current_type == "PHYSICAL":
            print("\nINFO: Product is already set to PHYSICAL. No update needed.")
            return

        print(f"\n--- STEP 2: Product type is '{current_type}'. Attempting to update to 'PHYSICAL' ---")
        
        # To safely update, we send back most of the original data,
        # changing only the fields we need to.
        update_payload = {
            "name": product_data.get("name"),
            "description": product_data.get("description"),
            "productType": "PHYSICAL", # The crucial change
            "medias": product_data.get("medias", []) # Include existing media
        }
        
        update_endpoint = f"{GHL_API_BASE_URL}/products/{product_id}"
        update_headers = {**headers, "Content-Type": "application/json"} # Add Content-Type for PUT

        update_response = requests.put(update_endpoint, headers=update_headers, json=update_payload)
        update_response.raise_for_status()

        print(f"SUCCESS: Product updated successfully! API returned status {update_response.status_code}.")
        print("The product type has been programmatically set to PHYSICAL.")

    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR: HTTP error occurred: {http_err}")
        print(f"  Response Text: {http_err.response.text}")
    except Exception as err:
        print(f"ERROR: An unexpected error occurred: {err}")


if __name__ == "__main__":
    verify_and_update_product_type()