import csv
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
PSF_LOCATION_ID = os.getenv("PSF_LOCATION_ID")
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"
CSV_FILE = "woocommerce.csv"  # Your CSV filename

# Map item values to tags
item_to_tag = {
    "Day 1 VIP Onsite 11 October": ["amanordic: fysisk pre 2025"],
    "Day 1 Onsite 6 October": ["amanordic: fysisk pre 2025"],
    "Day 1+2 Digital 6-7 October": ["amanordic: virtual pre 2025"],
    "Day 1 Digital 6 October": ["amanordic: virtual pre 2025"],
    "Day 1+2": ["amanordic: fysisk pre 2025"],
    "Day 1": ["amanordic: fysisk pre 2025"],
    # Add more mappings as needed
}

def search_contact(email):
    url = f"{GHL_API_BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }
    email = email.lower().strip()
    payload = {
        "query": email,
        "locationId": PSF_LOCATION_ID,
        "pageLimit": 1,  # Add this line
        "filters": [
            {
                "field": "email",
                "operator": "EQ",  # Use "EQ" instead of "EQUALS"
                "value": email
            }
        ]
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("contacts"):
            return data["contacts"][0]
        else:
            print(f"DEBUG: No contacts found for {email}. API response: {data}")
    else:
        print(f"DEBUG: API error for {email}: {resp.status_code} - {resp.text}")
    return None

def update_contact_tags(contact_id, tags):
    url = f"{GHL_API_BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }
    payload = {
        "tags": tags
    }
    resp = requests.put(url, headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"Updated tags for contact {contact_id}: {tags}")
    else:
        print(f"Failed to update tags for contact {contact_id}: {resp.text}")

def main():
    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            email = row['email'].lower().strip()  # Normalize here too
            item = row['item']
            tags = item_to_tag.get(item)
            if not tags:
                print(f"No tag mapping for item '{item}' (email: {email})")
                continue
            contact = search_contact(email)
            if contact:
                update_contact_tags(contact['id'], tags)
            else:
                print(f"Contact not found for email: {email}")

if __name__ == "__main__":
    main()