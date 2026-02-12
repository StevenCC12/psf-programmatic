import csv
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PSF_ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN")
GHL_API_BASE_URL = "https://services.leadconnectorhq.com"
LOCATION_ID = os.getenv("PSF_LOCATION_ID")  # Set this in your .env file

CSV_FILE = "woocommerce.csv"  # Change to your CSV filename

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

def clean_row(row):
    # Clean and format each field as specified
    row['fn'] = row['fn'].title().strip()
    row['ln'] = row['ln'].title().strip()
    row['company'] = row['company'].title().strip() if row['company'] else ""
    row['city'] = row['city'].title().strip()
    row['address'] = row['address'].title().strip()
    row['email'] = row['email'].lower().strip()
    row['postcode'] = row['postcode'].replace(" ", "")
    return row

def search_contact(email):
    url = f"{GHL_API_BASE_URL}/contacts/search"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }
    payload = {
        "query": email,
        "filters": [
            {
                "filterType": "email",
                "value": email
            }
        ]
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("contacts"):
            return data["contacts"][0]  # Return first match
    return None

def create_contact(row):
    # Get tags based on the item column
    tags = item_to_tag.get(row['item'], [])
    url = f"{GHL_API_BASE_URL}/contacts/"
    headers = {
        "Authorization": f"Bearer {PSF_ACCESS_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }
    payload = {
        "firstName": row['fn'],
        "lastName": row['ln'],
        "name": f"{row['fn']} {row['ln']}",
        "email": row['email'],
        "locationId": LOCATION_ID,
        "phone": row['phone'],
        "address1": row['address'],
        "city": row['city'],
        "postalCode": row['postcode'],
        "country": row['country'],
        "companyName": row['company'],
        "tags": tags,
        "source": "woocommerce import"
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 201:
        print(f"Created contact: {row['email']} with tags: {tags}")
    else:
        try:
            error = resp.json()
            if (
                error.get("statusCode") == 400 and
                "duplicated contacts" in error.get("message", "")
            ):
                print(f"Email already in CRM - skipping: {row['email']}")
            else:
                print(f"Failed to create contact: {row['email']} - {resp.text}")
        except Exception:
            print(f"Failed to create contact: {row['email']} - {resp.text}")

def main():
    found_contacts = []
    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            row = clean_row(row)
            contact = search_contact(row['email'])
            if contact:
                found_contacts.append(contact)
            else:
                create_contact(row)
    print("\nContacts found in CRM (merge manually):")
    for c in found_contacts:
        print(f"{c.get('firstName', '')} {c.get('lastName', '')} | {c.get('email', '')} | {c.get('id', '')}")

if __name__ == "__main__":
    main()