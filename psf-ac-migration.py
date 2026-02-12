import os
import json
import time
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ContactMigrator:
    def __init__(self):
        # Environment variables
        self.psf_token = os.getenv('PSF_ACCESS_TOKEN')
        self.psf_location_id = os.getenv('PSF_LOCATION_ID')
        self.ac_account_name = os.getenv('AC_ACCOUNT_NAME', 'cleanconversion')
        self.ac_api_key = os.getenv('AC_API_KEY')
        
        # API endpoints
        self.psf_base_url = "https://services.leadconnectorhq.com"
        self.ac_base_url = f"https://{self.ac_account_name}.api-us1.com/api/3"
        
        # Request headers
        self.psf_headers = {
            'Authorization': f'Bearer {self.psf_token}',
            'Version': '2021-07-28',
            'Content-Type': 'application/json'
        }
        self.ac_headers = {
            'Api-Token': self.ac_api_key,
            'Content-Type': 'application/json'
        }
        
        # Configuration
        self.page_limit = 100
        self.retry_limit = 3
        self.request_delay = 2  # seconds
        
        # Tracking variables
        self.total_contacts_processed = 0
        self.successful_migrations = 0
        self.failed_migrations = []
        self.duplicate_contacts = []
        self.custom_field_cache = {}  # Cache for created custom fields
        self.ac_list_id = None  # Will be set after creating/getting a list
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Contact migration script initialized")
        
    def validate_config(self) -> bool:
        """Validate required configuration"""
        required_vars = ['PSF_ACCESS_TOKEN', 'PSF_LOCATION_ID', 'AC_API_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing required environment variables: {missing_vars}")
            return False
            
        self.logger.info("Configuration validated successfully")
        return True
        
    def make_request_with_retry(self, method: str, url: str, headers: Dict, 
                              data: Dict = None, max_retries: int = None) -> Tuple[bool, Optional[Dict]]:
        """Make HTTP request with retry logic"""
        if max_retries is None:
            max_retries = self.retry_limit
            
        for attempt in range(max_retries + 1):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, params=data)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=data)
                elif method.upper() == 'PUT':
                    response = requests.put(url, headers=headers, json=data)
                else:
                    self.logger.error(f"Unsupported HTTP method: {method}")
                    return False, None
                    
                if response.status_code in [200, 201]:
                    return True, response.json()
                elif response.status_code == 422 and 'already exists' in response.text.lower():
                    # Handle duplicate email case
                    return True, {'duplicate': True, 'response': response.json()}
                else:
                    self.logger.warning(f"Request failed (attempt {attempt + 1}): {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request exception (attempt {attempt + 1}): {str(e)}")
                
            if attempt < max_retries:
                time.sleep(self.request_delay * (attempt + 1))  # Exponential backoff
                
        return False, None
        
    def search_psf_contacts(self, search_after: List = None) -> Tuple[bool, List[Dict], Optional[List]]:
        """Search contacts in PSF excluding unsubscribed contacts"""
        url = f"{self.psf_base_url}/contacts/search"
        
        # Build request payload
        payload = {
            "locationId": self.psf_location_id,
            "pageLimit": self.page_limit,
            "filters": [
                {
                    "field": "tags",
                    "operator": "not_contains",
                    "value": "contact: unsubscribed"
                }
            ],
            "sort": [
                {
                    "field": "dateAdded",
                    "direction": "asc"
                }
            ]
        }
        
        if search_after:
            payload["searchAfter"] = search_after
        else:
            payload["page"] = 1
            
        self.logger.info(f"Searching PSF contacts (page limit: {self.page_limit})")
        success, response = self.make_request_with_retry('POST', url, self.psf_headers, payload)
        
        if not success or not response:
            self.logger.error("Failed to search PSF contacts")
            return False, [], None
            
        contacts = response.get('contacts', [])
        next_search_after = None
        
        # Check if there are more results for pagination
        if len(contacts) == self.page_limit:
            # Get searchAfter from the last contact (typically includes timestamp and ID)
            last_contact = contacts[-1]
            next_search_after = [last_contact.get('dateAdded'), last_contact.get('id')]
            
        self.logger.info(f"Retrieved {len(contacts)} contacts from PSF")
        return True, contacts, next_search_after
        
    def get_or_create_ac_list(self) -> bool:
        """Get or create a default list in ActiveCampaign for custom fields"""
        url = f"{self.ac_base_url}/lists"
        
        # First, try to get existing lists
        success, response = self.make_request_with_retry('GET', url, self.ac_headers)
        if success and response:
            lists = response.get('lists', [])
            if lists:
                self.ac_list_id = lists[0]['id']  # Use first available list
                self.logger.info(f"Using existing AC list ID: {self.ac_list_id}")
                return True
                
        # Create a new list if none exists
        list_data = {
            "list": {
                "name": "PSF Migration List",
                "stringid": "psf-migration-list",
                "sender_url": f"https://{self.ac_account_name}.activehosted.com",
                "sender_reminder": "You signed up for updates from our website."
            }
        }
        
        success, response = self.make_request_with_retry('POST', url, self.ac_headers, list_data)
        if success and response:
            self.ac_list_id = response['list']['id']
            self.logger.info(f"Created new AC list ID: {self.ac_list_id}")
            return True
            
        self.logger.error("Failed to get or create AC list")
        return False
        
    def create_ac_custom_field(self, field_name: str, field_type: str = "text", 
                             options: List[str] = None) -> Optional[str]:
        """Create a custom field in ActiveCampaign"""
        if field_name in self.custom_field_cache:
            return self.custom_field_cache[field_name]
            
        url = f"{self.ac_base_url}/fields"
        
        # Map PSF field types to AC field types
        type_mapping = {
            'text': 'text',
            'textarea': 'textarea', 
            'select': 'listbox',
            'multiselect': 'checkbox',
            'date': 'date',
            'number': 'text',
            'phone': 'text',
            'email': 'text',
            'url': 'text'
        }
        
        ac_field_type = type_mapping.get(field_type, 'text')
        
        # Create sanitized field name for perstag
        perstag = field_name.lower().replace(' ', '_').replace('-', '_')
        perstag = ''.join(c for c in perstag if c.isalnum() or c == '_')[:20]  # Limit length
        
        field_data = {
            "field": {
                "title": field_name,
                "descript": f"Migrated from PSF: {field_name}",
                "type": ac_field_type,
                "perstag": perstag,
                "visible": True,
                "show_in_list": True
            }
        }
        
        success, response = self.make_request_with_retry('POST', url, self.ac_headers, field_data)
        if not success or not response:
            self.logger.error(f"Failed to create custom field: {field_name}")
            return None
            
        field_id = response['field']['id']
        self.custom_field_cache[field_name] = field_id
        
        # Create field relationship to list
        self.create_field_relationship(field_id)
        
        # Add options if it's a select field
        if options and ac_field_type in ['listbox', 'checkbox']:
            self.create_field_options(field_id, options)
            
        self.logger.info(f"Created custom field '{field_name}' with ID: {field_id}")
        return field_id
        
    def create_field_relationship(self, field_id: str) -> bool:
        """Create relationship between custom field and list"""
        url = f"{self.ac_base_url}/fieldRels"
        
        rel_data = {
            "fieldRel": {
                "relid": self.ac_list_id,
                "field": field_id
            }
        }
        
        success, response = self.make_request_with_retry('POST', url, self.ac_headers, rel_data)
        return success and response is not None
        
    def create_field_options(self, field_id: str, options: List[str]) -> bool:
        """Create options for select/multiselect fields"""
        url = f"{self.ac_base_url}/fieldOption/bulk"
        
        field_options = []
        for i, option in enumerate(options[:10]):  # Limit to 10 options
            field_options.append({
                "orderid": i + 1,
                "value": option,
                "label": option,
                "isdefault": False,
                "field": field_id
            })
            
        options_data = {"fieldOptions": field_options}
        
        success, response = self.make_request_with_retry('POST', url, self.ac_headers, options_data)
        return success and response is not None
        
    def map_psf_to_ac_contact(self, psf_contact: Dict) -> Dict:
        """Map PSF contact data to ActiveCampaign format"""
        ac_contact = {
            "contact": {
                "email": psf_contact.get('email', ''),
                "firstName": psf_contact.get('firstNameLowerCase', '').title(),
                "lastName": psf_contact.get('lastNameLowerCase', '').title(),
                "phone": psf_contact.get('phone', ''),
                "fieldValues": []
            }
        }
        
        # Map additional standard fields as custom fields
        field_mappings = {
            'address': psf_contact.get('address'),
            'city': psf_contact.get('city'),
            'state': psf_contact.get('state'),
            'country': psf_contact.get('country'),
            'postalCode': psf_contact.get('postalCode'),
            'companyName': psf_contact.get('companyName'),
            'businessName': psf_contact.get('businessName'),
            'source': psf_contact.get('source'),
            'type': psf_contact.get('type'),
            'dateOfBirth': psf_contact.get('dateOfBirth'),
            'website': psf_contact.get('website'),
            'phoneLabel': psf_contact.get('phoneLabel')
        }
        
        # Handle tags as a single custom field
        if psf_contact.get('tags'):
            field_mappings['tags'] = ', '.join(psf_contact['tags'])
            
        # Handle additional emails
        if psf_contact.get('additionalEmails'):
            field_mappings['additionalEmails'] = ', '.join(psf_contact['additionalEmails'])
            
        # Handle additional phones  
        if psf_contact.get('additionalPhones'):
            field_mappings['additionalPhones'] = ', '.join(psf_contact['additionalPhones'])
            
        # Process field mappings
        for field_name, field_value in field_mappings.items():
            if field_value:
                field_id = self.create_ac_custom_field(field_name)
                if field_id:
                    ac_contact["contact"]["fieldValues"].append({
                        "field": field_id,
                        "value": str(field_value)
                    })
                    
        # Handle PSF custom fields
        if psf_contact.get('customFields'):
            for custom_field in psf_contact['customFields']:
                field_id_psf = custom_field.get('id')
                field_value = custom_field.get('value')
                
                if field_id_psf and field_value:
                    # Create custom field with PSF field ID as name
                    field_name = f"PSF_Custom_{field_id_psf}"
                    
                    # Handle array values
                    if isinstance(field_value, list):
                        field_value = ', '.join(str(v) for v in field_value)
                        
                    ac_field_id = self.create_ac_custom_field(field_name)
                    if ac_field_id:
                        ac_contact["contact"]["fieldValues"].append({
                            "field": ac_field_id,
                            "value": str(field_value)
                        })
                        
        return ac_contact
        
    def create_ac_contact(self, ac_contact_data: Dict) -> Tuple[bool, Optional[str]]:
        """Create contact in ActiveCampaign"""
        url = f"{self.ac_base_url}/contacts"
        
        success, response = self.make_request_with_retry('POST', url, self.ac_headers, ac_contact_data)
        
        if success and response:
            if response.get('duplicate'):
                return True, 'duplicate'
            else:
                contact_id = response.get('contact', {}).get('id')
                return True, contact_id
                
        return False, None
        
    def migrate_contacts(self) -> bool:
        """Main migration process"""
        self.logger.info("Starting contact migration process")
        
        # Validate configuration
        if not self.validate_config():
            return False
            
        # Get or create AC list for custom fields
        if not self.get_or_create_ac_list():
            return False
            
        search_after = None
        page_count = 0
        
        while True:
            page_count += 1
            self.logger.info(f"Processing page {page_count}")
            
            # Get contacts from PSF
            success, contacts, next_search_after = self.search_psf_contacts(search_after)
            if not success:
                self.logger.error("Failed to retrieve contacts from PSF")
                break
                
            if not contacts:
                self.logger.info("No more contacts to process")
                break
                
            # Process each contact
            for contact in contacts:
                self.total_contacts_processed += 1
                contact_email = contact.get('email', 'No Email')
                
                try:
                    # Map PSF contact to AC format
                    ac_contact_data = self.map_psf_to_ac_contact(contact)
                    
                    # Create contact in ActiveCampaign
                    success, result = self.create_ac_contact(ac_contact_data)
                    
                    if success:
                        if result == 'duplicate':
                            self.duplicate_contacts.append({
                                'email': contact_email,
                                'psf_id': contact.get('id'),
                                'name': f"{contact.get('firstNameLowerCase', '')} {contact.get('lastNameLowerCase', '')}"
                            })
                            self.logger.info(f"Duplicate contact found: {contact_email}")
                        else:
                            self.successful_migrations += 1
                            self.logger.info(f"Successfully migrated: {contact_email} (AC ID: {result})")
                    else:
                        self.failed_migrations.append({
                            'email': contact_email,
                            'psf_id': contact.get('id'),
                            'error': 'Failed to create in ActiveCampaign'
                        })
                        self.logger.error(f"Failed to migrate: {contact_email}")
                        
                except Exception as e:
                    self.failed_migrations.append({
                        'email': contact_email,
                        'psf_id': contact.get('id'),
                        'error': str(e)
                    })
                    self.logger.error(f"Exception processing contact {contact_email}: {str(e)}")
                    
                # Rate limiting
                time.sleep(self.request_delay)
                
            # Check for next page
            if not next_search_after:
                break
                
            search_after = next_search_after
            
        self.generate_final_report()
        return True
        
    def generate_final_report(self):
        """Generate final migration report"""
        report = {
            'migration_summary': {
                'total_contacts_processed': self.total_contacts_processed,
                'successful_migrations': self.successful_migrations,
                'failed_migrations': len(self.failed_migrations),
                'duplicate_contacts': len(self.duplicate_contacts),
                'custom_fields_created': len(self.custom_field_cache)
            },
            'duplicate_contacts': self.duplicate_contacts,
            'failed_migrations': self.failed_migrations,
            'custom_fields_created': list(self.custom_field_cache.keys())
        }
        
        # Save report to file
        report_filename = f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2)
            
        # Log summary
        self.logger.info("=" * 60)
        self.logger.info("MIGRATION COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total contacts processed: {self.total_contacts_processed}")
        self.logger.info(f"Successful migrations: {self.successful_migrations}")
        self.logger.info(f"Failed migrations: {len(self.failed_migrations)}")
        self.logger.info(f"Duplicate contacts: {len(self.duplicate_contacts)}")
        self.logger.info(f"Custom fields created: {len(self.custom_field_cache)}")
        self.logger.info(f"Detailed report saved to: {report_filename}")
        
        if self.duplicate_contacts:
            self.logger.info("\nDUPLICATE CONTACTS:")
            for dup in self.duplicate_contacts:
                self.logger.info(f"  - {dup['email']} ({dup['name']})")
                
        if self.failed_migrations:
            self.logger.info("\nFAILED MIGRATIONS:")
            for failed in self.failed_migrations:
                self.logger.info(f"  - {failed['email']}: {failed['error']}")

def main():
    """Main execution function"""
    migrator = ContactMigrator()
    success = migrator.migrate_contacts()
    
    if success:
        print("Migration completed successfully!")
    else:
        print("Migration failed. Check logs for details.")
        
if __name__ == "__main__":
    main()