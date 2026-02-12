import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
# This allows for secure management of credentials like API tokens and IDs.
load_dotenv()
print("INFO: Loaded environment variables.")

# --- Configuration: Values will be loaded from .env or use defaults ---
# These variables need to be defined in your .env file (e.g., PSF_ACCESS_TOKEN=your_actual_token)
ACCESS_TOKEN = os.getenv("PSF_ACCESS_TOKEN", "YOUR_BEARER_TOKEN_DEFAULT")  # Your HighLevel API Access Token
LOCATION_ID = os.getenv("PSF_LOCATION_ID", "YOUR_LOCATION_ID_DEFAULT")    # The Location ID in HighLevel
DEFAULT_TEST_CONTACT_ID = os.getenv("PSF_TEST_CONTACT_ID", "HxtEIMhtLvNhkqNQIWym") # Target Contact ID for testing

# --- API Details ---
BASE_URL = "https://services.leadconnectorhq.com"
API_VERSION = "2021-04-15"

# Standard headers for all API requests
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Version": API_VERSION,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def find_latest_email_to_reply_to_pragmatic(contact_id, location_id):
    """
    Finds the relevant conversation and the newest email message within it.
    It then extracts the appropriate ID to be used as 'replyMessageId' for sending a threaded reply.
    This function uses a pragmatic approach due to observed API inconsistencies (e.g., missing 'direction' field).
    """
    print(f"Step 1: Searching for conversation with contactId: {contact_id}")
    print(f"  Criteria: Last message must be an INBOUND EMAIL to identify the correct active conversation.")
    search_conv_url = f"{BASE_URL}/conversations/search"
    search_params = {
        "contactId": contact_id, 
        "locationId": location_id, 
        "lastMessageType": "TYPE_EMAIL", # Ensure the conversation's context is email
        "lastMessageDirection": "inbound", # Focus on conversations where the contact last replied
        "sortBy": "last_message_date", 
        "sort": "desc", 
        "limit": 1 # We only need the most recent relevant conversation
    }
    try:
        response = requests.get(search_conv_url, headers=headers, params=search_params)
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        conversations_data = response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"  HTTP error during conversation search: {http_err}")
        print(f"  Response body: {http_err.response.text if http_err.response else 'No response body'}")
        return None, None # Return tuple (message_id_for_reply, original_subject)
    except Exception as e:
        print(f"  Error during conversation search: {e}")
        return None, None

    if not conversations_data.get("conversations") or len(conversations_data["conversations"]) == 0:
        print(f"  No conversation found for contactId: {contact_id} matching the strict criteria.")
        return None, None 
    conversation_object = conversations_data["conversations"][0]
    conversation_id = conversation_object.get("id")
    if not conversation_id:
        print(f"  Found conversation object but it's missing an 'id' attribute.")
        return None, None
    print(f"  Found relevant conversationId: {conversation_id}")

    print(f"\nStep 2: Fetching recent email messages for conversationId: {conversation_id}") 
    get_messages_url = f"{BASE_URL}/conversations/{conversation_id}/messages"
    # Fetching only emails, limit to a few recent ones as the target is expected to be the newest.
    messages_params = {"type": "TYPE_EMAIL", "limit": 5} 
    try:
        response = requests.get(get_messages_url, headers=headers, params=messages_params)
        response.raise_for_status()
        messages_data = response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"  HTTP error fetching messages: {http_err}")
        print(f"  Response body: {http_err.response.text if http_err.response else 'No response body'}")
        return None, None
    except Exception as e:
        print(f"  Error fetching messages: {e}")
        return None, None

    actual_messages_list = None
    # messages_data["messages"] was found to be a dictionary containing metadata and another "messages" key for the array.
    messages_top_level_value = messages_data.get("messages") 
    if not isinstance(messages_top_level_value, dict):
        print(f"  Error: Content under 'messages' key is not a dict as expected. Type: {type(messages_top_level_value)}")
        print(f"  DEBUG Full messages_data: {json.dumps(messages_data, indent=2)}")
        return None, None
    
    # DEBUG print to show the structure of messages_data["messages"] if issues arise
    # print(f"  DEBUG: messages_data['messages'] content: {json.dumps(messages_top_level_value, indent=2)}")

    # Adaptive parsing: Try the previously problematic "." structure first (though less likely now).
    dot_container = messages_top_level_value.get(".")
    if isinstance(dot_container, dict) and isinstance(dot_container.get("messages"), list):
        actual_messages_list = dot_container.get("messages")
        # print("  Parsed messages using: response_json['messages']['.']['messages']") 
    
    # Fallback: Try the structure confirmed from logs: response_json["messages"]["messages"]
    if actual_messages_list is None and isinstance(messages_top_level_value.get("messages"), list):
        actual_messages_list = messages_top_level_value.get("messages")
        print("  Successfully parsed messages using structure: response_json['messages']['messages']")
    
    if actual_messages_list is None: # If neither parsing attempt found a list
        print(f"  Error: Could not locate list of messages within the API response.")
        print(f"  DEBUG: Full messages_data response was: {json.dumps(messages_data, indent=2)}") 
        return None, None
    if not actual_messages_list: # List was found but is empty
        print(f"  Messages list is empty for conversationId: {conversation_id}.")
        return None, None

    print(f"\nStep 3: Selecting newest email message & extracting ID for reply (pragmatic)...")
    # API returns messages newest first, so the first element is the most recent.
    target_message = actual_messages_list[0] 
    if not isinstance(target_message, dict):
        print(f"  Error: Newest message is not a dict. Type: {type(target_message)}")
        return None, None
    # print(f"  DEBUG: Selected newest message for reply: {json.dumps(target_message, indent=2)}") # Useful for deep debugging

    message_id_for_reply = None
    general_message_id = target_message.get("id") # The main ID of the message object
    message_subject = target_message.get("subject") # Observed to be missing in API responses
    message_type = target_message.get("messageType")
    
    # Attempt to get a more specific email message ID from the 'meta.email.messageIds' field.
    # This was found to be necessary to avoid 404 errors when using replyMessageId.
    meta_email_info = target_message.get("meta", {}).get("email", {}) 
    if isinstance(meta_email_info, dict):
        meta_email_ids = meta_email_info.get("messageIds", []) 
        if meta_email_ids and isinstance(meta_email_ids, list) and len(meta_email_ids) > 0:
            message_id_for_reply = meta_email_ids[0] # Use the first ID from this list
            print(f"  Using specific email message ID from meta.email.messageIds for reply: {message_id_for_reply}")
            if len(meta_email_ids) > 1:
                print(f"    Note: Multiple IDs found in meta.email.messageIds: {meta_email_ids}. Using the first one: {message_id_for_reply}.")
        else:
            print(f"  Warning: meta.email.messageIds not found, empty, or not a list. Will attempt to use general message ID.")
    else:
        print(f"  Warning: meta.email structure not as expected or missing. Will attempt to use general message ID.")

    # Fallback to general message ID if a specific one from meta.email.messageIds wasn't found.
    # Note: Using general_message_id previously led to "404 EmailMessage not found" errors.
    if message_id_for_reply is None: 
        print(f"  Fallback: Using general message ID for reply: {general_message_id}")
        message_id_for_reply = general_message_id
    
    if not message_id_for_reply: # If even general_message_id was None (should not happen if target_message is valid)
        print("  Error: Could not determine any message ID to use for replyMessageId.")
        return None, None

    print(f"    Final ID selected for replyMessageId: {message_id_for_reply}")
    if message_id_for_reply != general_message_id: # Log if we used a meta ID
        print(f"    (This was different from the general message.id: {general_message_id})")
    print(f"    Message Type: {message_type}") # Should be TYPE_EMAIL due to API filter
    if message_subject: print(f"    Subject: {message_subject}")
    else: print(f"    Subject: (Not found in message object - API currently omits this)")
        
    if message_type != "TYPE_EMAIL": 
        print(f"  Warning: Selected message type is {message_type}, not TYPE_EMAIL as expected by API filter.")

    # Return the chosen ID for reply and the subject (or a default if subject was missing)
    return message_id_for_reply, message_subject if message_subject else "Email" 


def send_automated_threaded_reply(contact_id, reply_to_message_id, original_subject):
    """
    Constructs and sends an email reply using the provided reply_to_message_id.
    The goal is for this reply to be threaded under the original message.
    """
    # Determine a fallback subject line if the original wasn't found
    reply_subject_text = original_subject if original_subject and original_subject != "Email" else "Your Recent Inquiry"
    reply_subject = f"Re: {reply_subject_text}" # Standard "Re:" prefix for replies
    
    # Email content
    html_body = (
        f"<p>Hello,</p><p>Thank you for your email regarding \"{reply_subject_text}\".</p>"
        f"<p>This is an automated confirmation. Our team will review your inquiry and get back to you soon.</p>"
        f"<p>(This reply is linked to message identified by: {reply_to_message_id})</p><p>Best regards,<br>Automated Support</p>"
    )
    text_body = (
        f"Hello,\n\nThank you for your email regarding \"{reply_subject_text}\".\n\n"
        f"This is an automated confirmation. Our team will review your inquiry and get back to you soon.\n\n"
        f"(This reply is linked to message identified by: {reply_to_message_id})\n\nBest regards,\nAutomated Support"
    )

    # Payload for the POST /conversations/messages endpoint
    reply_payload = {
        "type": "Email", 
        "contactId": contact_id, 
        "subject": reply_subject,
        "html": html_body, 
        "message": text_body, # Plain text version of the email
        "replyMessageId": reply_to_message_id # CRUCIAL: This ID should link the reply for threading
    }

    send_message_url = f"{BASE_URL}/conversations/messages"
    print(f"\nStep 4: Sending automated threaded reply to message identified by: {reply_to_message_id} for contact: {contact_id}")
    print(f"  Reply Subject: {reply_subject}")
    # For deep debugging of the payload being sent:
    # print(f"  DEBUG PAYLOAD: {json.dumps(reply_payload, indent=2)}")


    try:
        response = requests.post(send_message_url, headers=headers, json=reply_payload)
        # raise_for_status() will throw an HTTPError if the HTTP request returned an unsuccessful status code (4xx or 5xx)
        response.raise_for_status() 
        
        print("\n--- Reply Sent Successfully! ---")
        response_data = response.json()
        print(f"  Status Code: {response.status_code}") # Should be 201 for created
        print(f"  New Sent Message ID (GHL General): {response_data.get('messageId')}")
        # This emailMessageId from response is key; observed to be None in tests, which is suspicious.
        print(f"  Email Message ID (of this sent reply): {response_data.get('emailMessageId')}") 
        print(f"  Conversation ID: {response_data.get('conversationId')}")
        print("\nVERIFICATION: Please check your email client for correct threading.")
        return response_data
    except requests.exceptions.HTTPError as http_err:
        # This block handles HTTP errors (e.g., 404, 500) that have a response object
        print(f"\n--- HTTP error occurred while sending reply ---")
        print(f"  Raw Exception Type: {type(http_err)}")
        print(f"  Raw Exception Args: {http_err.args}")
        if hasattr(http_err, 'request') and http_err.request: # The request that caused the error
            print(f"  Request Method: {http_err.request.method}, URL: {http_err.request.url}")
        print(f"  Response Object: {http_err.response}") # The response object from the server
        
        status_code_to_print = 'N/A'
        response_text_to_print = 'No response object or text with error.'

        if http_err.response is not None:
            status_code_to_print = http_err.response.status_code
            response_text_to_print = http_err.response.text # Get raw text from response
            print(f"  Status Code (from response): {status_code_to_print}")
            print(f"  Response Body (from response):")
            try:
                # Try to parse and pretty-print if it's JSON
                print(json.dumps(http_err.response.json(), indent=2))
            except json.JSONDecodeError:
                # If not JSON, print raw text
                print(response_text_to_print if response_text_to_print else "[Empty Response Body Text]")
        else:
            # This path was hit in earlier tests when HTTPError had no response object
            print(f"  Status Code: {status_code_to_print}") 
            print(f"  Response Body: {response_text_to_print}")
        return None
    except requests.exceptions.RequestException as req_err: 
        # Catches other network-related errors like ConnectionError, Timeout, TooManyRedirects
        print(f"\n--- Request exception (e.g., network issue, timeout) occurred while sending reply ---")
        print(f"  Error Type: {type(req_err)}")
        print(f"  Error: {req_err}")
        return None
    except Exception as e: # Catch-all for any other unexpected errors during the send process
        print(f"\n--- An unexpected error occurred while sending reply ---")
        print(f"  Error Type: {type(e)}")
        print(f"  Error: {e}")
        return None

def run_automated_reply_bot(contact_id_trigger):
    """
    Orchestrates the process of finding the latest email from a contact and sending an automated threaded reply.
    """
    # Basic validation for inputs
    if not contact_id_trigger or (contact_id_trigger == "DEFAULT_CONTACT_ID_FOR_TESTING" and DEFAULT_TEST_CONTACT_ID == "DEFAULT_CONTACT_ID_FOR_TESTING"):
        print(f"Error: A valid CONTACT_ID must be provided to run the bot. Current DEFAULT_TEST_CONTACT_ID is '{DEFAULT_TEST_CONTACT_ID}'.")
        return
    
    # Check if essential credentials are placeholder values from the script, indicating they weren't loaded from .env
    if ACCESS_TOKEN == "YOUR_BEARER_TOKEN_DEFAULT" or not ACCESS_TOKEN:
        print("CRITICAL: ACCESS_TOKEN is not configured correctly. Please check your .env file for PSF_ACCESS_TOKEN.")
        return
    if LOCATION_ID == "YOUR_LOCATION_ID_DEFAULT" or not LOCATION_ID:
        print("CRITICAL: LOCATION_ID is not configured correctly. Please check your .env file for PSF_LOCATION_ID.")
        return

    print(f"===== Automated Reply Bot Activated for Contact ID: {contact_id_trigger} =====")
    
    # Find the message to reply to
    message_id_to_reply_to, original_subject = find_latest_email_to_reply_to_pragmatic(contact_id_trigger, LOCATION_ID)
    
    if message_id_to_reply_to:
        # If a message ID was found, attempt to send the reply
        send_automated_threaded_reply(contact_id_trigger, message_id_to_reply_to, original_subject)
    else:
        # If no suitable message was found (e.g., no emails, or ID extraction failed)
        print("\nNo suitable email message ID found to reply to. Bot action terminated for this trigger.")
    
    print(f"===== Bot Cycle Complete for Contact ID: {contact_id_trigger} =====")

# This block executes when the script is run directly
if __name__ == "__main__":
    # Initial informational prints about the configuration being used
    print(f"INFO: Using ACCESS_TOKEN: {'Set (actual token hidden)' if ACCESS_TOKEN and ACCESS_TOKEN != 'YOUR_BEARER_TOKEN_DEFAULT' else 'NOT SET or using default placeholder'}")
    print(f"INFO: Using LOCATION_ID: {'Set (actual ID hidden)' if LOCATION_ID and LOCATION_ID != 'YOUR_LOCATION_ID_DEFAULT' else 'NOT SET or using default placeholder'}")
    print(f"INFO: Using DEFAULT_TEST_CONTACT_ID (loaded from .env as PSF_TEST_CONTACT_ID or script default): {DEFAULT_TEST_CONTACT_ID}")

    # The contact ID that will be used for this test run.
    # It defaults to DEFAULT_TEST_CONTACT_ID, which itself defaults to "HxtEIMhtLvNhkqNQIWym" if PSF_TEST_CONTACT_ID is not in .env
    test_contact_id_to_use = DEFAULT_TEST_CONTACT_ID 
    
    # Pre-run critical checks for essential configurations
    if (not ACCESS_TOKEN or ACCESS_TOKEN == "YOUR_BEARER_TOKEN_DEFAULT" or
        not LOCATION_ID or LOCATION_ID == "YOUR_LOCATION_ID_DEFAULT"):
        print("CRITICAL: Script will not run. Please ensure PSF_ACCESS_TOKEN and PSF_LOCATION_ID are correctly set in your .env file.")
    # Check if the test_contact_id_to_use is still the very generic placeholder from early dev,
    # or if it's the specific one we've been using ("HxtEIMhtLvNhkqNQIWym") and no .env override was provided for it.
    elif test_contact_id_to_use == "HxtEIMhtLvNhkqNQIWym" and os.getenv("PSF_TEST_CONTACT_ID") is None and DEFAULT_TEST_CONTACT_ID == "HxtEIMhtLvNhkqNQIWym":
        print("INFO: Test run will use the specific Contact ID 'HxtEIMhtLvNhkqNQIWym'.")
        print("      If you intend to test with a different contact, set PSF_TEST_CONTACT_ID in your .env file or change DEFAULT_TEST_CONTACT_ID.")
        print(f"Attempting test run with Contact ID: {test_contact_id_to_use}")
        run_automated_reply_bot(test_contact_id_to_use)
    elif not test_contact_id_to_use: # If DEFAULT_TEST_CONTACT_ID somehow ended up empty
        print("Skipping test run: No Contact ID configured for testing (DEFAULT_TEST_CONTACT_ID is empty or None).")
    else: # If DEFAULT_TEST_CONTACT_ID was set (e.g. by PSF_TEST_CONTACT_ID in .env) to something else
        print(f"Attempting test run with Contact ID: {test_contact_id_to_use}")
        run_automated_reply_bot(test_contact_id_to_use)