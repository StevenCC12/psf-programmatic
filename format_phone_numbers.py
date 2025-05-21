import csv
import os # Used for joining path for output file

# --- Configuration ---
INPUT_CSV_FILENAME = "input_phones.csv"  # <--- CHANGE THIS to your input CSV file name
OUTPUT_CSV_FILENAME = "output_phones_corrected.csv" # Name of the new CSV file that will be created
PHONE_COLUMN_HEADER = "phone"  # <--- CHANGE THIS if your phone column has a different header name

def correct_phone_numbers_in_csv(input_filepath, output_filepath, phone_header):
    """
    Reads a CSV file, checks a specified phone column, and prepends a '+'
    to phone numbers that don't already start with one.
    Writes the results to a new CSV file.
    """
    print(f"Starting phone number correction...")
    print(f"Input CSV: {input_filepath}")
    print(f"Output CSV: {output_filepath}")
    print(f"Phone column header to check: '{phone_header}'")

    rows_processed = 0
    phones_corrected = 0

    try:
        with open(input_filepath, mode='r', encoding='utf-8-sig', newline='') as infile, \
             open(output_filepath, mode='w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            
            if phone_header not in reader.fieldnames:
                print(f"ERROR: Phone column header '{phone_header}' not found in the input CSV.")
                print(f"Available headers are: {reader.fieldnames}")
                return

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader() # Write the header row to the output file

            for row in reader:
                rows_processed += 1
                phone_value = row.get(phone_header) # Get the current phone number

                # Make a copy of the row to modify, to avoid changing the original dict during iteration if not needed
                # Though for DictWriter, modifying row directly is fine.
                output_row = row.copy()

                if isinstance(phone_value, str):
                    cleaned_phone = phone_value.strip() # Remove leading/trailing whitespace
                    if cleaned_phone and not cleaned_phone.startswith('+'):
                        output_row[phone_header] = '+' + cleaned_phone
                        phones_corrected += 1
                    # If it already starts with '+', or is empty, or not a string, it remains as is in output_row
                    # (or as cleaned_phone if only whitespace was removed but it already had '+')
                    elif cleaned_phone: # It's not empty, might have just been whitespace removed
                        output_row[phone_header] = cleaned_phone 
                    else: # It was empty or only whitespace
                        output_row[phone_header] = '' # Ensure it's an empty string not None for CSV
                elif phone_value is None:
                    output_row[phone_header] = '' # Ensure None becomes empty string in CSV

                writer.writerow(output_row)

        print(f"\nProcessing complete.")
        print(f"Total rows processed: {rows_processed}")
        print(f"Phone numbers corrected (added '+'): {phones_corrected}")
        print(f"Corrected data saved to: {output_filepath}")

    except FileNotFoundError:
        print(f"ERROR: Input file not found at '{input_filepath}'. Please check the filename and path.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Construct full path for output file in the current working directory if not already full path
    # This makes it clear where the file will be saved.
    current_directory = os.getcwd()
    output_file_path_full = os.path.join(current_directory, OUTPUT_CSV_FILENAME)
    
    # Assuming input_phones.csv is also in the current directory or INPUT_CSV_FILENAME includes path
    input_file_path_full = os.path.join(current_directory, INPUT_CSV_FILENAME)


    # Check if input file exists before starting
    if not os.path.exists(input_file_path_full):
         print(f"ERROR: Input file '{input_file_path_full}' does not exist. Please create it or check INPUT_CSV_FILENAME.")
    else:
        correct_phone_numbers_in_csv(input_file_path_full, output_file_path_full, PHONE_COLUMN_HEADER)