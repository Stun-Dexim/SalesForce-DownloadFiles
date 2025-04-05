import concurrent.futures  
from simple_salesforce import Salesforce  
import requests  
import os  
import csv  
import re  
import logging  
import threading  
import argparse  
import configparser  
import getpass  # Required for masking password and token inputs  
import datetime  
import sys  

INI_PATH = 'download.ini'  
  
INI_TEMPLATE = r"""[salesforce]
	username = YourUserName@somedomain.com

	password = YourSF_PW

	security_token = SFUserSecurityToken        
		##Instructions to Reset your token: https://help.salesforce.com/s/articleView?language=en_US&id=user_security_token.htm&type=5

	connect_to_sandbox = False

	domain =                                    
		##Not required.  Script can figure this out based on your username.


[Extraction Arguments and Parameters]
	output_dir = C:\Files_Extract\              
		##Location to extract files to.  

	output_dir_DateTimeStamp = True             
		##Add DateTimeStamp Dir automaticly each time you run script (True or False)?  Format like YYYYMMDDHHmmss (i.e. 20250403151336)

	query = "SELECT Id, LatestPublishedVersion.Id, Title, FileExtension, LatestPublishedVersion.ContentSize, LatestPublishedVersion.VersionData FROM ContentDocument order by CreatedDate desc  Limit 200"
		#Usage & Purpose:  You can query from ContentDocument, ContentDocumentLink, ContentVersion or Attachment object. All selected fields included in SOQL can be ported to -f and/or -m arguments for flexibility. Manditory Include ContentVersion.VersionData or Attachment.Body for File Creation.
		#Sample Select from ContentDocument:  
			#"SELECT LatestPublishedVersion.Id, LatestPublishedVersion.Title,LatestPublishedVersion.FileExtension,Id, LatestPublishedVersion.ContentSize, LatestPublishedVersion.CreatedBy.Name, LatestPublishedVersion.CreatedDate, LatestPublishedVersion.FileType, LatestPublishedVersion.LastModifiedDate, 	 LatestPublishedVersion.VersionData, LatestPublishedVersion.FirstPublishLocation.Name FROM ContentDocument WHERE LatestPublishedVersion.FirstPublishLocation.Name like 'BPS%'"
		#Sample Select from ContentDocumentLink: 
			#"SELECT ContentDocument.LatestPublishedVersion.Id, ContentDocument.LatestPublishedVersion.Title, ContentDocument.LatestPublishedVersion.FileExtension, 	ContentDocument.LatestPublishedVersion.FileType, ContentDocument.Id, ContentDocument.LatestPublishedVersion.ContentSize, ContentDocument.LatestPublishedVersion.CreatedBy.Name,ContentDocument.LatestPublishedVersion.CreatedDate, ContentDocument.LatestPublishedVersion.LastModifiedDate, 	LinkedEntityId, ContentDocument.LatestPublishedVersion.VersionData FROM ContentDocumentLink WHERE LinkedEntityId IN (SELECT Id 	FROM Opportunity WHERE StageName = '05 - Win')"
		#Sample Select from Attachments: 
			#"SELECT Name, TYPEOF Parent WHEN Opportunity THEN Account.Name, AccountId END, TYPEOF Parent WHEN Opportunity THEN Id, Name,  StageName ELSE Id END, Id,  Body, BodyLength, ContentType, CreatedBy.Name, CreatedDate, 	Description, LastModifiedBy.Name, LastModifiedDate, SystemModstamp, 	ParentId FROM Attachment WHERE ParentId IN ( SELECT Id 	FROM Opportunity WHERE StageName = '05 - Win' )"

	

	filenamepattern = {1}\{2}_{3}.{4}
		##Filename pattern using indexed SOQL fields.  Allows user full customization of output folder and file names and structure.  
		##Defaults: {1}\{2}_{3}.{4} 
		##Example: If SOQL starts with "SELECT LatestPublishedVersion.Id, Title, FileExtension, Id,..." then the folder\file pattern produced may look like "YourOutput_dir\xxx\00530000000h9RlAAI\06840000000pahcAAA_YourFileName.Ext".
		##Be Aware that if you dont specify the ID Column in this pattern, you may end up having duplicate filenames overwrite each other which will make it seem that not all files are extracted.

	metadata = "1,2,3"
		##CSV Metadata file output.  CSV is produced using indexed SOQL fields.
		##Default: "1,2,3"  This assumes the user is selecting at least 3 fields.

	batch_size = 1000
		##Allows local system memory to be utilized more effectively.  

	loglevel = INFO

	threadcount = 10
		##Determines how many concurrent files can be downloaded at once.  Some SalesForce orgs may restrict this pool to 10 concurrent threads.  Higher the number the faster the download.


[RecordFiltering]   
	Attachments_list_CSV_filepath =    # C:\Files_Extract\Exclude or Include.csv 
		#CSV File Format: Ensure your provided CSV file is a simple csv file having a list only containing the IDs (ContentVersionId or AttachmentId) as the first column. Additional columns are ignored.
		#Comment Attachments_list_CSV_filepath argument or set to '' if you just want the SOQL to return all results.
		#****Be aware that your SOQL has to include the ID's that you wish to include in this more specific list (or exclude).  The SOQL is the Pre-requisite.

	AttachID_list_Incl_or_Excl = Include  
		#Include or Exclude: Clearly specify "Include" or "Exclude" in the INI. The default is "Include" if unspecified.

"""  

  
csv_writer_lock = threading.Lock()  
status_lock = threading.Lock()  

def load_id_list_from_csv(csv_filepath):  
    """Loads IDs from a CSV file (first column only). Returns a set of IDs."""  
    id_set = set()  
    try:  
        with open(csv_filepath, 'r', encoding='utf-8-sig') as csvfile:  
            reader = csv.reader(csvfile)  
            for row in reader:  
                if row:  # ensure row not empty  
                    id_set.add(row[0].strip())  
        logging.info(f"Loaded {len(id_set)} IDs from {csv_filepath}")  
    except Exception as e:  
        logging.error(f"Failed to load IDs from CSV: {e}")  
        exit(1)  
    return id_set  

  
# Define illegal characters explicitly (control chars, reserved punctuation, non-ASCII)  
ILLEGAL_CHARS_PATTERN = re.compile(r'[\x00-\x1F<>:"/\\|?*]|[^\x00-\x7F]')    
  
def sanitize_with_mask(original_str, replace_with=' '):     #<<<< See note below.
    """  
    Removes illegal characters from original_str and returns both sanitized string and illegal mask.  
        Reserved Characters for Filenames/Paths (Commonly Problematic): \x00-\x1F: Removes all control characters (ASCII 0–31).
        Characters Problematic for CSV Formatting: <>:"|?*:/ Removes reserved filename characters (Windows/Linux issues).
    Illegal mask helps identify removed characters for debugging purposes.  
      
    Args:  
        original_str (str): Original input string to sanitize.  
        replace_with (str): Character to replace illegal chars with ('' to remove completely).  
      
    Returns:  
        sanitized (str): The sanitized string.  
        illegal_mask (str): A mask showing removed characters and their positions.  
    """  
    sanitized = []  
    illegal_mask = []  
    for c in original_str:  
        if ILLEGAL_CHARS_PATTERN.match(c):  
            sanitized.append(replace_with)  # Replace or remove illegal char  
            illegal_mask.append(c)          # Record illegal char in mask  
            # Optional debug print line (uncomment if needed):  
            # print(f"Removed '{c}' (ASCII: {ord(c)}) from input.")  
        else:  
            sanitized.append(c)             # Keep legal char  
            illegal_mask.append(' ')        # Placeholder for legal char  
    return ''.join(sanitized), ''.join(illegal_mask)  
  
  
def split_into_batches(items, batch_size):  
    """  
    Yields successive batches of items.  
    """  
    for i in range(0, len(items), batch_size):  
        yield items[i:i + batch_size]  
  
  
def remove_double_extension(filename):  
    """  
    Removes redundant double file extensions if identical.  
    E.g., 'filename.PDF.pdf' becomes 'filename.pdf', but 'filename.xls.pdf' remains unchanged.  
      
    Args:  
        filename (str): The input filename.  
      
    Returns:  
        str: Filename with redundant double extension removed if present.  
    """  
    base, ext = os.path.splitext(filename)  
    base2, ext2 = os.path.splitext(base)  
    # Compare extensions ignoring case; if identical, remove inner extension  
    if ext.lower() == ext2.lower() and ext != '':  
        filename = base2 + ext.lower()  
    return filename  
  
  
def create_filename(title, file_extension, output_directory, filename_pattern, indexed_fields):  
    """  
    Creates a sanitized filename, removes double extensions, and returns filename and debug mask.  
      
    Args:  
        title (str): The title part of the filename.  
        file_extension (str): The file extension (with or without leading dot).  
        output_directory (str): Directory to save the file.  
        filename_pattern (str): Pattern for filename formatting.  
        indexed_fields (list): List of additional fields to include in filename.  
      
    Returns:  
        tuple: (full_path, full_path_mask) representing the sanitized file path and mask.  
    """  
    sanitized_fields = []  
    illegal_masks = []  
  
    # Sanitize indexed fields individually and collect illegal masks  
    for field in indexed_fields:  
        sanitized, mask = sanitize_with_mask(str(field))  
        sanitized_fields.append(sanitized)  
        illegal_masks.append(mask)  
  
    # Sanitize title and extension separately  
    sanitized_title, title_mask = sanitize_with_mask(title)  
    sanitized_extension, ext_mask = sanitize_with_mask(file_extension)  
  
    # Generate filename from sanitized fields and title  
    filename = (filename_pattern.format('', *sanitized_fields)  
                .replace('{title}', sanitized_title)  
                .replace('{ext}', sanitized_extension))  
  
    # Remove double extensions explicitly and reliably  
    filename = remove_double_extension(filename)  
  
    # Construct the full path and ensure directory exists  
    full_path = os.path.join(output_directory, filename)  
    os.makedirs(os.path.dirname(full_path), exist_ok=True)  
  
    # Generate the corresponding illegal mask aligned with final path  
    full_path_mask = os.path.join(  
        output_directory,  
        filename_pattern.format('', *illegal_masks)  
        .replace('{title}', title_mask)  
        .replace('{ext}', ext_mask)  
    )  
  
    return full_path, full_path_mask  
  
  
# ----------------- EXAMPLE OF OPTIONAL REPLACEMENT CONFIGURATION -----------------  
  
# If you prefer illegal characters replaced with underscores, just uncomment below:  
# sanitized, mask = sanitize_with_mask(original_str, replace_with='_')  
  
# Alternatively, replace illegal characters with spaces:  
# sanitized, mask = sanitize_with_mask(original_str, replace_with=' ')  
  
# The default above removes illegal characters entirely (replace_with='').  
  
# -------------------------------------------------------------------------------  
  

  
def extract_fields_from_soql(soql):  
    fields_section = re.search(r'\s*SELECT\s+(.*?)\s+FROM\s+', soql, re.IGNORECASE | re.DOTALL)  
    if not fields_section:  
        raise ValueError("Invalid SOQL Query. Could not extract fields.")  
  
    raw_fields = fields_section.group(1).split(',')  
    field_list = []  
  
    for raw_field in raw_fields:  
        field = raw_field.strip()  
  
        # Handle TYPEOF fields explicitly  
        typeof_match = re.match(r'TYPEOF\s+(\w+)\s+WHEN\s+(\w+)\s+THEN\s+([\w_]+)\s+END', field, re.IGNORECASE)  
        if typeof_match:  
            parent_obj, related_type, related_field = typeof_match.groups()  
            nested_field = f"{parent_obj}.{related_field}"  
            field_list.append(nested_field)  
        else:  
            field_list.append(field)  
  
    return field_list  
    
  
def get_nested_field(record, field_path):  
    fields = field_path.split('.')  
    value = record  
    for fld in fields:  
        if isinstance(value, dict):  
            value = value.get(fld)  
            if value is None:  
                return None  # early exit if missing  
        else:  
            return None  
    return value  


  
def download_file(args):  
    record, output_directory, sf, results_path, filename_pattern, metadata_field_indexes, total_files, progress_counter, field_list, session, salesforce_object, metadata_dict = args      
  
    indexed_fields = [get_nested_field(record, field_list[idx - 1]) or 'Unknown' for idx in range(1, len(field_list) + 1)]  
    if salesforce_object == 'attachment':  
        title = get_nested_field(record, 'Name') or 'NoTitle'  
        content_type = get_nested_field(record, 'ContentType') or 'application/octet-stream'  
        file_extension = content_type.split('/')[-1]  # minimal way to guess extension from MIME type  
    else:  
        title = get_nested_field(record, 'Title') or 'NoTitle'  
        file_extension = get_nested_field(record, 'FileExtension') or ''   
  
    # Create sanitized filename and illegal chars mask  
    filename, illegal_mask = create_filename(title, file_extension, output_directory, filename_pattern, indexed_fields)  
  
    # Prepare metadata row independently of file download success  
    metadata_row = [get_nested_field(record, field_list[idx - 1]) or '' for idx in metadata_field_indexes]  
  
    status = 'Not Attempted'  
      
    if salesforce_object == 'attachment':  
        body_url = record.get('Body')  
        if not body_url:  
            status = 'Failed (No Body URL)'  
        else:  
            url = f"https://{sf.sf_instance}{body_url}"  
            try:  
                response = session.get(url, headers={  
                    "Authorization": "OAuth " + sf.session_id,  
                    "Content-Type": "application/octet-stream"  
                }, timeout=600)  
      
                if response.ok:  
                    os.makedirs(os.path.dirname(filename), exist_ok=True)  
                    with open(filename, "wb") as file_out:  
                        file_out.write(response.content)  
                    status = 'Success'  
                else:  
                    status = f"Failed (HTTP {response.status_code})"  
      
            except requests.exceptions.RequestException as e:  
                status = f"Failed (Exception: {str(e)})"  
      
    else:  # Existing logic unchanged for ContentDocument/ContentVersion  
        version_data_url = get_nested_field(record, 'LatestPublishedVersion.VersionData') or record.get('VersionData')  
        if not version_data_url:  
            status = 'Failed (No VersionData URL)'  
        else:  
            url = f"https://{sf.sf_instance}{version_data_url}"  
            try:  
                response = session.get(url, headers={  
                    "Authorization": "OAuth " + sf.session_id,  
                    "Content-Type": "application/octet-stream"  
                }, timeout=600)  
      
                if response.ok:  
                    os.makedirs(os.path.dirname(filename), exist_ok=True)  
                    with open(filename, "wb") as file_out:  
                        file_out.write(response.content)  
                    status = 'Success'  
                else:  
                    status = f"Failed (HTTP {response.status_code})"  
      
            except requests.exceptions.RequestException as e:  
                status = f"Failed (Exception: {str(e)})"  
  
    # Correct minimal update to metadata_dict (no CSV write here!)  
    with csv_writer_lock:  
        unique_id = record.get('Id')  
        file_name_only = os.path.basename(filename)  
        xls_hyperlink = f'=HYPERLINK("{filename}", "{file_name_only}")'  
        metadata_dict[unique_id][-4:] = [filename, xls_hyperlink, status, illegal_mask]  
  
    # Thread-safe update of progress  
    with status_lock:  
        progress_counter[0] += 1  
        completed = progress_counter[0]  
        print(f"\rProgress: {completed}/{total_files} files completed ({completed/total_files:.1%})", end='', flush=True)  


  
def fetch_files(sf, results, output_directory, filename_pattern, results_path, metadata_field_indexes, batch_size, thread_count, field_list, salesforce_object, metadata_dict, metadata_header):    
    total_files = len(results)  
    progress_counter = [0]  
  
    # Robust slicing logic to ensure no records are skipped  
    total_files = len(results)  
    progress_counter = [0]  
      
    with requests.Session() as session:  # HTTP session reuse implemented here  
        session.headers.update({"Authorization": "OAuth " + sf.session_id})  
      
        # Explicitly slice records into batches using Python list comprehension  
        batches = [results[i:i + batch_size] for i in range(0, len(results), batch_size)]  
        total_batches = len(batches)  
        logging.info(f"Total batches to process: {total_batches}")  
      
        for idx, batch in enumerate(batches, 1):  
            logging.info(f"Processing batch {idx}/{total_batches}...")  
            args_list = [  
                (  
                    record, output_directory, sf, results_path, filename_pattern,  
                    metadata_field_indexes, total_files, progress_counter, field_list,  
                    session, salesforce_object, metadata_dict  
                ) for record in batch  
            ]       
      
            with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:  
                executor.map(download_file, args_list)   
      
        print("\nDownload process completed successfully.")  
        
    # Write fully updated metadata back to CSV after all downloads  
    with csv_writer_lock:  
        with open(results_path, 'w', encoding='utf-8', newline='') as f_csv:  
            writer = csv.writer(f_csv)  
            writer.writerow(metadata_header)  
            for row in metadata_dict.values():  
                writer.writerow(row)  
  


def main():  
    
    #Logic to handle check for existance and auto-creation of Download.ini (user interaction).
    if not os.path.exists(INI_PATH):  
        print(f"INI file '{INI_PATH}' not found.")  # <-- Correct indentation clearly  
        create_ini = input("Would you like to create a template INI file now? (Y/N): ").strip().upper()  
        if create_ini == 'Y':  
            with open(INI_PATH, 'w', encoding='utf-8') as ini_file:  
                ini_file.write(INI_TEMPLATE)  
            print(f"Template INI file created at '{os.path.abspath(INI_PATH)}'. Please fill in your details and re-run the script.")  
            sys.exit(0)  
        else:  
            print("Proceeding without INI file. You must provide all required arguments via command line.")  
    
    
    # Parse arguments (set defaults to None to detect if provided at runtime)  
    parser = argparse.ArgumentParser(description='Export Salesforce Files')  
    parser.add_argument('-q', '--query', help='SOQL query to select files.  You can query from ContentDocument, ContentDocumentLink, ContentVersion or Attachment object.  All selected fields included in SOQL can be ported to -f and/or -m arguments for flexibility.  Manditory Include ContentVersion.VersionData or Attachment.Body for File Creation.')  
    parser.add_argument('-f', '--filenamepattern',  help='Filename pattern using ordinal position of SOQL fields.  default={1}\\{2}_{3}.{4}  Be Aware that if you dont specify the ID Column in this pattern, you may end up having duplicate filenames overwrite each other which will make it seem that not all files are extracted.')  
    parser.add_argument('-m', '--metadata', help='Comma-separated indexed fields for metadata CSV output, e.g. "1,2,3"')  
    parser.add_argument('-t', '--threadcount', type=int, help='Number of concurrent threads')  
    args = parser.parse_args()  
      
    # Load INI file clearly  
    config = configparser.ConfigParser()  
    config.read(INI_PATH)  
      
    # Assign arguments, giving precedence clearly to runtime arguments if provided  
    query = args.query or config['Extraction Arguments and Parameters'].get('query', '').strip('"')  
    filenamepattern = args.filenamepattern or config['Extraction Arguments and Parameters'].get('filenamepattern', '{1}\\{2}_{3}.{4}')  
    metadata = args.metadata or config['Extraction Arguments and Parameters'].get('metadata', '1,2,3').strip('"')  
    threadcount = args.threadcount or config['Extraction Arguments and Parameters'].getint('threadcount', fallback=10)  
      
    # Check mandatory query clearly  
    if not query:  
        logging.error("SOQL query not specified. Provide via command-line (-q) or INI file.")  
        exit(1)    

      
    # Get output directory from INI clearly  
    output_directory = config['Extraction Arguments and Parameters'].get('output_dir', 'C:\\Files_Extract\\').strip()  
      
    # Check if timestamp directory is requested clearly  
    if config['Extraction Arguments and Parameters'].getboolean('output_dir_DateTimeStamp', fallback=False):  
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")  
        output_directory = os.path.join(output_directory, timestamp)  
  
    # Create the output directory clearly  
    os.makedirs(output_directory, exist_ok=True) 

  
    # Salesforce credentials setup  
    username = config.get('salesforce', 'username', fallback=None)  
    if not username:  
        username = input("Please enter Salesforce username: ")  
    password = config.get('salesforce', 'password', fallback=None)  
    if not password:  
        password = getpass.getpass("Please enter Salesforce password: ")  
    token = config.get('salesforce', 'security_token', fallback=None)  
    if not token:  
        token = getpass.getpass("Please enter Salesforce security token: ")  
  
    is_sandbox = config['salesforce'].getboolean('connect_to_sandbox', False)  
    domain = config['salesforce'].get('domain', '')  
    domain = f"{domain}.my" if domain else ('test' if is_sandbox else 'login')  
  
     
    batch_size = config['Extraction Arguments and Parameters'].getint('batch_size', fallback=1000)  
    loglevel = logging.getLevelName(config['Extraction Arguments and Parameters']['loglevel'])  
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=loglevel)  
  
    logging.info('Connecting to Salesforce...')  
    sf = Salesforce(username=username, password=password, security_token=token, domain=domain)  
    logging.info('Connected successfully.')  
  
    os.makedirs(output_directory, exist_ok=True)  
    results_path = os.path.join(output_directory, 'files_metadata.csv')  
  
    field_list = extract_fields_from_soql(args.query)  
    metadata_field_indexes = [int(i.strip()) for i in args.metadata.split(',')]  
  
    metadata_header = [field_list[idx - 1] for idx in metadata_field_indexes] + ['FilePath', 'XLS_Link_FilePath', 'Status', 'Illegal_Chars_Stripped']  
  
    # Execute SOQL query  
    logging.info('Executing SOQL query to retrieve files...')  
    query_result = sf.query_all(args.query)  
    records = query_result.get('records', [])  
    total_records = len(records)  
    logging.info(f"Retrieved {total_records} records.")  
  
    # Identify Salesforce object from SOQL query (must happen here before filtering)  
    soql_object_match = re.search(r'FROM\s+(\w+)', args.query, re.IGNORECASE)  
    if not soql_object_match:  
        logging.error("Unable to parse Salesforce object from SOQL query.")  
        exit(1)  
  
    salesforce_object = soql_object_match.group(1).lower()  
  
    # ---- START OF ROBUST ID FILTERING LOGIC ----  
    csv_id_filepath = config.get('RecordFiltering', 'Attachments_list_CSV_filepath', fallback=None)  
    include_or_exclude = config.get('RecordFiltering', 'AttachID_list_Incl_or_Excl', fallback='Include').strip().lower()  
  
    if csv_id_filepath:  
        id_list_from_csv = load_id_list_from_csv(csv_id_filepath)  
        original_record_count = len(records)  
  
        # Explicitly determine the correct ID field based on Salesforce object  
        if salesforce_object == 'attachment':  
            sf_id_field = 'Id'  
        elif salesforce_object == 'contentdocument':  
            sf_id_field = 'LatestPublishedVersion.Id'  
        elif salesforce_object == 'contentversion':  
            sf_id_field = 'Id'  
        elif salesforce_object == 'contentdocumentlink':  
            sf_id_field = 'ContentDocument.LatestPublishedVersion.Id'  
        else:  
            logging.error(f"Unsupported Salesforce object for ID filtering: {salesforce_object}.")  
            exit(1)  
  
        # Use get_nested_field to handle nested fields correctly  
        if include_or_exclude == 'exclude':  
            records = [rec for rec in records if get_nested_field(rec, sf_id_field) not in id_list_from_csv]  
            logging.info(f"Excluding IDs from CSV: {original_record_count - len(records)} records removed.")  
        else:  # default to include  
            records = [rec for rec in records if get_nested_field(rec, sf_id_field) in id_list_from_csv]  
            logging.info(f"Including only IDs from CSV: {original_record_count - len(records)} records removed.")  
  
        total_records = len(records)  
        logging.info(f"Total records after filtering: {total_records}")  
    # ---- END OF ROBUST ID FILTERING LOGIC ----  
  
    # Create metadata dictionary indexed by unique record ID  
    metadata_dict = {}  
    for record in records:  
        unique_id = record.get('Id')  
        metadata_row = [get_nested_field(record, field_list[idx - 1]) or '' for idx in metadata_field_indexes]  
        metadata_dict[unique_id] = metadata_row + ['Not Created', 'N/a', 'Failed', 'N/a']  
  
    # Write initial metadata file with default failed status  
    with open(results_path, 'w', encoding='utf-8', newline='') as f_csv:  
        writer = csv.writer(f_csv)  
        writer.writerow(metadata_header)  
        for row in metadata_dict.values():  
            writer.writerow(row)  
  
    # Required fields validation  
    required_fields = []  
    if salesforce_object == 'attachment':  
        required_fields = ['Body', 'BodyLength', 'ContentType']  
    elif salesforce_object in ['contentdocument', 'contentversion', 'contentdocumentlink']:  
        required_fields = ['VersionData']  
    else:  
        logging.error(f"Unsupported Salesforce object: {salesforce_object}.")  
        exit(1)  
  
    missing_fields = []  
    for req_field in required_fields:  
        if not any(f.split('.')[-1] == req_field for f in field_list):  
            missing_fields.append(req_field)  
  
    if missing_fields:  
        logging.error(f"SOQL query missing required fields for {salesforce_object}: {missing_fields}")  
        exit(1)  
  
    if total_records == 0:  
        logging.info("No records found. Exiting.")  
        return  
  
    # Fetch and download files concurrently  
    fetch_files(  
        sf=sf,  
        results=records,  
        output_directory=output_directory,  
        filename_pattern=args.filenamepattern,  
        results_path=results_path,  
        metadata_field_indexes=metadata_field_indexes,  
        batch_size=batch_size,  
        thread_count=args.threadcount,  
        field_list=field_list,  
        salesforce_object=salesforce_object,  
        metadata_dict=metadata_dict,  
        metadata_header=metadata_header  
    )  
  
if __name__ == "__main__":  
    main()  