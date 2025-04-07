This script forked from snorf/salesforce-files-download.  I had need to be able to perform extensive extractions.  


# Highlight Features include
	1. Threading and session reuse for speed: In testing, was able to download 80k file (contentversion) in 45 min and an accompanying metadata file is always produced.  YMMV depending on network connection, etc.
	2. Flexible SOQL handling:  This solution is fully dynamic and robust. 
		a. This Python script will seamlessly and correctly handle any SOQL query.
		b. Whether it contains standard fields, nested fields, or polymorphic (TYPEOF) fields, without manual intervention or special-case handling.
	3. Metadata file creation.  Very flexible w/ success/failure audit of download.  Also an illegal chars mask which shows which chars would have caused your downloads to fail in windows file system if not stripped.
	4. Ability to supply include or exclude ID list which will exclusively include/exclude your specified ContentVersionIDs or AttachmentIDs based on csv file.
	5. Auto Timestamp on Extraction directory: Add Date Timestamp Dir automatically each time you run script (True or False)?  Format like YYYYMMDDHHmmss (i.e. 20250403151336)
	6. Double/dup file extension sanitization (removes troublesome double extensions like yourfilename.Pdf.pdf will be downloaded as yourfilename.pdf).
	7. Illegal Char stripping from file creation path and filename.
	8. Alternate ini file created at runtime if user wishes.  Allows for runtime arguments to be specified/retrieved from ini file (initialization file).
	9. Sensitive Credentials Hiding:  If you need to run an extraction but your SF admin will not give you pw and access token:
		a. then you can prepare your usage w/ this script and accompanying ini file (leave the pw and token fields blank in the ini file)
		b. share your screen w/ your admin and provide control
		c. Kick off your script and it will automatically prompt for credentials not specified in the ini file.  
		d. Admin enters credentials (entry is fully hidden and session fully disposed when script finishes running)
		e. Facilitates secret credential passing over shared desktop from admin to developer or end user.

  


# Usage
	Download.py [-h] [-q QUERY] [-f FILENAMEPATTERN] [-m METADATA] [-t THREADCOUNT]     Export Salesforce Files                                                                                                 
	  options:
	    -h, --help            show this help message and exit
	    -q QUERY, --query QUERY
				  SOQL query to select files. You can query from ContentDocument, ContentDocumentLink,
				  ContentVersion or Attachment object. All selected fields included in SOQL can be ported to -f
				  and/or -m arguments for flexibility. Manditory Include ContentVersion.VersionData or
				  Attachment.Body for File Creation.
	    -f FILENAMEPATTERN, --filenamepattern FILENAMEPATTERN
				  Filename pattern using ordinal position of SOQL fields. default={1}\{2}_{3}.{4} Be Aware that
				  if you dont specify the ID Column in this pattern, you may end up having duplicate filenames
				  overwrite each other which will make it seem that not all files are extracted.
	    -m METADATA, --metadata METADATA
				  Comma-separated indexed fields for metadata CSV output, e.g. "1,2,3"
	    -t THREADCOUNT, --threadcount THREADCOUNT
				  Number of concurrent threads



# Alternate ini file: created at runtime if user wishes.  Content below shows the extended features and flexibility of this script.
		[salesforce]
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
				#"SELECT LatestPublishedVersion.Id, LatestPublishedVersion.Title,LatestPublishedVersion.FileExtension,Id, LatestPublishedVersion.ContentSize, LatestPublishedVersion.CreatedBy.Name, LatestPublishedVersion.CreatedDate, LatestPublishedVersion.FileType, LatestPublishedVersion.LastModifiedDate,          LatestPublishedVersion.VersionData, LatestPublishedVersion.FirstPublishLocation.Name FROM ContentDocument WHERE LatestPublishedVersion.FirstPublishLocation.Name like 'BPS%'"
			#Sample Select from ContentDocumentLink: 
				#"SELECT ContentDocument.LatestPublishedVersion.Id, ContentDocument.LatestPublishedVersion.Title, ContentDocument.LatestPublishedVersion.FileExtension,         ContentDocument.LatestPublishedVersion.FileType, ContentDocument.Id, ContentDocument.LatestPublishedVersion.ContentSize, ContentDocument.LatestPublishedVersion.CreatedBy.Name,ContentDocument.LatestPublishedVersion.CreatedDate, ContentDocument.LatestPublishedVersion.LastModifiedDate,         LinkedEntityId, ContentDocument.LatestPublishedVersion.VersionData FROM ContentDocumentLink WHERE LinkedEntityId IN (SELECT Id         FROM Opportunity WHERE StageName = '05 - Win')"
			#Sample Select from Attachments: 
				#"SELECT Name, TYPEOF Parent WHEN Opportunity THEN Account.Name, AccountId END, TYPEOF Parent WHEN Opportunity THEN Id, Name,  StageName ELSE Id END, Id,  Body, BodyLength, ContentType, CreatedBy.Name, CreatedDate,         Description, LastModifiedBy.Name, LastModifiedDate, SystemModstamp,         ParentId FROM Attachment WHERE ParentId IN ( SELECT Id         FROM Opportunity WHERE StageName = '05 - Win' )"
	
		
	
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

