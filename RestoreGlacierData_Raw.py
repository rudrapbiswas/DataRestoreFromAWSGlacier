#!/bin/python
import sys
import os
import boto3

# DEFINE CONSTANTS
AWS_Access_Key = "XXXXXXXXXX"
AWS_Secret_Key = "XXXXXXXXX"
REGION  = "us-east-1"
S3_BUCKET  = "benbkt"
FOLDER_PREFIX  = "be123/demo/2022-10-11T00-00Z"
RESTORATION_LIFESPAN= 14

file_list = []
bucket = S3_BUCKET
prefix = FOLDER_PREFIX

#Create an S3 Client Session
client = boto3.client(
      's3',
      aws_access_key_id=AWS_Access_Key,
      aws_secret_access_key=AWS_Secret_Key,
      region_name=REGION
   )

# Get List Of Glacier Objects
def restoreGlacier(type):
    EXIT_CODE = 0
   # Try to get S3 list
    try:
      s3_result = client.list_objects_v2(
         Bucket=bucket,
         Prefix=prefix
      )
    except Exception as e:
        print("Unable to list these files with error: ", e)
        exit()
    
    if 'Contents' not in s3_result:
      return []

   # Get list of Contents
    for file in s3_result['Contents']:
      # Only Log Glacier Files
        try:
            print("Restoring file: ", file, "\n")
            doRestoreContents(file, type, file_list)
        except Exception as e:
            EXIT_CODE = 1
            print("Unable to list contents of file: ", file, " with error:\n", e)
         
   # Get list of Contents when More than 1000 Items
    while s3_result['IsTruncated']:
        continuation_key = s3_result['NextContinuationToken']
        s3_result = s3_conn.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key)
        for file in s3_result['Contents']:
            try:
                print("Restoring file: ", file, "\n")
                doRestoreContents(file, type, file_list)
            except Exception as e:
                EXIT_CODE = 1
                print("Unable to list contents of file: ", file, " for more than 1000 items with error:\n", e)          

    # Return File List
    return EXIT_CODE

def doRestoreContents(file, type, file_list):
    resObject = client.restore_object(Bucket=bucket, Key=file['Key'],RestoreRequest={'Days': RESTORATION_LIFESPAN, 'GlacierJobParameters': {
            'Tier': type}})
    if file['StorageClass'] == 'GLACIER':
        if type=="Bulk":
            response = resObject
        elif type=="Standard":
            response = resObject
            file_list.append(file['Key'])

def glacierRestore(type):
   if type=="Bulk":
      op = restoreGlacier(type)
      # If Bulk Success
      if op == 0:
        print("Bulk Request Success!", "Bulk Restore Request Successful! Your files will be restored in 5-12 hours.")
   elif type=="Standard":
      op = restoreGlacier(type)
      # If Standard Success
      if op == 0:
          print("Standard Request Success!", "Standard Restore Request Successful! Your files will be restored in 3-5 hours.")
   else:
      # If Failure
      print("Error", "There was an issue requesting restore of files, please contact your S3 administrator.")
    
if __name__ == '__main__':
    glacierRestore("Bulk")
