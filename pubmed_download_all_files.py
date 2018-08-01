from ftplib import FTP
import boto3  #*/todo mention about creds file in in Git which is in .aws in local
import logging
import os
from botocore.exceptions import ClientError

def ftp_login():
    """

    :return:
    """
    ftp = FTP("ftp.ncbi.nlm.nih.gov", "", "")
    ftp.login()
    ftp.retrlines("LIST")
    ftp.cwd("pubmed/updatefiles")
    list_of_all_files = []
    ftp.retrlines("LIST", list_of_all_files.append)
    # print(list_of_all_files)
    return ftp, list_of_all_files


def bucket_folder_creation():
    s3_client = boto3.client('s3')
    try:
        response_1 = s3_client.put_object(Bucket='s4-radbioinfo-textmining', Key='Files_with_.xml.gz/')
        response_2 = s3_client.put_object(Bucket='s4-radbioinfo-textmining', Key='Files_with_.xml.gz.md5/')
        response_3 = s3_client.put_object(Bucket='s4-radbioinfo-textmining', Key='Files_with_.html/')
        logging.info("%s", response_1, response_2, response_3)
    except Exception as e:
        logging.warning("Bucket error %s", e)
    return s3_client

def file_extenion_sort(ftp, s3, list_of_all_files):
    """

    :param ftp:
    :param list_of_all_files:
    :return:
    """
    for index, entry in enumerate(list_of_all_files):
        words = list_of_all_files[index].split(None, 8)
        filename = words[-1].lstrip()
        if filename.endswith(".md5"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz.md5/'+filename)
            if check_result == False:
                upload_to_S3(ftp,filename, 'Files_with_.xml.gz.md5/')
            else:
                print("File already exists.")
        elif filename.endswith(".gz"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz/' + filename)
            if check_result == False:
                upload_to_S3(ftp,filename, 'Files_with_.xml.gz/')
            else:
                print("File already exists.")
        elif filename.endswith(".html"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.html/' + filename)
            if check_result == False:
                upload_to_S3(ftp,filename, 'Files_with_.html/')
            else:
                print("File already exists.")

def upload_to_S3(ftp, filename, folder):
     bucket_name = 's4-radbioinfo-textmining'
     s3_client = boto3.resource('s3')
     ftp.cwd('/pubmed/updatefiles')
     ftp_filename = 'RETR ' + filename
     try:
         print("Downloading {} ....".format(filename))
         # *todo: replace path for new system
         ftp.retrbinary(ftp_filename, open("C:\\Users\\reshma.regi\\Desktop\\pubmed_temp\\" + filename, 'wb').write)
         s3_client.meta.client.upload_file("C:\\Users\\reshma.regi\\Desktop\\pubmed_temp\\" + filename, bucket_name, folder+''+filename)
         print("File {} uploaded to S3".format(filename))
         if os.path.isfile("C:\\Users\\reshma.regi\\Desktop\\pubmed_temp\\" + filename):
             os.remove("C:\\Users\\reshma.regi\\Desktop\\pubmed_temp\\" + filename)
         else:  ## Show an error if file doesnt exist
             print("Error: %s file not found" % "C:\\Users\\reshma.regi\\Desktop\\pubmed_temp\\" + filename)

     except Exception as e:
         print("s3_client", e)

def check(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True
