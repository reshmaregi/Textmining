from ftplib import FTP
import boto3
import logging
import os
from botocore.exceptions import ClientError
import gzip



def ftp_login():
    """

    :return:
    """
    ftp = FTP("ftp.ncbi.nlm.nih.gov", "", "")
    ftp.login()
    ftp.retrlines("LIST")
    ftp.cwd("pubmed/baseline")
    # ftp.set_pasv(False)
    list_of_all_files = []
    ftp.retrlines("LIST", list_of_all_files.append)
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
        file1 = filename[9:(len(filename) - 4)]
        # if file1 > '0647':
        if filename.endswith(".md5"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz.md5/' + filename)
            if check_result == False:
                upload_to_S3(ftp, filename, 'Files_with_.xml.gz.md5/')
            else:
                print("File already exists.")
        elif filename.endswith(".gz"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz/' + filename)
            if check_result == False:
                upload_to_S3(ftp, filename, 'Files_with_.xml.gz/')
            else:
                print("File already exists.")
        elif filename.endswith(".html"):
            check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.html/' + filename)
            if check_result == False:
                upload_to_S3(ftp, filename, 'Files_with_.html/')
            else:
                print("File already exists.")
            if filename.endswith(".md5"):
                check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz.md5/' + filename)
                if check_result == False:
                    upload_to_S3(ftp, filename, 'Files_with_.xml.gz.md5/')
                else:
                    print("File already exists.")
            elif filename.endswith(".gz"):
                check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.xml.gz/' + filename)
                if check_result == False:
                    upload_to_S3(ftp, filename, 'Files_with_.xml.gz/')
                else:
                    print("File already exists.")
            elif filename.endswith(".html"):
                check_result = check(s3, 's4-radbioinfo-textmining', 'Files_with_.html/' + filename)
                if check_result == False:
                    upload_to_S3(ftp, filename, 'Files_with_.html/')
                else:
                    print("File already exists.")


def upload_to_S3(ftp, filename, folder):
    file = filename.split(".g")[0]
    bucket_name = 'Enter bucket name'
    s3_client = boto3.resource('s3')
    ftp.cwd('/pubmed/baseline')
    ftp_filename = 'RETR ' + filename
    try:
        print("Downloading {} ....".format(filename))
        # *todo: replace path for new system
        ftp.retrbinary(ftp_filename, open("Enter path of the file" + filename, 'wb').write)

        if filename.endswith(".gz"):
            inF = gzip.open("Enter path of the file" + filename, 'rb')
            outF = open("Enter path of the file" + file, 'wb')
            outF.write(inF.read())
            inF.close()
            outF.close()
        else:
            print('not .GZ File')

        s3_client.meta.client.upload_file("Enter path of the file" + filename, bucket_name,
                                      folder + '' + filename)

    # upload_xml_to_s3(s3_client, f_out, 'Files_with_.xml/')
        print("File {} uploaded to S3".format(filename))
        if os.path.isfile("Enter path of the file" + filename):
            os.remove("Enter path of the file" + filename)
        else:  ## Show an error if file doesnt exist
            print("Error: %s file not found" % "Enter path of the file" + filename)

    except Exception as e:
        print("s3_client", e)


def check(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True


