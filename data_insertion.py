import os
import random
import string
import boto3
from urllib.parse import urljoin



def insert_UID_content_filename_url():
    content_list = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('s4-radbioinfo-textmining')
    UID= 9999999999
    print("UID \t\t Content \t\t Filename \t\t URL")
    for obj in bucket.objects.filter(Prefix='Files_with_.xml.gz.md5/'):
        filename = str(obj)
        filename = filename[85:(len(filename)-1)]
        if not filename:
            continue
        md5_content = obj.get()['Body'].read()
        content = md5_content[27:(len(md5_content))]
        if not content:
            continue
        content = content.decode("utf-8")
        UID += 1
        # UID = process_UID_value()+ UID
        newurl = urljoin('https://s3.amazonaws.com/BucketName/Files_with_.xml.gz.md5/', filename)
        print(UID, '\t', content, '\t', filename, '\t', newurl)
        content_list.append((UID, content,filename, newurl))
    # print(content_list)
    return content_list

def process_UID_value():
    gen_UID =''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(10)])
    # print("gen_PMID", gen_PMID)
    if gen_UID == None:
        gen_UID = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)])
    else:
        return str(gen_UID)

def data_for_md5():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('s4-radbioinfo-textmining')
    for obj in bucket.objects.filter(Prefix='Files_with_.xml.gz.md5/'):
        md5_content = obj.get()['Body'].read()
        content = md5_content[27:(len(md5_content))]
        decode_content = content.decode('utf-8')
        print(decode_content)



def main():
    insert_UID_content_filename_url()
    data_for_md5()
if __name__ == '__main__':
    main()