import pubmed_download_all_files as pdaf
import sql_server_connection_establisment as ssce
import data_insertion as di

def main():
    ftp, list_of_files = pdaf.ftp_login()
    s3 = pdaf.bucket_folder_creation()
    pdaf.file_extenion_sort(ftp, s3, list_of_files)
    ftp.close()

if __name__ == '__main__':
    main()