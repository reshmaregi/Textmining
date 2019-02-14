import pymysql
import paramiko
import pandas as pd
import data_insertion
from sshtunnel import SSHTunnelForwarder
from os.path import expanduser



def connection():
    home = expanduser('~')
    mypkey = paramiko.RSAKey.from_private_key_file(
        'Enter_Path_to-key' + '.ssh/id_dsa')  # mypkey = paramiko.RSAKey.from_private_key_file(home + pkeyfilepath)
    # if you want to use ssh password use - ssh_password='your ssh password', bellow

    sql_hostname = '127.0.0.1'
    sql_username = 'root'
    sql_password = 'EnterPassWord'
    sql_main_database = 'Enter_DB_Name'
    sql_port = 3306
    ssh_host = ''
    ssh_user = 'Enter_Usr_Name'
    ssh_port = 22
    # ssh_password = 'MySQL@123'
    # sql_ip = '1.1.1.1.1'

    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                               passwd=sql_password, db=sql_main_database,
                               port=tunnel.local_bind_port)
        my_cursor = conn.cursor()
        # *\todo: Remove truncate for final code
        my_cursor.execute("TRUNCATE TABLE pubmed_table")
        sql, val = query_to_insert_MD()
        my_cursor.executemany(sql, val)
        conn.commit()
        query = '''

            SELECT * from pubmed_table
        '''
        data = pd.read_sql_query(query, conn)
        print(data)
        conn.close()

def query_to_insert_MD():
    sql = "INSERT INTO pubmed_table( UID, digital_signature, Filename, path_S3bucket) VALUES (%s, %s, %s, %s ) "
    val = data_insertion.insert_UID_content_filename_url()
    return sql, val


def main():
    connection()
    query_to_insert_MD()


if __name__ == '__main__':
    main()
