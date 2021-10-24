###Importing postgres function
from conn_postgres import open_pg_conn
import urllib.request


db_conn = open_pg_conn("localhost","15432","postgres", "postgres",  "123456")
db_cursor = db_conn.cursor()

def insert_values(csv_file):
###Insert values from csv files on postgres db
    columns_file_relation = {}
    f_contents = open('.\\files\downloaded\\' + csv_file, 'r')
    in_file = open('.\\files\\tables_info\\columns_files.csv')
    for line in in_file:
        key, value = line.split()
        columns_file_relation[key] = value
    table_name = csv_file.replace('.csv', '')
    in_file = open(columns_file_relation[csv_file], 'r')
    columns = in_file.readline().strip().split(',')
    try:
        db_cursor.copy_from(f_contents, table_name, columns=columns, sep=";")
        print(table_name, 'Table was loaded on Postgres...')
        db_conn.commit()
    except psycopg2.Error as e:
        t_message = "Database error: " + e + "/n copy_from"


def get_files_from_urls():
#download files on url list
    #nomes_pracas_dict = {}
    a_file = open("url_list.txt")
    for line in a_file:
        key, value = line.split()
        print('Beginning download of {} file'.format(value))
        value2 = ('.\\files\downloaded\\' + value)
        urllib.request.urlretrieve(key, value2)
        insert_values(value)
    db_cursor.close()
    db_conn.close()
       #nomes_pracas_dict[key] = value
