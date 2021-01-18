from sql_utils import *
import mysql.connector
import csv
import datetime

config = {
  'user': 'root',
  'password': 'root',
  'host': '127.0.0.1',
  'port': '3307',
  'database': 'swvl',
  'allow_local_infile': True
}

mydb = mysql.connector.connect(**config)
mydb.autocommit = True

files = ["file1", "file2", "file3"]
OUTPUT_FILE_PATH = "/Users/selim_alawwa/enviroment/swvl_file_merge/result"
FETCH_SIZE = 1000

mycursor = mydb.cursor()

table_name = "swvl_records"
schema = "(id INT, val VARCHAR(255), created_at TIMESTAMP)"
create_table_query = get_create_table_query(table_name, schema)
delete_records_query = "delete from {}".format(table_name)
mycursor.execute(create_table_query)

mycursor.execute(delete_records_query) #to ensure table is empty

for file_name in files:
    file_path = "/Users/selim_alawwa/enviroment/swvl_file_merge/source_files/"
    full_path = file_path + file_name
    load_file_query = get_load_file_to_table_query(full_path, table_name)
    mycursor.execute(load_file_query)

result_query = get_filter_results_query("result_query_using_subquery")

mycursor.execute(result_query)

with open(OUTPUT_FILE_PATH, 'w') as output_file:
    csvwriter = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    while True:
        rows = mycursor.fetchmany(FETCH_SIZE)
        if not rows:
            break
        for row in rows:
            csvwriter.writerow(row)

mycursor.execute(delete_records_query) #to free space from db

print("Result file saved to {}".format(OUTPUT_FILE_PATH))

mycursor.close()
mydb.close()