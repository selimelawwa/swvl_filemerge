import os
import pandas as pd
from swvl_file import SWVLFile
from swvl_file_pool import SWVLFilePool

SOURCE_FILES_DIRECTORY = "/Users/selim_alawwa/enviroment/swvl_file_merge/source_files/"
SORTED_FILES_DIRECTORY = "/Users/selim_alawwa/enviroment/swvl_file_merge/sorted_source_files/"
OUTPUT_FILE_PATH = "python_result.csv"

def sort_file(file_path):
    file_name = file_path.split("/")[-1]
    output_path = SORTED_FILES_DIRECTORY + file_name
    # using pandas here as had a problem in spark config
    # but in production will use pyspark it supports external sorting
    # as stated here: [https://issues.apache.org/jira/browse/SPARK-983]
    df = pd.read_csv(file_path, header=None)
    df = df.sort_values(by=[0,1], ascending=[True, False])
    df.to_csv(output_path, header=False, index=False, index_label=False)
    print("File [{}] sorted result was printed to: [{}]".format(file_path, output_path))

source_files = os.listdir(SOURCE_FILES_DIRECTORY)

for source_file in source_files:
    source_file_path = SOURCE_FILES_DIRECTORY + source_file
    sort_file(source_file_path)

sorted_files = os.listdir(SORTED_FILES_DIRECTORY)

swvl_files = [SWVLFile(open(SORTED_FILES_DIRECTORY + sorted_file)) for sorted_file in sorted_files]
swvl_file_pool = SWVLFilePool(swvl_files)

next_row = swvl_file_pool.get_next_row()
with open(OUTPUT_FILE_PATH, "w") as output_file:
    while next_row:
        print(next_row)
        output_file.write(next_row)
        next_row = swvl_file_pool.get_next_row()

print("Result file saved to {}".format(OUTPUT_FILE_PATH))