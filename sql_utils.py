def get_create_table_query(table_name, schema):
    return "CREATE TABLE IF NOT EXISTS {} {}".format(table_name, schema)

def get_load_file_to_table_query(file_name, table_name):
    return """LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';""".format(file_name, table_name)

def get_filter_results_query(method):
    result_query_using_row_num = """select * from (
                    SELECT id,val,created_at, ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at desc) row_num
                    FROM swvl_records as s
                    )
                    WHERE row_num=1
                    ORDER BY created_at"""

    result_query_using_join = """SELECT s1.id, s1.val, s1.created_at
                            FROM swvl_records s1
                            INNER JOIN (
                            SELECT id, MAX(created_at) AS latest_date
                            FROM swvl_records
                            GROUP BY id ASC
                            ) s2
                            ON s1.id = s2.id
                            AND s1.created_at = s2.latest_date"""

    result_query_using_subquery = """
                SELECT id,val,created_at
                FROM swvl_records s1
                WHERE created_at = (SELECT MAX(created_at) FROM swvl_records s2 WHERE s1.id = s2.id)
                ORDER BY created_at
                """
    switcher = { 
        "result_query_using_join": result_query_using_join, 
        "result_query_using_row_num": result_query_using_row_num, 
        "result_query_using_subquery": result_query_using_subquery, 
    } 
  
    return switcher.get(method, result_query_using_subquery)

# write_to_file_query_suffix = """INTO OUTFILE '/Users/selim_alawwa/enviroment/swvl_file_merge/result'
# FIELDS TERMINATED BY ','
# LINES TERMINATED BY '\n';
# """