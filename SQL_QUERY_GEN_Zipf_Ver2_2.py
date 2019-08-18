import MySQLdb
import time
import numpy as np
import csv

## Setting up and opening the Database Connection.

db_connection = MySQLdb.connect(host="localhost", user="root", passwd="050194.Piku", db="lineitem")

## Declaring my Database Connection Cursor.
cursor = db_connection.cursor()

## Declaring Empty Arrays to store my Query Results.

## list of empty arrays to store query execution time running in the loop.

exe_time_1 = []
exe_time_2 = []
exe_time_3 = []
exe_time_4 = []
exe_time_5 = []
exe_time_6 = []
exe_time_7 = []
exe_time_8 = []


## list of empty arrays to store query result rows returned while running in the loop.


## list of empty arrays to store min. and max. range values of extended price generated randomly in the loop.
# lineitem_ext_price1_list = []
# lineitem_ext_price2_list = []

lines_1 = []
lines_2 = []
lines_3 = []
lines_4 = []
lines_5 = []
lines_6 = []
lines_7 = []
lines_8 = []


columns = ['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Avg_Execution_Time',
           'Med_Execution_Time', 'Std_Deviation']
lines_1.append(columns)



for x in range(3):  ## specifying the no. of times to execute each query.

    ## Generating random values for min. and max. range and storing them to the empty arrays declared above.
    a = 1.01
    n1 = np.random.zipf(a, 1)
    n2 = np.random.zipf(a, 1)
    p = n1[0]
    q = n2[0]
    if p > q:
        min_range = q
        max_range = p
    else:
        min_range = p
        max_range = q
    min_range_value = str(min_range)
    max_range_value = str(max_range)


    sql_res_size_query1 = 0
    sql_res_size_query2 = 0
    sql_res_size_query3 = 0
    sql_res_size_query4 = 0
    sql_res_size_query5 = 0
    sql_res_size_query6 = 0
    sql_res_size_query7 = 0
    sql_res_size_query8 = 0



    for y in range(5):

        sql_query_lineitem2 = "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_1 = time.time()
        cursor.execute(sql_query_lineitem2)
        df = cursor.fetchall()
        sql_res_size_query1 = len(df)
        time_taken_1 = time.time() - initial_time_1
        exe_time_1.append(time_taken_1)

        sql_query_join_lineitem_3 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                                 WHERE L_EXTENDEDPRICE BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_2 = time.time()
        cursor.execute(sql_query_join_lineitem_3)
        df = cursor.fetchall()
        sql_res_size_query2= len(df)
        time_taken_2 = time.time() - initial_time_2
        exe_time_2.append(time_taken_2)

        sql_query_order2 = "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_3 = time.time()
        cursor.execute(sql_query_order2)
        df = cursor.fetchall()
        sql_res_size_query3 = len(df)
        time_taken_3 = time.time() - initial_time_3
        exe_time_3.append(time_taken_3)

        sql_query_join_order_3 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                                         WHERE O_TOTALPRICE BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_4 = time.time()
        cursor.execute(sql_query_join_order_3)
        df = cursor.fetchall()
        sql_res_size_query4 = len(df)
        time_taken_4 = time.time() - initial_time_4
        exe_time_4.append(time_taken_4)

        sql_query_lineitem3 = "SELECT * FROM lineitem_table WHERE L_ORDERKEY BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_5 = time.time()
        cursor.execute(sql_query_lineitem3)
        df = cursor.fetchall()
        sql_res_size_query5 = len(df)
        time_taken_5 = time.time() - initial_time_5
        exe_time_5.append(time_taken_5)

        sql_query_join_lineitem_1 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                         WHERE L_ORDERKEY BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_6 = time.time()
        cursor.execute(sql_query_join_lineitem_1)
        df = cursor.fetchall()
        sql_res_size_query6 = len(df)
        time_taken_6 = time.time() - initial_time_6
        exe_time_6.append(time_taken_6)

        sql_query_join_order_2 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                                     WHERE O_ORDERKEY BETWEEN '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_7 = time.time()
        cursor.execute(sql_query_join_order_2)
        df = cursor.fetchall()
        sql_res_size_query7 = len(df)
        time_taken_7 = time.time() - initial_time_7
        exe_time_7.append(time_taken_7)

        sql_query_order3 = "SELECT * FROM order_table WHERE O_ORDERKEY = '" + min_range_value + "' AND '" + max_range_value + "';"

        initial_time_8 = time.time()
        cursor.execute(sql_query_order3)
        df = cursor.fetchall()
        sql_res_size_query8 = len(df)
        time_taken_8 = time.time() - initial_time_8
        exe_time_8.append(time_taken_8)

    lines_1.append(['lineitem_table', 'L_EXTENDEDPRICE', min_range_value, max_range_value, sql_res_size_query1,
                  np.mean(exe_time_1[:]), np.median(exe_time_1[:]), np.std(exe_time_1[:])])

    lines_2.append(['lineitem_table', 'L_EXTENDEDPRICE', min_range_value, max_range_value, sql_res_size_query2,
                   np.mean(exe_time_2[:]), np.median(exe_time_2[:]), np.std(exe_time_2[:])])

    lines_3.append(['order_table', 'O_TOTALPRICE', min_range_value, max_range_value, sql_res_size_query3,
                    np.mean(exe_time_3[:]), np.median(exe_time_3[:]), np.std(exe_time_3[:])])

    lines_4.append(['order_table', 'O_TOTALPRICE', min_range_value, max_range_value, sql_res_size_query4,
                    np.mean(exe_time_4[:]), np.median(exe_time_4[:]), np.std(exe_time_4[:])])

    lines_5.append(['lineitem_table', 'L_ORDERKEY', min_range_value, max_range_value, sql_res_size_query5,
                    np.mean(exe_time_5[:]), np.median(exe_time_5[:]), np.std(exe_time_5[:])])

    lines_6.append(['lineitem_table', 'L_ORDERKEY', min_range_value, max_range_value, sql_res_size_query6,
                    np.mean(exe_time_6[:]), np.median(exe_time_6[:]), np.std(exe_time_6[:])])

    lines_7.append(['order_table', 'O_ORDERKEY', min_range_value, max_range_value, sql_res_size_query7,
                    np.mean(exe_time_7[:]), np.median(exe_time_7[:]), np.std(exe_time_7[:])])

    lines_8.append(['order_table', 'O_ORDERKEY', min_range_value, max_range_value, sql_res_size_query8,
                    np.mean(exe_time_8[:]), np.median(exe_time_8[:]), np.std(exe_time_8[:])])

with open('D:\\UofG\\Random_Zipf_1_Results.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    writer.writerows(lines_1)
    writer.writerows(lines_2)
    writer.writerows(lines_3)
    writer.writerows(lines_4)
    writer.writerows(lines_5)
    writer.writerows(lines_6)
    writer.writerows(lines_7)
    writer.writerows(lines_8)
writeFile.close()

cursor.close()   ## Closing the Database Connection Cursor
db_connection.close() ## Closing the Database Connection
