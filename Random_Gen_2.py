import MySQLdb
from random import randint
from datetime import date
import time
import pandas as pd
import numpy as np
import random
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



for x in range(2):  ## specifying the no. of times to execute each query.

    ## Generating random values for min. and max. range and storing them to the empty arrays declared above.
    lineitem_extended_price_range1 = round(random.uniform(900, 110000), 5)
    lineitem_extended_price_range2 = round(random.uniform(900, 110000), 5)
    if lineitem_extended_price_range1 > lineitem_extended_price_range2:
        max_ext_price = lineitem_extended_price_range1
        min_ext_price = lineitem_extended_price_range2
    else:
        max_ext_price = lineitem_extended_price_range2
        min_ext_price = lineitem_extended_price_range1
    min_ext_range_price = str(min_ext_price)
    max_ext_range_price = str(max_ext_price)

    orderkey_range_1 = randint(1, 6000000)
    orderkey_range_2 = randint(1, 6000000)
    if orderkey_range_1 > orderkey_range_2:
        max_orderkey = orderkey_range_1
        min_orderkey = orderkey_range_2
    else:
        max_orderkey = orderkey_range_2
        min_orderkey = orderkey_range_1
    max_orderkey_range = str(max_orderkey)
    min_orderkey_range = str(min_orderkey)

    order_total_price_range1 = round(random.uniform(850, 560000), 5)
    order_total_price_range2 = round(random.uniform(850, 560000), 5)
    if order_total_price_range1 > order_total_price_range2:
        max_total_price = order_total_price_range1
        min_total_price = order_total_price_range2
    else:
        max_total_price = order_total_price_range2
        min_total_price = order_total_price_range1
    order_total_price1 = str(min_total_price)
    order_total_price2 = str(max_total_price)


    sql_res_size_query1 = 0
    sql_res_size_query2 = 0
    sql_res_size_query3 = 0
    sql_res_size_query4 = 0
    sql_res_size_query5 = 0
    sql_res_size_query6 = 0
    sql_res_size_query7 = 0
    sql_res_size_query8 = 0



    for y in range(5):

        sql_query_lineitem2 = "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price + "' AND '" + max_ext_range_price + "';"

        initial_time_1 = time.time()
        cursor.execute(sql_query_lineitem2)
        df = cursor.fetchall()
        sql_res_size_query1 = len(df)
        time_taken_1 = time.time() - initial_time_1
        exe_time_1.append(time_taken_1)

        sql_query_join_lineitem_3 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                                 WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price + "' AND '" + max_ext_range_price + "';"

        initial_time_2 = time.time()
        cursor.execute(sql_query_join_lineitem_3)
        df = cursor.fetchall()
        sql_res_size_query2= len(df)
        time_taken_2 = time.time() - initial_time_2
        exe_time_2.append(time_taken_2)

        sql_query_order2 = "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "';"

        initial_time_3 = time.time()
        cursor.execute(sql_query_order2)
        df = cursor.fetchall()
        sql_res_size_query3 = len(df)
        time_taken_3 = time.time() - initial_time_3
        exe_time_3.append(time_taken_3)

        sql_query_join_order_3 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                                         WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "';"

        initial_time_4 = time.time()
        cursor.execute(sql_query_join_order_3)
        df = cursor.fetchall()
        sql_res_size_query4 = len(df)
        time_taken_4 = time.time() - initial_time_4
        exe_time_4.append(time_taken_4)

        sql_query_lineitem3 = "SELECT * FROM lineitem_table WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

        initial_time_5 = time.time()
        cursor.execute(sql_query_lineitem3)
        df = cursor.fetchall()
        sql_res_size_query5 = len(df)
        time_taken_5 = time.time() - initial_time_5
        exe_time_5.append(time_taken_5)

        sql_query_join_lineitem_1 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                         WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

        initial_time_6 = time.time()
        cursor.execute(sql_query_join_lineitem_1)
        df = cursor.fetchall()
        sql_res_size_query6 = len(df)
        time_taken_6 = time.time() - initial_time_6
        exe_time_6.append(time_taken_6)

        sql_query_join_order_2 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                                     WHERE O_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

        initial_time_7 = time.time()
        cursor.execute(sql_query_join_order_2)
        df = cursor.fetchall()
        sql_res_size_query7 = len(df)
        time_taken_7 = time.time() - initial_time_7
        exe_time_7.append(time_taken_7)

        sql_query_order3 = "SELECT * FROM order_table WHERE O_ORDERKEY = '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

        initial_time_8 = time.time()
        cursor.execute(sql_query_order3)
        df = cursor.fetchall()
        sql_res_size_query8 = len(df)
        time_taken_8 = time.time() - initial_time_8
        exe_time_8.append(time_taken_8)



    lines_1.append(['lineitem_table', 'L_EXTENDEDPRICE', min_ext_range_price, max_ext_range_price, sql_res_size_query1,
                  np.mean(exe_time_1[:]), np.median(exe_time_1[:]), np.std(exe_time_1[:])])

    lines_2.append(['lineitem_table', 'L_EXTENDEDPRICE', min_ext_range_price, max_ext_range_price, sql_res_size_query2,
                   np.mean(exe_time_2[:]), np.median(exe_time_2[:]), np.std(exe_time_2[:])])

    lines_3.append(['order_table', 'O_TOTALPRICE', order_total_price1, order_total_price1, sql_res_size_query3,
                    np.mean(exe_time_3[:]), np.median(exe_time_3[:]), np.std(exe_time_3[:])])

    lines_4.append(['order_table', 'O_TOTALPRICE', order_total_price1, order_total_price1, sql_res_size_query4,
                    np.mean(exe_time_4[:]), np.median(exe_time_4[:]), np.std(exe_time_4[:])])

    lines_5.append(['lineitem_table', 'L_ORDERKEY', min_orderkey_range, max_orderkey_range, sql_res_size_query5,
                    np.mean(exe_time_5[:]), np.median(exe_time_5[:]), np.std(exe_time_5[:])])

    lines_6.append(['lineitem_table', 'L_ORDERKEY', min_orderkey_range, max_orderkey_range, sql_res_size_query6,
                    np.mean(exe_time_6[:]), np.median(exe_time_6[:]), np.std(exe_time_6[:])])

    lines_7.append(['order_table', 'O_ORDERKEY', min_orderkey_range, max_orderkey_range, sql_res_size_query7,
                    np.mean(exe_time_7[:]), np.median(exe_time_7[:]), np.std(exe_time_7[:])])

    lines_8.append(['order_table', 'O_ORDERKEY', min_orderkey_range, max_orderkey_range, sql_res_size_query8,
                    np.mean(exe_time_8[:]), np.median(exe_time_8[:]), np.std(exe_time_8[:])])

with open('D:\\UofG\\Random_Gen_1_Results.csv', 'w') as writeFile:
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