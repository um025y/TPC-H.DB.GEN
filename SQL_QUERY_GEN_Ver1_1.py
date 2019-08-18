## My List of Library Imports.
import MySQLdb
from random import randint
from datetime import date
import time
import pandas as pd
import numpy as np
import random

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
exe_time_9 = []
exe_time_10 = []
exe_time_11 = []
exe_time_12 = []

## list of empty arrays to store query result rows returned while running in the loop.

sql_res_size_query1 = []
sql_res_size_query2 = []
sql_res_size_query3 = []
sql_res_size_query4 = []
sql_res_size_query5 = []
sql_res_size_query6 = []
sql_res_size_query7 = []
sql_res_size_query8 = []
sql_res_size_query9 = []
sql_res_size_query10 = []
sql_res_size_query11 = []
sql_res_size_query12 = []

## list of empty arrays to store min. and max. range values of date generated randomly in the loop.

min_date_list = []
max_date_list = []

## list of empty arrays to store min. and max. range values of extended price generated randomly in the loop.

lineitem_ext_price1_list = []
lineitem_ext_price2_list = []

## list of empty arrays to store min. and max. range values of total price generated randomly in the loop.

order_total_price1_list = []
order_total_price2_list = []

## list of empty arrays to store min. and max. range values of order keys generated randomly in the loop.
orderkey1_list = []
orderkey2_list = []


for x in range(5): ## specifying the no. of times to execute each query.

## Generating random values for min. and max. range and storing them to the empty arrays declared above.
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


    start_dt = date(1992, 1, 1).replace(day=1, month=1).toordinal()
    end_dt = date(1998, 12, 1).toordinal()
    random_day1 = date.fromordinal(random.randint(start_dt, end_dt))
    random_day2 = date.fromordinal(random.randint(start_dt, end_dt))
    if random_day1 > random_day2:
        max_date = random_day1
        min_date = random_day2

    else:
        max_date = random_day2
        min_date = random_day1
    min_range_date = str(min_date.year) + "-" + str(min_date.month) + "-" + str(min_date.day)
    max_range_date = str(max_date.year) + "-" + str(max_date.month) + "-" + str(max_date.day)

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

    min_date_list.append(min_range_date)
    max_date_list.append(max_range_date)
    lineitem_ext_price1_list.append(min_ext_range_price)
    lineitem_ext_price2_list.append(max_ext_range_price)
    order_total_price1_list.append(order_total_price1)
    order_total_price2_list.append(order_total_price2)
    orderkey1_list.append(min_orderkey_range)
    orderkey2_list.append(max_orderkey_range)

## Running queries on the Database from Python and fetching result and time statisitics and storing them to the empty arrays

    cursor.execute('set GLOBAL max_allowed_packet=67108864')

    sql_no_cache_1 = "SELECT  SQL_NO_CACHE * FROM lineitem_table;"
    cursor.execute(sql_no_cache_1)

    sql_no_cache_2 = "SELECT SQL_NO_CACHE * FROM order_table;"
    cursor.execute(sql_no_cache_2)

    sql_query_lineitem1 = "SELECT * FROM lineitem_table WHERE L_SHIPDATE BETWEEN '" + min_range_date + "' AND '" + max_range_date + "';"

    initial_time_1 = time.time()
    cursor.execute(sql_query_lineitem1)
    df = cursor.fetchall()
    sql_res_size_query1.append(len(df))
    time_taken_1 = time.time() - initial_time_1 ## Storing execution time for each execution in the loop
    exe_time_1.append(time_taken_1)


    sql_query_order1= "SELECT * FROM order_table WHERE O_ORDERDATE BETWEEN '" + min_range_date + "' AND '" + max_range_date + "';"

    initial_time_2 = time.time()
    cursor.execute(sql_query_order1)
    df = cursor.fetchall()
    sql_res_size_query2.append(len(df))
    time_taken_2 = time.time() - initial_time_2
    exe_time_2.append(time_taken_2)

    sql_query_lineitem2 = "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price + "' AND '" + max_ext_range_price + "';"

    initial_time_3 = time.time()
    cursor.execute(sql_query_lineitem2)
    df = cursor.fetchall()
    sql_res_size_query3.append(len(df))
    time_taken_3 = time.time() - initial_time_3
    exe_time_3.append(time_taken_3)

    sql_query_order2 = "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "';"

    initial_time_4 = time.time()
    cursor.execute(sql_query_order2)
    df = cursor.fetchall()
    sql_res_size_query4.append(len(df))
    time_taken_4 = time.time() - initial_time_4
    exe_time_4.append(time_taken_4)

    sql_query_lineitem3 = "SELECT * FROM lineitem_table WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

    initial_time_5 = time.time()
    cursor.execute(sql_query_lineitem3)
    df = cursor.fetchall()
    sql_res_size_query5.append(len(df))
    time_taken_5 = time.time() - initial_time_5
    exe_time_5.append(time_taken_5)

    sql_query_order3 = "SELECT * FROM order_table WHERE O_ORDERKEY = '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

    initial_time_6 = time.time()
    cursor.execute(sql_query_order3)
    df = cursor.fetchall()
    sql_res_size_query6.append(len(df))
    time_taken_6 = time.time() - initial_time_6
    exe_time_6.append(time_taken_6)

    sql_query_join_order_1 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                         WHERE O_ORDERDATE BETWEEN '" + min_range_date + "' AND '" + max_range_date + "';"

    initial_time_7 = time.time()
    cursor.execute(sql_query_join_order_1)
    df = cursor.fetchall()
    sql_res_size_query7.append(len(df))
    time_taken_7 = time.time() - initial_time_7
    exe_time_7.append(time_taken_7)

    sql_query_join_order_2 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                             WHERE O_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

    initial_time_8 = time.time()
    cursor.execute(sql_query_join_order_2)
    df = cursor.fetchall()
    sql_res_size_query8.append(len(df))
    time_taken_8 = time.time() - initial_time_8
    exe_time_8.append(time_taken_8)

    sql_query_join_order_3 = "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY \
                                 WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "';"

    initial_time_9 = time.time()
    cursor.execute(sql_query_join_order_3)
    df = cursor.fetchall()
    sql_res_size_query9.append(len(df))
    time_taken_9 = time.time() - initial_time_9
    exe_time_9.append(time_taken_9)

    sql_query_join_lineitem_1 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                 WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range + "' AND '" + max_orderkey_range + "';"

    initial_time_10 = time.time()
    cursor.execute(sql_query_join_lineitem_1)
    df = cursor.fetchall()
    sql_res_size_query10.append(len(df))
    time_taken_10 = time.time() - initial_time_10
    exe_time_10.append(time_taken_10)

    sql_query_join_lineitem_2 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                     WHERE L_SHIPDATE BETWEEN '" + min_range_date + "' AND '" + max_range_date + "';"

    initial_time_11 = time.time()
    cursor.execute(sql_query_join_lineitem_2)
    df = cursor.fetchall()
    sql_res_size_query11.append(len(df))
    time_taken_11 = time.time() - initial_time_11
    exe_time_11.append(time_taken_11)

    sql_query_join_lineitem_3 = "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY \
                                         WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price + "' AND '" + max_ext_range_price + "';"

    initial_time_12 = time.time()
    cursor.execute(sql_query_join_lineitem_3)
    df = cursor.fetchall()
    sql_res_size_query12.append(len(df))
    time_taken_12 = time.time() - initial_time_12
    exe_time_12.append(time_taken_12)

cursor.close()   ## Closing the Database Connection Cursor
db_connection.close() ## Closing the Database Connection

## Arranging returned result statistics in DataFrames and exporting the results as a .csv  file

data1 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_SHIPDATE', 'Range_Min': min_date_list[:], 'Range_Max': max_date_list[:], 'Result Set Returned': sql_res_size_query1[:],
         'Execution_Time': exe_time_1[:]}
results1 = pd.DataFrame(data1, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results1.to_csv(r'D:\UofG\Results_1\SQL_QUERY_LINEITEM_1_SHIPDATE.csv', index=False)

data2 = {'Table_Name': 'order_table', 'Column_Name': 'O_ORDERDATE', 'Range_Min': min_date_list[:], 'Range_Max': max_date_list[:], 'Result Set Returned': sql_res_size_query2[:],
         'Execution_Time': exe_time_2[:]}
results2 = pd.DataFrame(data2, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results2.to_csv(r'D:\UofG\Results_1\SQL_QUERY_ORDER_1_ORDERDATE.csv', index=False)

data3 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_EXTENDEDPRICE', 'Range_Min': lineitem_ext_price1_list[:], 'Range_Max': lineitem_ext_price2_list[:], 'Result Set Returned': sql_res_size_query3[:],
         'Execution_Time': exe_time_3[:]}
results3 = pd.DataFrame(data3, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results3.to_csv(r'D:\UofG\Results_1\SQL_QUERY_LINEITEM_1_EXTENDEDPRICE.csv', index=False)

data4 = {'Table_Name': 'order_table', 'Column_Name': 'O_TOTALPRICE', 'Range_Min': order_total_price1_list[:], 'Range_Max': order_total_price2_list[:], 'Result Set Returned': sql_res_size_query4[:],
         'Execution_Time': exe_time_4[:]}
results4 = pd.DataFrame(data4, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results4.to_csv(r'D:\UofG\Results_1\SQL_QUERY_ORDER_1_TOTALPRICE.csv', index=False)

data5 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_ORDERKEY', 'Range_Min': orderkey1_list[:], 'Range_Max': orderkey2_list[:], 'Result Set Returned': sql_res_size_query5[:],
         'Execution_Time': exe_time_5[:]}
results5 = pd.DataFrame(data5, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results5.to_csv(r'D:\UofG\Results_1\SQL_QUERY_LINEITEM_1_ORDERKEY.csv', index=False)

data6 = {'Table_Name': 'order_table', 'Column_Name': 'O_ORDERKEY', 'Range_Min': orderkey1_list[:], 'Range_Max': orderkey2_list[:], 'Result Set Returned': sql_res_size_query6[:],
         'Execution_Time': exe_time_6[:]}
results6 = pd.DataFrame(data6, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results6.to_csv(r'D:\UofG\Results_1\SQL_QUERY_ORDER_1_ORDERKEY.csv', index=False)

data7 = {'Table_Name': 'order_table', 'Column_Name': 'O_ORDERDATE', 'Range_Min': min_date_list[:], 'Range_Max': max_date_list[:], 'Result Set Returned': sql_res_size_query7[:],
         'Execution_Time': exe_time_7[:]}
results7 = pd.DataFrame(data7, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results7.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_ORDER_1_ORDERDATE.csv', index=False)

data8 = {'Table_Name': 'order_table', 'Column_Name': 'O_ORDERKEY', 'Range_Min': orderkey1_list[:], 'Range_Max': orderkey2_list[:], 'Result Set Returned': sql_res_size_query8[:],
         'Execution_Time': exe_time_8[:]}
results8 = pd.DataFrame(data8, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results8.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_ORDER_1_ORDERKEY.csv', index=False)

data9 = {'Table_Name': 'order_table', 'Column_Name': 'O_TOTALPRICE', 'Range_Min': order_total_price1_list[:], 'Range_Max': order_total_price2_list[:], 'Result Set Returned': sql_res_size_query9[:],
         'Execution_Time': exe_time_9[:]}
results9 = pd.DataFrame(data9, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results9.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_ORDER_1_TOTALPRICE.csv', index=False)

data10 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_ORDERKEY', 'Range_Min': orderkey1_list[:], 'Range_Max': orderkey2_list[:], 'Result Set Returned': sql_res_size_query10[:],
         'Execution_Time': exe_time_10[:]}
results10 = pd.DataFrame(data10, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results10.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_LINEITEM_1_ORDERKEY.csv', index=False)

data11 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_SHIPDATE', 'Range_Min': min_date_list[:], 'Range_Max': max_date_list[:], 'Result Set Returned': sql_res_size_query11[:],
         'Execution_Time': exe_time_11[:]}
results11 = pd.DataFrame(data11, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results11.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_LINEITEM_1_SHIPDATE.csv', index=False)

data12 = {'Table_Name': 'lineitem_table', 'Column_Name': 'L_EXTENDEDPRICE', 'Range_Min': lineitem_ext_price1_list[:], 'Range_Max': lineitem_ext_price2_list[:], 'Result Set Returned': sql_res_size_query12[:],
         'Execution_Time': exe_time_12[:]}
results12 = pd.DataFrame(data12, columns=['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Execution_Time'])
results12.to_csv(r'D:\UofG\Results_1\SQL_QUERY_JOIN_LINEITEM_1_EXTENDEDPRICE.csv', index=False)

## Computing and arranging execution time statistics in DataFrames and exporting the results as a .csv  file

time_data = {'QUERY NO.': ['QUERY 1', 'QUERY 2', 'QUERY 3', 'QUERY 4', 'QUERY 5', 'QUERY 6', 'QUERY 7', 'QUERY 8',
                           'QUERY 9', 'QUERY 10', 'QUERY 11', 'QUERY 12'],
             'AVG. TIME TAKEN': [np.mean(exe_time_1), np.mean(exe_time_2), np.mean(exe_time_3), np.mean(exe_time_4), np.mean(exe_time_5), np.mean(exe_time_6),
                                 np.mean(exe_time_7), np.mean(exe_time_8), np.mean(exe_time_9), np.mean(exe_time_10), np.mean(exe_time_11), np.mean(exe_time_12)],
             'MEDIAN OF EXECUTION TIME': [np.median(exe_time_1), np.median(exe_time_2), np.median(exe_time_3), np.median(exe_time_4), np.median(exe_time_5),
                                          np.median(exe_time_6), np.median(exe_time_7), np.median(exe_time_8), np.median(exe_time_9), np.median(exe_time_10),
                                          np.median(exe_time_11), np.median(exe_time_12)],
             'STD. DEV. OF EXECUTION TIME': [np.std(exe_time_1), np.std(exe_time_2), np.std(exe_time_3), np.std(exe_time_4), np.std(exe_time_5),
                                             np.std(exe_time_6), np.std(exe_time_7), np.std(exe_time_8), np.std(exe_time_9), np.std(exe_time_10),
                                             np.median(exe_time_11), np.median(exe_time_12)]}
time_data_df = pd.DataFrame(time_data, columns=['QUERY NO.', 'AVG. TIME TAKEN', 'MEDIAN OF EXECUTION TIME', 'STD. DEV. OF EXECUTION TIME'])
time_data_df.to_csv(r'D:\UofG\Results_1\TIME_CALCULATIONS_1.csv', index=False)












