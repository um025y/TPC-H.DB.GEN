import MySQLdb
import random
from random import randint
import datetime
from datetime import timedelta
#import time
#import numpy as np
import pandas as pd

db_connection = MySQLdb.connect(host="localhost", user="root", passwd="050194.Piku", db = "lineitem")

cursor = db_connection.cursor()



for x in range(2):
    date_range1 = datetime.date(randint(1992, 1995), randint(1, 12), randint(1, 30))
    date_range2 = datetime.date(randint(1996, 1998), randint(1, 12), randint(1, 30))
    mdate1 = str(date_range1.year) + "-" + str(date_range1.month) + "-" + str(date_range1.day)
    mdate2 = str(date_range2.year) + "-" + str(date_range2.month) + "-" + str(date_range2.day)

    orderkey = str(randint(1, 6000000))

    lineitem_extended_price_range1 = round(random.uniform(900, 90000), 5)
    lineitem_extended_price_range2 = round(random.uniform(90001, 110000), 5)
    lineitem_ext_price1 = str(lineitem_extended_price_range1)
    lineitem_ext_price2 = str(lineitem_extended_price_range2)

    order_total_price_range1 = round(random.uniform(850, 85000), 5)
    order_total_price_range2 = round(random.uniform(85001, 560000), 5)
    order_total_price1 = str(order_total_price_range1)
    order_total_price2 = str(order_total_price_range2)



    sql_query_lineitem1 = "SELECT * FROM lineitem_table WHERE L_SHIPDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 10;"
    sql_query_lineitem2 = "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + lineitem_ext_price1 + "' AND '" + lineitem_ext_price2 + "' LIMIT 10;"
    sql_query_lineitem3 = "SELECT * FROM lineitem_table WHERE L_ORDERKEY = '" + orderkey + "' LIMIT 10;"
    sql_query_order4 = "SELECT * FROM order_table WHERE O_ORDERKEY = '" + orderkey + "' LIMIT 10;"
    sql_query_order5 = "SELECT * FROM order_table WHERE O_ORDERDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 10;"
    sql_query_order6 = "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "' LIMIT 10;"
    sql_query_join = "SELECT * FROM lineitem_table INNER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY LIMIT 10;"

    globals()["mdate1" + str(x)] = mdate1
    globals()["mdate2" + str(x)] = mdate2
    globals()["ext_price1" + str(x)] = lineitem_ext_price1
    globals()["ext_price2" + str(x)] = lineitem_ext_price2
    globals()["orderkey" + str(x)] = orderkey
    globals()["total_price1" + str(x)] = order_total_price1
    globals()["total_price2" + str(x)] = order_total_price2

    # cursor.execute(sql_query_lineitem1)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query1_' + str(x) + '.csv', index=False)
    #average_execution_sum = 0

    #initial_time1 = time.time()
    # cursor.execute(sql_query_lineitem1)
    # globals()["df_01" + str(x)] = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])




    #time_taken1 = time.time() - initial_time

    # cursor.execute(sql_query_lineitem2)
    # globals()["df2_" + str(x)] = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # #df.to_csv(r'D:\UofG\Query2_' + str(x) + '.csv', index=False)
    #
    #
    # cursor.execute(sql_query_lineitem3)
    # df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query3_' + str(x) + '.csv', index=False)
    #
    # cursor.execute(sql_query_order4)
    # df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query4_' + str(x) + '.csv', index=False)
    # #
    # #
    # cursor.execute(sql_query_order5)
    # df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query5_' + str(x) + '.csv', index=False)
    # #
    # #
    # cursor.execute(sql_query_order6)
    # df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query6_' + str(x) + '.csv', index=False)
    # #
    # #
    # cursor.execute(sql_query_join)
    # df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    # df.to_csv(r'D:\UofG\Query7_' + str(x) + '.csv', index=False)
    # #




cursor.close()
db_connection.close()

print(df)
