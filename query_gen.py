import MySQLdb
import random
from random import randint
import sys
import os.path
import datetime
from datetime import timedelta
import time
#import pandas as pd

average_execution_sum = 0


for x in range(2):

    date_range1 = datetime.date(randint(1992, 1995), randint(1, 12), randint(1, 30))
    date_range2 = datetime.date(randint(1996, 1998), randint(1, 12), randint(1, 30))

    mdate1 = str(date_range1.year)+"-"+str(date_range1.month)+"-"+str(date_range1.day)
    mdate2 = str(date_range2.year)+"-"+str(date_range2.month)+"-"+str(date_range2.day)

    orderkey = str(randint(1, 6000000))

    lineitem_extended_price_range1 = round(random.uniform(900, 90000), 5)
    lineitem_extended_price_range2 = round(random.uniform(90001, 110000), 5)
    lineitem_ext_price1 = str(lineitem_extended_price_range1)
    lineitem_ext_price2 = str(lineitem_extended_price_range2)


    order_total_price_range1 = round(random.uniform(850,85000), 5)
    order_total_price_range2 = round(random.uniform(85001,560000), 5)
    order_total_price1 = str(order_total_price_range1)
    order_total_price2 = str(order_total_price_range2)

    db_connection = MySQLdb.connect (host="localhost", user="root", passwd="050194.Piku", db = "lineitem")


    sql_query_lineitem1 = "SELECT CAST(L_ORDERKEY as CHAR(15)), CAST(L_EXTENDEDPRICE as CHAR(20)) FROM lineitem_table WHERE L_SHIPDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 5;"
    sql_query_lineitem2 = "SELECT CAST(L_ORDERKEY as CHAR(15)), CAST(L_SHIPDATE as CHAR(20)) FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + lineitem_ext_price1 + "' AND '" + lineitem_ext_price2 + "' LIMIT 5;"
    sql_query_lineitem3 = "SELECT CAST(L_EXTENDEDPRICE as CHAR(20)), CAST(L_SHIPDATE as CHAR(20))  FROM lineitem_table WHERE L_ORDERKEY = '" + orderkey + "' LIMIT 5;"
    sql_query_order4 = "SELECT CAST(O_TOTALPRICE as CHAR(20)), CAST(O_ORDERDATE as CHAR(20))  FROM order_table WHERE O_ORDERKEY = '" + orderkey + "' LIMIT 5;"
    sql_query_order5 = "SELECT CAST(O_ORDERKEY as CHAR(15)), CAST(O_TOTALPRICE as CHAR(20)) FROM order_table WHERE O_ORDERDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 5;"
    sql_query_order6 = "SELECT CAST(O_ORDERKEY as CHAR(15)), CAST(O_ORDERDATE as CHAR(20)) FROM order_table WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "' LIMIT 5;"
    sql_query_join = "SELECT CAST(lineitem_table.L_ORDERKEY as CHAR(15)), CAST(lineitem_table.L_EXTENDEDPRICE as CHAR(20)), CAST(lineitem_table.L_SHIPDATE as CHAR(20)),\
    CAST(order_table.O_TOTALPRICE as CHAR(20)), CAST(order_TABLE.O_ORDERDATE as CHAR(20)) \
    FROM lineitem_table INNER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY;"




    cursor = db_connection.cursor()

    average_execution_sum = 0

    initial_time1 = time.time()
    cursor.execute(sql_query_lineitem1)
    l_records = cursor.fetchall()


    initial_time2 = time.time()
    cursor.execute(sql_query_lineitem2)
    ext_price = cursor.fetchall()
    time_taken2 = time.time() - initial_time2

    initial_time3 = time.time()
    cursor.execute(sql_query_lineitem3)
    l_orderkey_data = cursor.fetchall()
    time_taken3 = time.time() - initial_time3

    initial_time4 = time.time()
    cursor.execute(sql_query_order4)
    o_orderkey_data = cursor.fetchall()
    time_taken4 = time.time() - initial_time4

    initial_time5 = time.time()
    cursor.execute(sql_query_order5)
    o_records = cursor.fetchall()
    time_taken5 = time.time() - initial_time5

    initial_time6 = time.time()
    cursor.execute(sql_query_order6)
    total_price = cursor.fetchall()
    time_taken6 = time.time() - initial_time6

    initial_time7 = time.time()
    cursor.execute(sql_query_join)
    joint_record = cursor.fetchall()
    time_taken7 = time.time() - initial_time7




    cursor.close()
    db_connection.close()

    save_path = 'D:\\UofG\\'
    name = "QUERY_GEN_DATA_COLLECTION " + str(x+1)
    completeName = os.path.join(save_path, name + ".txt")
    file = open(completeName, 'w')
    sys.stdout = file
    print("This is Generation Number " , x+1)
    print("The date range :", mdate2 ,"to", mdate1)
    print("\nL_ORDERKEY ", "\t", " L_EXTENDEDPRICE")
    for r in l_records:
        print(r[0].ljust(11), "\t", r[1].ljust(20))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken1)))
    average_execution_sum += time_taken1
    average_execution_time1 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time1)

    print("\nThe extended price range :", lineitem_ext_price2, "to", lineitem_ext_price1)
    print("\nL_ORDERKEY ", "\t", " L_SHIPDATE")
    for m in ext_price:
        print(m[0].ljust(11), "\t", m[1].ljust(10))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken2)))
    average_execution_sum += time_taken2
    average_execution_time2 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time2)

    print("\nThe Order Key is:", orderkey)
    print("\nL_EXTENDEDPRICE ", "\t", " L_SHIPDATE")
    for n in l_orderkey_data:
        print(n[0].ljust(20), "\t", n[1].ljust(10))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken3)))
    average_execution_sum += time_taken3
    average_execution_time3 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time3)

    print("\nThe Order Key is:", orderkey)
    print("\nO_TOTALPRICE ", "\t", "O_ORDERDATE")
    for p in o_orderkey_data:
        print(p[0].ljust(20), "\t", p[1].ljust(10))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken4)))
    average_execution_sum += time_taken4
    average_execution_time4 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time4)

    print("The date range :", mdate2 ,"to", mdate1)
    print("\nO_ORDERKEY ", "\t", "O_TOTALPRICE")
    for y in o_records:
        print(y[0].ljust(11), "\t", y[1].ljust(20))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken5)))
    average_execution_sum += time_taken5
    average_execution_time5 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time5)

    print("\nThe total price range :", order_total_price2 ,"to", order_total_price2)
    print("\nO_ORDERKEY ", "\t", " O_ORDERDATE")
    for z in total_price:
        print(z[0].ljust(11), "\t", z[1].ljust(10))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken6)))
    average_execution_sum += time_taken6
    average_execution_time6 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time6)

    print("\nThe result for joint query is")
    print("\nL_ORDERKEY ", "\t", "L_EXTENDEDPRICE", "\t", "L_SHIPDATE", "\t", "O_TOTALPRICE", "\t", "O_ORDERDATE",)
    print("The numer of rows for this query is %d", joint_record)
    for c in joint_record:
        print(c[0].ljust(11), "\t", c[1].ljust(20), "\t", c[2].ljust(10), "\t", c[3].ljust(20), "\t", c[4].ljust(10))
    print("\nTime taken to complete the execution : %s secs" % timedelta(seconds=round(time_taken7)))
    average_execution_sum += time_taken7
    average_execution_time7 = average_execution_sum / (x + 1)
    print("\n Average execution time so far : ", average_execution_time7)

    file.close()

