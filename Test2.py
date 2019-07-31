import MySQLdb
from random import randint
import random
import datetime
import time
import pandas as pd


db_connection = MySQLdb.connect(host="localhost", user="root", passwd="050194.Piku", db = "lineitem")

cursor = db_connection.cursor()


sql_1_df = pd.DataFrame()
time_taken_execution_1 = pd.Series()

sql_2_df = pd.DataFrame()
time_taken_execution_2 = pd.Series()

sql_3_df = pd.DataFrame()
time_taken_execution_3 = pd.Series()

sql_4_df = pd.DataFrame()
time_taken_execution_4 = pd.Series()

sql_5_df = pd.DataFrame()
time_taken_execution_5 = pd.Series()

sql_6_df = pd.DataFrame()
time_taken_execution_6 = pd.Series()

for x in range(20):

    orderkey = str(randint(1, 6000000))

    date_range1 = datetime.date(randint(1992, 1995), randint(1, 12), randint(1, 30))
    date_range2 = datetime.date(randint(1996, 1998), randint(1, 12), randint(1, 30))
    mdate1 = str(date_range1.year) + "-" + str(date_range1.month) + "-" + str(date_range1.day)
    mdate2 = str(date_range2.year) + "-" + str(date_range2.month) + "-" + str(date_range2.day)

    lineitem_extended_price_range1 = round(random.uniform(900, 90000), 5)
    lineitem_extended_price_range2 = round(random.uniform(90001, 110000), 5)
    lineitem_ext_price1 = str(lineitem_extended_price_range1)
    lineitem_ext_price2 = str(lineitem_extended_price_range2)

    order_total_price_range1 = round(random.uniform(850, 85000), 5)
    order_total_price_range2 = round(random.uniform(85001, 560000), 5)
    order_total_price1 = str(order_total_price_range1)
    order_total_price2 = str(order_total_price_range2)

    # globals()["mdate1" + str(x)] = mdate1
    # globals()["mdate2" + str(x)] = mdate2

    sql_query_lineitem_1 = "SELECT * FROM lineitem_table WHERE L_SHIPDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 30;"

    initial_time_1 = time.time()
    cursor.execute(sql_query_lineitem_1)
    df1 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_1 = pd.Series(time.time() - initial_time_1)
    sql_1_df = pd.DataFrame(sql_1_df.append(df1))
    time_taken_execution_1 = pd.Series(time_taken_execution_1.append(time_taken_1))

    sql_query_order_1 = "SELECT * FROM order_table WHERE O_ORDERDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 30;"

    initial_time_2 = time.time()
    cursor.execute(sql_query_order_1)
    df2 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_2 = pd.Series(time.time() - initial_time_2)
    sql_2_df = pd.DataFrame(sql_2_df.append(df2))
    time_taken_execution_2 = pd.Series(time_taken_execution_2.append(time_taken_2))

    sql_query_lineitem_2 = "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + lineitem_ext_price1 + "' AND '" + lineitem_ext_price2 + "' LIMIT 30;"

    initial_time_3 = time.time()
    cursor.execute(sql_query_lineitem_2)
    df3 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_3 = pd.Series(time.time() - initial_time_3)
    sql_3_df = pd.DataFrame(sql_3_df.append(df3))
    time_taken_execution_3 = pd.Series(time_taken_execution_3.append(time_taken_3))

    sql_query_order_2 = "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + order_total_price1 + "' AND '" + order_total_price2 + "' LIMIT 30;"

    initial_time_4 = time.time()
    cursor.execute(sql_query_order_2)
    df4 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_4 = pd.Series(time.time() - initial_time_4)
    sql_4_df = pd.DataFrame(sql_4_df.append(df4))
    time_taken_execution_4 = pd.Series(time_taken_execution_4.append(time_taken_4))

    sql_query_lineitem_3 = "SELECT * FROM lineitem_table WHERE L_ORDERKEY = '" + orderkey + "' LIMIT 30;"

    initial_time_5 = time.time()
    cursor.execute(sql_query_lineitem_3)
    df5 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_5 = pd.Series(time.time() - initial_time_5)
    sql_5_df = pd.DataFrame(sql_5_df.append(df5))
    time_taken_execution_5 = pd.Series(time_taken_execution_5.append(time_taken_5))

    sql_query_order_3 = "SELECT * FROM order_table WHERE O_ORDERKEY = '" + orderkey + "' LIMIT 30;"

    initial_time_6 = time.time()
    cursor.execute(sql_query_order_3)
    df6 = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    time_taken_6 = pd.Series(time.time() - initial_time_6)
    sql_6_df = pd.DataFrame(sql_6_df.append(df6))
    time_taken_execution_6 = pd.Series(time_taken_execution_6.append(time_taken_6))

cursor.close()
db_connection.close()

print(sql_1_df)
print(time_taken_execution_1)
print('\n', time_taken_execution_1.mean())
print('\n', time_taken_execution_1.std())
print('\n', time_taken_execution_1.median())





# print(sql_2_df)
# print(time_taken_execution_2)
# print('\n', time_taken_execution_2.mean())
# print('\n', time_taken_execution_2.std())
# print('\n', time_taken_execution_2.median())
#
#
# print(sql_3_df)
# print(time_taken_execution_3)
# print('\n', time_taken_execution_3.mean())
# print('\n', time_taken_execution_3.std())
# print('\n', time_taken_execution_3.median())
#
#
# print(sql_4_df)
# print(time_taken_execution_4)
# print('\n', time_taken_execution_4.mean())
# print('\n', time_taken_execution_4.std())
# print('\n', time_taken_execution_4.median())
#
#
# print(sql_5_df)
# print(time_taken_execution_5)
# print('\n', time_taken_execution_5.mean())
# print('\n', time_taken_execution_5.std())
# print('\n', time_taken_execution_5.median())
#
#
# print(sql_6_df)
# print(time_taken_execution_6)
# print('\n', time_taken_execution_6.mean())
# print('\n', time_taken_execution_6.std())
# print('\n', time_taken_execution_6.median())




data1 = {'No of total Result Set Rows': [sql_1_df.shape[0]], 'Range_Min': [mdate1], 'Range_Max': [mdate2], 'Avg. Time Taken': [time_taken_execution_1.mean()], ''}
# df2 = pd.DataFrame(data, columns=['Set of Results', 'Range_Min', 'Range_Max', 'Time Taken'])
# df2.to_csv(r'D:\UofG\QueryTest1_1.csv', index=False)


