import MySQLdb
from random import randint
import datetime
import time
#import pandas as pd
import numpy as np
import csv


db_connection = MySQLdb.connect(host="localhost", user="root", passwd="050194.Piku", db="lineitem")

cursor = db_connection.cursor()

exe_time = []
count_res_set = []
lines = []
lines.append(['Table_Name',  'AVG', 'STD', 'MED'])

for x in range(5):

    date_range1 = datetime.date(randint(1992, 1995), randint(1, 12), randint(1, 30))
    date_range2 = datetime.date(randint(1996, 1998), randint(1, 12), randint(1, 30))
    mdate1 = str(date_range1.year) + "-" + str(date_range1.month) + "-" + str(date_range1.day)
    mdate2 = str(date_range2.year) + "-" + str(date_range2.month) + "-" + str(date_range2.day)

    sql_query_lineitem1 = "SELECT * FROM lineitem_table WHERE L_SHIPDATE BETWEEN '" + mdate1 + "' AND '" + mdate2 + "' LIMIT 30;"

    globals()["mdate1" + str(x)] = mdate1
    globals()["mdate2" + str(x)] = mdate2


    initial_time = time.time()
    cursor.execute(sql_query_lineitem1)
    df = cursor.fetchall()
    count_res_set.append(len(df))
    time_taken_1 = time.time() - initial_time
    exe_time.append(time_taken_1)
    # df2 = pd.DataFrame(df2.append(df))
    # time_taken_execution = pd.Series(time_taken_execution.append(time_taken_1))






cursor.close()
db_connection.close()


lines.append(['lineitem_table', np.mean(exe_time), np.std(exe_time), np.median(exe_time)])
with open('query_data_0.csv', 'w') as csvFile:
    writer = csv.writer(csvFile)
    writer.writerows(lines)
csvFile.close()



print(exe_time)
print(count_res_set)
print(np.mean(exe_time))
print(np.std(exe_time))
print(np.median(exe_time))
# print(df2)
# print(time_taken_execution)
# print('\n', time_taken_execution.mean())
# print('\n', time_taken_execution.std())
# print('\n', time_taken_execution.median())







# data = {'Set of Result Set': [df.shape[0]], 'Range_Min': [mdate1], 'Range_Max': [mdate2], 'Time Taken': [time_taken1]}
# df2 = pd.DataFrame(data, columns=['Set of Results', 'Range_Min', 'Range_Max', 'Time Taken'])
# df2.to_csv(r'D:\UofG\QueryTest1_1.csv', index=False)


