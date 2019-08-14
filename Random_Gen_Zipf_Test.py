import MySQLdb
import time
import numpy as np
import csv

"""
    Generate price or total price range for a given
    Minimum and Maximum price values using, uniform distribution
"""


def generate_minmax_range(minVal, maxVal, cnt=5, pr=0, dist='uniform'):
    min_values = []
    max_values = []
    for i in range(cnt):
        if dist == 'uniform':
            val_1 = np.random.uniform(minVal, maxVal)
            val_2 = np.random.uniform(minVal, maxVal)
        else:
            a = 1.01
            val_1 = np.random.zipf(a)
            val_2 = np.random.zipf(a)

        if pr > 0:
            price_1 = round(val_1, pr)
            price_2 = round(val_2, pr)
        else:
            price_1 = round(val_1)
            price_2 = round(val_2)

        if price_1 < price_2 :
            min_values.append(str(price_1))
            max_values.append(str(price_2))
        else:
            min_values.append(str(price_2))
            max_values.append(str(price_1))

    return {"min_values": min_values, "max_values": max_values}

"""
    Execute query and return required results
"""


def execute_query(cursor, sql_query):
    start_time = time.time()
    cursor.execute(sql_query)
    df = cursor.fetchall()
    end_time = time.time()

    return {"size": len(df), "execution_time": (end_time - start_time)}


"""
    Get query string with appropriate minimum and max values for execution
"""


def get_query_string(case, idx):
    switcher = {
        0: "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price[idx] + "' AND '" +
           max_ext_range_price[idx] + "';",
        1: "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY WHERE L_EXTENDEDPRICE BETWEEN '" +
           min_ext_range_price[idx] + "' AND '" + max_ext_range_price[idx] + "';",
        2: "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + min_total_price[idx] + "' AND '" +
           max_total_price[idx] + "';",
        3: "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY WHERE O_TOTALPRICE BETWEEN '" +
           min_total_price[idx] + "' AND '" + max_total_price[idx] + "';",
        4: "SELECT * FROM lineitem_table WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range[idx] + "' AND '" +
           max_orderkey_range[idx] + "';",
        5: "SELECT * FROM lineitem_table RIGHT OUTER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY WHERE L_ORDERKEY BETWEEN '" +
           min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';",
        6: "SELECT * FROM order_table RIGHT OUTER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY WHERE O_ORDERKEY BETWEEN '" +
           min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';",
        7: "SELECT * FROM order_table WHERE O_ORDERKEY = '" + min_orderkey_range[idx] + "' AND '" + max_orderkey_range[
            idx] + "';"
    }
    return switcher.get(case, "")


"""
    Get CSV line based on query used
"""


def get_CSV_line(case, idx, size, exe_time):
    switcher = {
        0: ['lineitem_table', 'L_EXTENDEDPRICE', min_ext_range_price[idx], max_ext_range_price[idx], size,
            np.mean(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:])],
        1: ['lineitem_table', 'L_EXTENDEDPRICE', min_ext_range_price[idx], max_ext_range_price[idx], size,
            np.mean(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:])],
        2: ['order_table', 'O_TOTALPRICE', min_total_price[idx], max_total_price[idx], size, np.mean(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:])],
        3: ['order_table', 'O_TOTALPRICE', min_total_price[idx], max_total_price[idx], size, np.mean(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:])],
        4: ['lineitem_table', 'L_ORDERKEY', min_orderkey_range[idx], max_orderkey_range[idx], size,
            np.mean(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:])],
        5: ['lineitem_table', 'L_ORDERKEY', min_orderkey_range[idx], max_orderkey_range[idx], size,
            np.mean(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:])],
        6: ['order_table', 'O_ORDERKEY', min_orderkey_range[idx], max_orderkey_range[idx], size, np.mean(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:])],
        7: ['order_table', 'O_ORDERKEY', min_orderkey_range[idx], max_orderkey_range[idx], size, np.mean(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:])]
    }
    return switcher.get(case, "")


# Setting up and opening the Database Connection.

db_connection = MySQLdb.connect(host="localhost", user="root", passwd="050194.Piku", db="lineitem")

# Declaring my Database Connection Cursor.
cursor = db_connection.cursor()

# Number of times to execute each query with different query parameter
maxQueryCnt = 2

# Number of iterations to execute query with same query parameter
maxIterations = 5

# Total number of different types of quieries

queryTypeCnt = 8

# CSV File header
ouputLines = []
columns = ['Table_Name', 'Column_Name', 'Range_Min', 'Range_Max', 'Result Set Returned', 'Avg_Execution_Time',
           'Med_Execution_Time', 'Std_Deviation']
ouputLines.append(columns)

# Generate random price range values

temp = generate_minmax_range(900, 110000, maxQueryCnt, 5)
min_ext_range_price = temp["min_values"]
max_ext_range_price = temp["max_values"]

# Generate random order key range values

temp = generate_minmax_range(1, 6000000, maxQueryCnt, 0)
min_orderkey_range = temp["min_values"]
max_orderkey_range = temp["max_values"]

# Generate random totalprice  range values

temp = generate_minmax_range(850, 560000, maxQueryCnt, 5)
min_total_price = temp["min_values"]
max_total_price = temp["max_values"]

"""
    For each query type
    1. For a given range values obtain query string
    2. Execute query string and obtain result size and time taken
    3. Calculate stats for each query type
    4. Finally write these values into a CSV file
"""

for i in range(queryTypeCnt):
    for j in range(maxQueryCnt):
        execution_time = []
        results = dict()
        for k in range(maxIterations):
            # Get SQL Query
            query = get_query_string(i, j)
            # Execute and obtain necessary stats
            results = execute_query(cursor, query)
            execution_time.append(results["execution_time"])

        ouputLines.append(get_CSV_line(i, j, results["size"], execution_time))

    print("Completed execution of " + str(i + 1) + " out of " + str(queryTypeCnt) + " query")

# Write all the results into CSV file

with open('D:\\UofG\\Random_Gen_1_1_Zipf_Results.csv', 'w', newline='') as writeFile:
    writer = csv.writer(writeFile)
    writer.writerows(ouputLines)

writeFile.close()  # Closing file being written
cursor.close()  # Closing the Database Connection Cursor
db_connection.close()  # Closing the Database Connection