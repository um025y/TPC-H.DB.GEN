import MySQLdb
import time
import numpy as np
import csv

"""
    Genrate random values from zipf for a given
    Minimum and Maximum values using zipf distribution
"""

def random_zipf(a: np.float64, min: np.uint64, max: np.uint64, size=None):
    """
    Generate Zipf-like random variables,
    but in inclusive [min...max] interval
    """
    if min == 0:
        raise ZeroDivisionError("")

    v   = np.arange(min, max+1) # values to sample
    p   = 1.0 / np.power(v, a)  # probabilities
    p   /= np.sum(p)            # normalized

    return np.random.choice(v, size=size, replace=True, p=p)


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
            val_2 = np.random.uniform(val_1, maxVal)
        else:
            a = 1.01
            val_1 = random_zipf(a, minVal, maxVal)
            val_2 = random_zipf(a, val_1, maxVal)

        if pr > 0:
            price_1 = round(val_1, pr)
            price_2 = round(val_2, pr)
        else:
            price_1 = round(val_1)
            price_2 = round(val_2)


        min_values.append(str(price_1))
        max_values.append(str(price_2))


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
        0: "SELECT * FROM lineitem_table WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price[idx] + "' AND '" + max_ext_range_price[idx] + "';",
        1: "SELECT * FROM lineitem_table INNER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY WHERE L_EXTENDEDPRICE BETWEEN '" + min_ext_range_price[idx] + "' AND '" + max_ext_range_price[idx] + "';",
        2: "SELECT * FROM order_table WHERE O_TOTALPRICE BETWEEN '" + min_total_price[idx] + "' AND '" + max_total_price[idx] + "';",
        3: "SELECT * FROM order_table INNER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY WHERE O_TOTALPRICE BETWEEN '" + min_total_price[idx] + "' AND '" + max_total_price[idx] + "';",
        4: "SELECT * FROM lineitem_table WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';",
        5: "SELECT * FROM lineitem_table INNER JOIN order_table ON lineitem_table.L_ORDERKEY = order_table.O_ORDERKEY WHERE L_ORDERKEY BETWEEN '" + min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';",
        6: "SELECT * FROM order_table INNER JOIN lineitem_table ON order_table.O_ORDERKEY = lineitem_table.L_ORDERKEY WHERE O_ORDERKEY BETWEEN '" + min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';",
        7: "SELECT * FROM order_table WHERE O_ORDERKEY BETWEEN '" + min_orderkey_range[idx] + "' AND '" + max_orderkey_range[idx] + "';"
    }
    return switcher.get(case, "")


"""
    Get CSV line based on query used
"""

def get_CSV_line(case, idx, size, exe_time):
    switcher = {
        0: ['Model_1', 'lineitem_table', 'L_EXTENDEDPRICE', 'SELECTION QUERY', min_ext_range_price[idx], max_ext_range_price[idx], size,
            np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        1: ['Model_2','lineitem_table', 'L_EXTENDEDPRICE', 'JOIN QUERY', min_ext_range_price[idx], max_ext_range_price[idx], size,
            np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        2: ['Model_3','order_table', 'O_TOTALPRICE', 'SELECTION QUERY', min_total_price[idx], max_total_price[idx], size,
            np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        3: ['Model_4','order_table', 'O_TOTALPRICE', 'JOIN QUERY', min_total_price[idx], max_total_price[idx], size, np.min(exe_time[:]), np.max(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        4: ['Model_5','lineitem_table', 'L_ORDERKEY', 'SELECTION QUERY', min_orderkey_range[idx], max_orderkey_range[idx], size,
            np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        5: ['Model_6','lineitem_table', 'L_ORDERKEY', 'JOIN QUERY', min_orderkey_range[idx], max_orderkey_range[idx], size,
            np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])],
        6: ['Model_7','order_table', 'O_ORDERKEY', 'JOIN QUERY', min_orderkey_range[idx], max_orderkey_range[idx], size, np.min(exe_time[:]), np.max(exe_time[:]),
            np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:]) ],
        7: ['Model_8','order_table', 'O_ORDERKEY', 'SELECTION QUERY', min_orderkey_range[idx], max_orderkey_range[idx], size, np.min(exe_time[:]),
            np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:]) ]
    }
    return switcher.get(case, "")



"""
    Main program to call running of SQL executor
"""

def run_SQL_executor():

    # CSV File header
    ouputLines  = []
    columns     = ['Model_Num', 'Table_Name', 'Column_Name', 'Query_Type', 'Range_Min', 'Range_Max', 'Result Set Returned',
                   'Min_Execution_Time', 'Max_Execution_Time', 'Avg_Execution_Time', 'Med_Execution_Time','Std_Deviation_Exe_Time']
    ouputLines.append(columns)

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

    cursor.close()          # Closing the Database Connection Cursor
    # db_connect.close()   # Closing the Database Connection

    return ouputLines

"""
    Write all results into CSV based on the fileName passed
"""

def write_results_to_file(fName, output):
    # Write all the results obtained in output array into file

    with open(fName, 'w', newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(output)

    writeFile.close()

def db_connect(hostName, userName, password, dbName):
    connection = MySQLdb.connect(host=hostName, user=userName, passwd=password, db=dbName)

    return connection.cursor()

"""
    Initialize all the necessary parameters
"""

def init_query_executor(queryCnt, maxIters, qType):
    global maxQueryCnt, maxIterations, queryTypeCnt, min_ext_range_price, max_ext_range_price, min_orderkey_range,\
        max_orderkey_range, min_total_price, max_total_price, cursor
    maxQueryCnt     = queryCnt
    maxIterations   = maxIters
    queryTypeCnt    = qType

    cursor = db_connect('localhost', 'root', '050194.Piku', 'lineitem')


    Query1 = "SELECT MIN(L_EXTENDEDPRICE) as MIN_PRICE, MAX(L_EXTENDEDPRICE) as MAX_PRICE  FROM lineitem_table"
    cursor.execute(Query1)
    extended_price_min_max = cursor.fetchone()
    extended_price_min = extended_price_min_max[0]
    extended_price_max = extended_price_min_max[1]

    Query2 = "SELECT MIN(O_TOTALPRICE) as MIN_TOTALPRICE, MAX(O_TOTALPRICE) as MAX_TOTALPRICE  FROM order_table"
    cursor.execute(Query2)
    total_price_min_max = cursor.fetchone()
    total_price_min = total_price_min_max[0]
    total_price_max = total_price_min_max[1]

    Query3 = "SELECT MIN(O_ORDERKEY) as MIN_ORDERKEY, MAX(O_ORDERKEY) as MAX_ORDERKEY  FROM order_table"
    cursor.execute(Query3)
    orderKey_min_max = cursor.fetchone()
    orderKey_min = orderKey_min_max[0]
    orderKey_max = orderKey_min_max[1]

    # Generate random price range values

    temp = generate_minmax_range(extended_price_min, extended_price_max, maxQueryCnt, 5, 'uniform')
    min_ext_range_price = temp["min_values"]
    max_ext_range_price = temp["max_values"]

    # Generate random order key range values

    temp = generate_minmax_range(orderKey_min, orderKey_max, maxQueryCnt, 0, 'uniform')
    min_orderkey_range = temp["min_values"]
    max_orderkey_range = temp["max_values"]

    # Generate random totalprice  range values

    temp = generate_minmax_range(total_price_min, total_price_max, maxQueryCnt, 5, 'uniform')
    min_total_price = temp["min_values"]
    max_total_price = temp["max_values"]
