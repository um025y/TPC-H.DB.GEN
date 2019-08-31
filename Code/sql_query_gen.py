import MySQLdb
import csv
import logging
import numpy  as np
import pandas as pd
import time

# Hostname where the database server lives
dbhost = 'localhost'

# DB username
dbuser = 'udayan'

# DB password
dbpass = 'udayan'

# DB prefix
dbprefix = 'TPCH_DATABASE_SCALE_'

# Caution: query string generator assumes that the join condition is on the two first attributes in this dictionary
attrs = {'o_orderkey' : 'order_table', 'l_orderkey' : 'lineitem_table', 'l_extendedprice' : 'lineitem_table', 'o_totalprice' : 'order_table'}
minmax = {}
cursor = None

"""
    Genrate random values from zipf for a given
    Minimum and Maximum values using zipf distribution
"""

def random_zipf(a: np.float64, min: np.uint64, max: np.uint64, perm_vals: list, pdf: list, size = None):
    if len(perm_vals) == 0:
        perm_vals.extend(np.random.permutation(np.arange(min, max + 1)))
    else:
        np.random.permutation(np.arange(min, max + 1)) # For repeatability with fixed random seed
    if len(pdf) == 0:
        _p = 1.0 / np.power(np.arange(1, len(perm_vals) + 1), a)
        _p /= np.sum(_p)
        pdf.extend(_p)
    return np.random.choice(perm_vals, size=size, replace=True, p=pdf)

"""
    Generate Minimum and Maximum values
"""
def generate_minmax(dist, minVal: int, maxVal: int, cnt = 5, selectivity = 0.6, *, zipf_a = 1.2, perm_vals = None, pdf_vals = None):
    min_values  = []
    max_values  = []
    print(type(maxVal), type(minVal), type(selectivity))
    range_val   = (maxVal - minVal + 1) * selectivity
    _maxVal     = maxVal - range_val
    if dist == 'uniform':
        min_values  = np.round(np.random.uniform(minVal, _maxVal, size=cnt), 0)
    elif dist == 'zipf':
        if perm_vals == None or pdf_vals == None:
            raise ValueError('perm_vals and pdf_vals cannot be None for zipf')
        min_values  = random_zipf(a=zipf_a, min=minVal, max=_maxVal, perm_vals=perm_vals, pdf=pdf_vals, size=cnt)
    else:
        raise ValueError('Unknown distribution "' , dist , '"; valid values: "uniform", "zipf"')

    max_values  = min_values + np.ceil(range_val)

    return {"min_values": min_values, "max_values": max_values}


"""
    Get query string with appropriate minimum and max values for execution
"""

def get_query_string(attr, idx, qtype):
    if qtype == 'RANGE':
        qfrom = attrs[attr]
    elif qtype == 'JOIN':
        k = list(attrs.keys())
        v = list(attrs.values())
        qfrom = v[0] + ' INNER JOIN ' + v[1] + ' ON ' + v[0] + '.' + k[0] + ' = ' + v[1] + '.' + k[1]
    else:
        raise ValueError("Query type cannot be '" + qtype + "'; possible values: [range, join]")

    return "select * from " + qfrom + " where " + attr + " between '" + str(minmax[attr]['min'][idx]) + "' AND '" + str(minmax[attr]['max'][idx]) + "';"

"""
    Get CSV line based on query used
"""

def get_CSV_line(attr, idx, qtype, size, exe_time):
    return ['Model_' + qtype + '_' + attr, attrs[attr], attr, qtype, minmax[attr]['min'][idx], minmax[attr]['max'][idx], size, np.min(exe_time[:]), np.max(exe_time[:]), np.median(exe_time[:]), np.std(exe_time[:]), np.mean(exe_time[:])]

"""
    Execute query and return required results
"""

def execute_query(cursor, sql_query):
    logging.getLogger().debug('Executing query "' + sql_query + '"...')
    start_time = time.time()
    cursor.execute(sql_query)
    end_time = time.time()
    logging.getLogger().debug("Done executing query (" + str(end_time - start_time) + "')")
    df = cursor.fetchall()
    return {"size": len(df), "execution_time": (end_time - start_time)}

"""
    Main program to call running of SQL executor
"""

def run_SQL_executor(numInstances, numIterations):

    # CSV File header
    ouputLines  = []
    columns     = ['Model_Num', 'Table_Name', 'Column_Name', 'Query_Type', 'Range_Min', 'Range_Max', 'Result Set Returned',
                   'Min_Execution_Time', 'Max_Execution_Time', 'Med_Execution_Time','Std_Deviation_Exe_Time', 'Avg_Execution_Time']
    ouputLines.append(columns)

    """
        For each query type
        1. For a given range values obtain query string
        2. Execute query string and obtain result size and time taken
        3. Calculate stats for each query type
        4. Finally write these values into a CSV file
    """

    logging.getLogger().info('Executing queries...')
    for qtype in ['RANGE', 'JOIN']:
        for attr in attrs.keys():
            for q in range(numInstances):
                execution_time = []
                temp_time = []
                results = dict()
                for k in range(numIterations):
                    # Get SQL Query
                    query = get_query_string(attr, q, qtype)
                    # Execute and obtain necessary stats
                    logging.getLogger().info("Executing " + qtype + " query " + str(q + 1) + "/" + str(numInstances) + " with constraint on " + attrs[attr] + "." + attr)
                    results = execute_query(cursor, query)
                    execution_time.append(results["execution_time"])
                  
                ouputLines.append(get_CSV_line(attr, q, qtype, results["size"], execution_time))

    logging.getLogger().info('Done executing queries')
    cursor.close()          # Closing the Database Connection Cursor
    
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
    Check if the file or directory at `path` can
    be accessed by the program using `mode` open flags.
"""

def is_accessible(path, mode='r'):
    try:
        f = open(path, mode)
        f.close()
    except IOError:
        return False
    return True

"""
    Initialize all the necessary parameters
"""

def init_query_executor(maxQueryCnt, select, dist, *, zipf_a=1.2, sf = 1, seed = np.random.randint(0, 2 ** 32 - 1)):
    global cursor, minmax

    np.random.seed(seed)

    savePermPdf     = {}

    orderkey_v      = []
    orderkey_p      = []
    ext_price_v     = []
    ext_price_p     = []
    tot_price_v     = []
    tot_price_p     = []
    print(dbprefix + str(sf))
    #connection = .connect(dbhost, dbuser, dbpass, dbprefix + str(sf))


    cursor = db_connect(dbhost, dbuser, dbpass, dbprefix + str(sf))
    #cursor = connection.cursor()
    

    params = {}

    logging.getLogger().info('Gathering statistics...')
    for attr in attrs.keys():
        query = "SELECT MIN(" + attr + "), MAX(" + attr + ") FROM " + attrs[attr]
        cursor.execute(query)
        result = cursor.fetchone()
        params[attr] = {}
        params[attr]['min'] = result[0]
        params[attr]['max'] = result[1]
        params[attr]['vals'] = None
        params[attr]['pdf'] = None
    logging.getLogger().info('Done gathering statistics')

    if dist=='zipf':
        # Check and read permutation/probabilities values
        for attr in attrs.keys():
            if savePermPdf.get(attr, None) == None:
                savePermPdf[attr] = {}
            for param in ['vals', 'pdf']:
                filename = attr + '_' + param[0] + "-" + str(seed) + '.csv'
                savePermPdf[attr][param] = is_accessible(filename)
                if savePermPdf[attr][param]:
                    params[attr][param] = pd.read_csv(filename,header=None)[0].values.tolist()
                else:
                    params[attr][param] = []
            
    for attr in attrs:
        # Generate random price range values
        temp = generate_minmax(dist, params[attr]['min'], params[attr]['max'], maxQueryCnt, select, zipf_a = zipf_a, perm_vals = params[attr]['vals'], pdf_vals = params[attr]['pdf'])
        minmax[attr] = {'min' : temp["min_values"], 'max' : temp["max_values"]}

    # First time store permutaion and probalities into a file
    if dist=='zipf':
        for attr in attrs:
            for param in ['vals', 'pdf']:
                if not savePermPdf[attr][param]:
                    np.savetxt(attr + '_' + param[0] + "-" + str(seed) + '.csv', params[attr][param], delimiter=",")
