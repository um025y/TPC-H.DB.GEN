import argparse
import logging
import sys
import SQL_QUERY_GEN_Ver3_2

logging.basicConfig(level=logging.WARNING, stream = sys.stderr)

# Number of times to execute each query with different query parameter
maxQueryCnt = 10
# Number of iterations to execute query with same query parameter
maxIterations = 5
# Total number of different types of queries
queryTypeCnt = 8

# Parse Arguments
parser=argparse.ArgumentParser(description='SOME Description')
parser.add_argument('--queryCount',     '-Q', help = 'Total number of queries to execute', default = maxQueryCnt, type = int)
parser.add_argument('--maxIterations',  '-I', help = 'Number of iterations for execution of each query', default = maxIterations, type = int)
parser.add_argument('--queryTypeCount', '-T', help = 'Total number of different types of queries', default = queryTypeCnt, type = int)
parser.add_argument('--verbose',        '-v', help = 'Turn logging on; specify multiple times for more verbosity', action = 'count')

args = parser.parse_args()

if args.verbose == 1:
    logging.getLogger().setLevel(logging.INFO)
elif args.verbose != None:
    logging.getLogger().setLevel(logging.DEBUG)

maxQueryCnt = args.queryCount
maxIterations = args.maxIterations
queryTypeCnt = args.queryTypeCount

SQL_QUERY_GEN_Ver3_2.init_query_executor(maxQueryCnt, maxIterations, queryTypeCnt)

output = SQL_QUERY_GEN_Ver3_2.run_SQL_executor()


SQL_QUERY_GEN_Ver3_2.write_results_to_file('D:\\UofG\\Random_Gen_1_5_Zipf_Results.csv', output)


