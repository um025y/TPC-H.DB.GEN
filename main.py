import argparse
import logging
import numpy as np
import sql_query_gen as sqg
import sys

# Number of times to execute each query with different query parameter
queryCount = 10

# Number of iterations to execute query with same query parameter
numIterations = 5

# Selectivity being a float in [0,1]
selectivity   = 0.01

# Distribution
distribution  = 'uniform'

# Zipf a parameter
zipf_a = 1.2

# TPCH scalefactor
scalefactor = 1

# PRNG seed
seed = np.random.randint(0, 2 ** 32 - 1)

def check_range(_value, minv, maxv = None):
    if maxv == None:
        _maxv = 'inf'
    else:
        _maxv = str(maxv)

    try:
        value = float(_value)
    except ValueError:
        raise argparse.ArgumentTypeError("Value should be in [" + str(minv) + ", " + _maxv + "]")

    if value < minv or (maxv != None and value > maxv):
        raise argparse.ArgumentTypeError("Value should be in [" + str(minv) + ", " + _maxv + "]")
    return value

def check_01(value):
    check_range(value, 0, 1)

def check_int(value, minv, maxv = None):
    try:
        if int(value) != float(value):
            raise argparse.ArgumentTypeError("Value should be an integer")
    except ValueError:
            raise argparse.ArgumentTypeError("Value should be an integer")
    return int(check_range(value, minv, maxv))

def check_nonneg(value):
    return check_int(value, 1)

def check_pos(value):
    return check_int(value, 0)

# Parse Arguments
parser=argparse.ArgumentParser(description='TPC-H query generator/benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--distribution', '-D', help = 'Select a distribution uniform or zipf', default = distribution, type = str, choices=['uniform','zipf'])
parser.add_argument('--zipf_a',       '-A', help = 'Set the distribution parameter for zipf (ignored if distribution is "uniform")', default = zipf_a , type = float)
parser.add_argument('--queryCount',   '-C', help = 'Number of query instances per query type (range, join) to execute', default = queryCount , type = check_nonneg)
parser.add_argument('--iterations',   '-I', help = 'Number of iterations for execution of each query instance', default = numIterations, type = check_nonneg)
parser.add_argument('--selectivity',  '-S', help = 'Selectivity value float number within [0,1]', default = selectivity, type = check_01)
parser.add_argument('--scalefactor',  '-F', help = 'TPCH scalefactor', default = scalefactor, type = int, choices=[1, 10, 100])
parser.add_argument('--output',       '-O', help = 'Filename where to store execution stats/results', default = 'test.csv', type = str)
parser.add_argument('--randomseed',   '-R', help = 'Set the random number seed', default = seed, type = check_pos)
parser.add_argument('--verbose',      '-v', help = 'Turn logging on; specify multiple times for more verbosity', action = 'count')

args = parser.parse_args()

log_switcher = {
        None: logging.WARNING,
        1: logging.INFO
        }
logging.basicConfig(level = log_switcher.get(args.verbose, logging.DEBUG), stream = sys.stderr)

logging.getLogger().info("Using " + args.distribution + " distribution with random seed: " + str(seed))

sqg.init_query_executor(args.queryCount, args.selectivity, args.distribution, zipf_a = args.zipf_a, sf = scalefactor, seed = args.randomseed)
output = sqg.run_SQL_executor(args.queryCount, args.iterations)
sqg.write_results_to_file(args.output, output)
