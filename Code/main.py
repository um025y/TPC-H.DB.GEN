import argparse
import logging
import numpy as np
import sys
sys.path.append(sys.path[0])
import sql_query_gen as sqg
import utils

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

# Parse Arguments
parser=argparse.ArgumentParser(description='TPC-H query generator/benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--distribution', '-D', help = 'Select a distribution uniform or zipf', default = distribution, type = str, choices=['uniform','zipf'])
parser.add_argument('--zipf_a',       '-A', help = 'Set the distribution parameter for zipf (ignored if distribution is "uniform")', default = zipf_a , type = float)
parser.add_argument('--queryCount',   '-C', help = 'Number of query instances per query type (range, join) to execute', default = queryCount , type = utils.check_nonneg)
parser.add_argument('--iterations',   '-I', help = 'Number of iterations for execution of each query instance', default = numIterations, type = utils.check_nonneg)
parser.add_argument('--selectivity',  '-S', help = 'Selectivity value float number within [0,1]', default = selectivity, type = utils.check_01)
parser.add_argument('--scalefactor',  '-F', help = 'TPCH scalefactor', default = scalefactor, type = int, choices=[1, 10, 100])
parser.add_argument('--output',       '-O', help = 'Filename where to store execution stats/results', default = 'test.csv', type = str)
parser.add_argument('--randomseed',   '-R', help = 'Set the random number seed', default = seed, type = utils.check_pos)
parser.add_argument('--verbose',      '-v', help = 'Turn logging on; specify multiple times for more verbosity', action = 'count')

args = parser.parse_args()

log_switcher = {
        None: logging.WARNING,
        1: logging.INFO
        }
logging.basicConfig(level = log_switcher.get(args.verbose, logging.DEBUG), stream = sys.stderr)

logging.getLogger().info("Configuration: [" + args.distribution + " (seed = " +
        str(seed) + ", a = " + str(args.zipf_a) + "), #queries = " +
        str(args.queryCount) + ", #iterations = " + str(args.iterations) +
        ", selectivity = " + str(args.selectivity) + ", scalefactor = " +
        str(args.scalefactor) + "]")

sqg.init_query_executor(args.queryCount, args.selectivity, args.distribution, zipf_a = args.zipf_a, sf = args.scalefactor, seed = args.randomseed)
sqg.run_SQL_executor(args.queryCount, args.iterations, fname = args.output)
