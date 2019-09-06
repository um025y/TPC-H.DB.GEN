import argparse
from multiprocessing import cpu_count
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNet, Lasso, LinearRegression, SGDRegressor
from sklearn.metrics import make_scorer, mean_squared_error, r2_score, explained_variance_score
from sklearn.model_selection import GridSearchCV, KFold, cross_val_score, train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor
import sys
import utils

# Default #folds for cross-validation
num_folds = 10

# Default query type
querytype = 'RANGE'

# Default query attribute
queryattr = 'o_orderkey'

# TPCH scalefactor
scalefactor = 1

# Distribution
distribution  = 'Zipf'

modelprefix='Model_'

def run_train_test(scalefactor, distribution, querytype, attribute, num_folds, verbose):
    df = pd.read_csv(Path(sys.path[0] + '/../Results') / ('SF' + str(args.scalefactor) + '_' + args.distribution + '.csv'))
    df1 = df.loc[df['Model_Num'] == (modelprefix + args.querytype + "_" + args.attribute)]

    X_all = df1[['Range_Min', 'Range_Max']]
    X_all = X_all.assign(Range = lambda x: (x['Range_Max'] - x['Range_Min']))
    X_all = X_all.drop(columns = ['Range_Max'])
    Y_all = df1[['Avg_Execution_Time']]

    X, X_test, Y, Y_test = train_test_split (X_all, Y_all, test_size = 0.2)

    pipelines = [
        {
            'name': 'LR',
            'scaled': False,
            'model': LinearRegression(),
            'params': { 'normalize':[True] }
        },
        {
            'name': 'ScaledLR',
            'scaled': True,
            'model': LinearRegression(),
            'params': { 'normalize':[False] }
        },
        {
            'name': 'LASSO',
            'scaled': False,
            'model': Lasso(),
            'params': { 'alpha':[1.0, 0.5, 0.1], 'normalize':[True], 'selection':['cyclic','random'], 'precompute': [True, False], 'positive': [True, False], 'max_iter':[1000,5000] }
        },
        {
            'name': 'ScaledLASSO',
            'scaled': True,
            'model': Lasso(),
            'params': { 'alpha':[1.0, 0.5, 0.1], 'normalize':[False], 'selection':['cyclic','random'], 'precompute': [True, False], 'positive': [True, False], 'max_iter':[1000,5000] }
        },
        {
            'name': 'EN',
            'scaled': False,
            'model': ElasticNet(),
            'params': { 'alpha':[1.0, 0.5, 0.1], 'l1_ratio':[0.01, .33, .66, 1.0], 'normalize':[True], 'selection':['cyclic','random'], 'precompute': [True, False], 'positive': [True, False], 'max_iter':[1000,5000] }
        },
        {
            'name': 'ScaledEN',
            'scaled': True,
            'model': ElasticNet(),
            'params': { 'alpha':[1.0, 0.5, 0.1], 'l1_ratio':[0.01, .33, .66, 1.0], 'normalize':[False], 'selection':['cyclic','random'], 'precompute': [True, False], 'positive': [True, False], 'max_iter':[1000,5000] }
        },
#       {
#           'name': 'KNN',
#           'scaled': False,
#           'model': KNeighborsRegressor(),
#           'params': { 'n_neighbors':[3,5,7], 'weights':['uniform','distance'], 'algorithm':['auto'], 'leaf_size':[15,30,45], 'p':[1, 2, 3] }
#       },
        {
            'name': 'ScaledKNN',
            'scaled': True,
            'model': KNeighborsRegressor(),
            'params': { 'n_neighbors':[3,5,7], 'weights':['uniform','distance'], 'algorithm':['auto'], 'leaf_size':[15,30,45], 'p':[1, 2, 3] }
        },
#       {
#           'name': 'SGD',
#           'scaled': False,
#           'model': SGDRegressor(),
#           'params': { 'loss':['squared_loss', 'huber', 'epsilon_insensitive', 'squared_epsilon_insensitive'], 'penalty':['l2','l1','elasticnet','none'], 'alpha':[.0001, .0005, .001], 'l1_ratio':[0.01, .33, .66, 1.0], 'shuffle': [True, False], 'learning_rate':['constant', 'optimal', 'invscaling', 'adaptive'], 'eta0':[0.005, 0.01, 0.02], 'power_t':[0.2, 0.5, 0.7], 'early_stopping':[True, False], 'validation_fraction':[0.1, 0.2], 'max_iter':[1000,5000] }
#       },
#       {
#           'name': 'ScaledSGD',
#           'scaled': True,
#           'model': SGDRegressor(),
#           'params': { 'loss':['squared_loss', 'huber', 'epsilon_insensitive', 'squared_epsilon_insensitive'], 'penalty':['l2','l1','elasticnet','none'], 'alpha':[.0001, .0005, .001], 'l1_ratio':[0.01, .33, .66, 1.0], 'shuffle':[True, False], 'learning_rate':['constant', 'optimal', 'invscaling', 'adaptive'], 'eta0':[0.005, 0.01, 0.02], 'power_t':[0.2, 0.5, 0.7], 'early_stopping':[True, False], 'validation_fraction':[0.1, 0.2], 'max_iter':[1000,5000] }
#       },
        {
            'name': 'DTR',
            'scaled': False,
            'model': DecisionTreeRegressor(),
            'params': { 'criterion':['mse','friedman_mse','mae'], 'splitter':['best','random'], 'max_depth':[3,6,None], 'min_samples_split':[.01,.1,.2], 'min_samples_leaf':[1,3], 'max_features':[1, 2] }
        },
        {
            'name': 'ScaledDTR',
            'scaled': True,
            'model': DecisionTreeRegressor(),
            'params': { 'criterion':['mse','friedman_mse','mae'], 'splitter':['best','random'], 'max_depth':[3,6,None], 'min_samples_split':[.01,.1,.2], 'min_samples_leaf':[1,3], 'max_features':[1, 2] }
        },
        {
            'name': 'GBM',
            'scaled': False,
            'model': GradientBoostingRegressor(),
            'params': { 'n_estimators':[100, 200], 'loss':['ls', 'lad', 'huber'], 'learning_rate':[.05, .01, .005], 'max_depth':[3, 6], 'min_samples_split':[.01,.1,.2], 'min_samples_leaf':[1,3], 'max_features':[1, 2], 'subsample':[1.0, 0.9] }
        },
        {
            'name': 'ScaledGBM',
            'scaled': True,
            'model': GradientBoostingRegressor(),
            'params': { 'n_estimators':[100, 200], 'loss':['ls', 'lad', 'huber'], 'learning_rate':[.05, .01, .005], 'max_depth':[3, 6], 'min_samples_split':[.01,.1,.2], 'min_samples_leaf':[1,3], 'max_features':[1, 2], 'subsample':[1.0, 0.9] }
        }
    ]

    num_jobs = cpu_count()
    X_scaled = StandardScaler().fit(X).transform(X)

    best_score = -1000
    best_model = None
    best_pipeline = None
    print("Grid-searching " + str(len(pipelines)) + " models...")
    pipeidx = 0
    for pipeline in pipelines:
        if pipeline['scaled'] == True:
            _X = X_scaled.copy()
        else:
            _X = X.copy()
        pipeidx += 1
        print("Processing model " + str(pipeidx) + "/" + str(len(pipelines)) + " (" + pipeline['name'] + ")... ", end="")
        sys.stdout.flush()
        grid        = GridSearchCV(estimator=pipeline['model'], param_grid=pipeline['params'], scoring='explained_variance', cv=num_folds, n_jobs=num_jobs, error_score=np.nan, verbose=args.verbose)
        grid_result = grid.fit(_X, Y)
        print("Done")

        print("Best %s: %f using %s" % (pipeline['name'], grid_result.best_score_, grid_result.best_params_))
        if best_score < grid_result.best_score_:
            best_score = grid_result.best_score_
            best_model = grid_result.best_estimator_
            best_pipeline = pipeline
        sys.stdout.flush()

    print("Best estimator: " + str(best_model))

    if best_pipeline['scaled'] == True:
        _X = StandardScaler().fit(X_test).transform(X_test)
    else:
        _X = X_test
    predictions = best_model.predict(_X)
    print("Regression (explained variance) score over the validation set: " + str(explained_variance_score(Y_test, predictions)))

    compare = pd.DataFrame({'Prediction': predictions, 'Validation Data' : Y_test})
    print(compare)

# Parse Arguments
parser=argparse.ArgumentParser(description='TPC-H query runtime/result set size predictor', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--distribution', '-D', help = 'Select a distribution uniform or zipf', default = distribution, type = str, choices=['Uniform','Zipf'])
parser.add_argument('--scalefactor',  '-F', help = 'TPCH scalefactor', default = scalefactor, type = int, choices=[1, 10, 100])
parser.add_argument('--querytype', '-t', help = 'Query type for which to build the model', default = querytype, type = str, choices=['RANGE', 'JOIN'])
parser.add_argument('--attribute', '-a', help = 'Query constraint attribute', default = queryattr, type = str, choices=['o_orderkey','o_totalprice','l_orderkey', 'l_extendedprice'])
parser.add_argument('--folds',     '-f', help = 'Number of folds to use in cross-validation', default = num_folds, type = utils.check_nonneg)
parser.add_argument('--verbose',   '-v', help = 'Turn logging on; specify multiple times for more verbosity', action = 'count')

args = parser.parse_args()
if args.verbose == None:
    args.verbose = 0

run_train_test(args.scalefactor, args.distribution, args.querytype, args.attribute, args.folds, args.verbose)
