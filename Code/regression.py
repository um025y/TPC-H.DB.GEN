import numpy as np
import pandas as pd
import csv
from scipy.special import expit
from sklearn.feature_selection import RFE
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn import model_selection
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso
from sklearn.linear_model import ElasticNet
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import r2_score, explained_variance_score


#reader  = csv.reader(open("SF1_Uniform_5.csv"))
#reader1 = csv.reader(open("SF1_Uniform_25.csv"))
#next(reader1, None)
#f = open("SF1_Uniform_5_25.csv", "w", newline = '')
#writer = csv.writer(f)

#for row in reader:
    #writer.writerow(row)
#for row in reader1:
    #writer.writerow(row)
#f.close()


df = pd.read_csv('SF1_Uniform_5_25.csv')

df1 = df.loc[df['Model_Num']=='Model_RANGE_l_extendedprice']

X = df1.iloc[:,4:6]
Y = df1.iloc[:,6]

X_train, X_test, Y_train, Y_test = train_test_split (X, Y, test_size = 0.2, random_state = 42)

pipelines   = []
results     = []
names       = []

pipelines.append(('ScaledLR',       Pipeline([('Scaler', StandardScaler()),('LR',   LinearRegression())])))
pipelines.append(('ScaledLASSO',    Pipeline([('Scaler', StandardScaler()),('LASSO',Lasso())])))
pipelines.append(('ScaledEN',       Pipeline([('Scaler', StandardScaler()),('EN',   ElasticNet())])))
pipelines.append(('ScaledKNN',      Pipeline([('Scaler', StandardScaler()),('KNN',  KNeighborsRegressor())])))


for name, model in pipelines:
    kfold       = KFold(n_splits=10, random_state=21)
    cv_results  = cross_val_score(model, X_train, Y_train, cv=kfold, scoring='explained_variance')
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
    print(msg)


scaler      = StandardScaler().fit(X_train)
rescaledX   = scaler.transform(X_train)
params  =   {'n_neighbors':[5, 10, 15, 20, ]}
model       = KNeighborsRegressor()
kfold       = KFold(n_splits=10, random_state=21)
grid_1        = GridSearchCV(estimator=model, param_grid=params, scoring='explained_variance', cv=kfold)
grid_result_1 = grid_1.fit(rescaledX, Y_train)

means_1       = grid_result_1.cv_results_['mean_test_score']
stds_1        = grid_result_1.cv_results_['std_test_score']
params_1      = grid_result_1.cv_results_['params']

for mean_1, stdev_1, param_1 in zip(means_1, stds_1, params_1):
    print("%f (%f) with: %r" % (mean_1, stdev_1, param_1))

print("Best: %f using %s" % (grid_result_1.best_score_, grid_result_1.best_params_))


scaler              = StandardScaler().fit(X_train)
rescaled_X_train    = scaler.transform(X_train)
model               = KNeighborsRegressor()
model.fit(X_train, Y_train)

rescaled_X_test     = scaler.transform(X_test)
predictions         = model.predict(X_test)
print(explained_variance_score(Y_test, predictions))

compare = pd.DataFrame({'Prediction': predictions, 'Test Data' : Y_test})
print(compare)


for name, model in pipelines:
    kfold       = KFold(n_splits=10, random_state=21)
    cv_results  = cross_val_score(model, X_train, Y_train, cv=kfold, scoring='r2')
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
    print(msg)

params  =   {'n_neighbors':[5, 10, 15, 20, ]}
model       = KNeighborsRegressor()
grid_2        = GridSearchCV(estimator=model, param_grid=params, scoring='r2', cv=kfold)
grid_result_2 = grid_2.fit(rescaledX, Y_train)

means_2       = grid_result_2.cv_results_['mean_test_score']
stds_2        = grid_result_2.cv_results_['std_test_score']
params_2      = grid_result_2.cv_results_['params']

for mean_2, stdev_2, param_2 in zip(means_2, stds_2, params_2):
    print("%f (%f) with: %r" % (mean_2, stdev_2, param_2))

print("Best: %f using %s" % (grid_result_2.best_score_, grid_result_2.best_params_))


scaler              = StandardScaler().fit(X_train)
rescaled_X_train    = scaler.transform(X_train)
model               = KNeighborsRegressor()
model.fit(X_train, Y_train)

rescaled_X_test     = scaler.transform(X_test)
predictions         = model.predict(X_test)
print(r2_score(Y_test, predictions))

compare = pd.DataFrame({'Prediction': predictions, 'Test Data' : Y_test})
print(compare)
