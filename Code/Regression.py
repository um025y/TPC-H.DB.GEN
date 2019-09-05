import numpy as np
import pandas as pd
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
from sklearn.tree import DecisionTreeRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score

df = pd.read_csv('SF1_Zipf_5.csv')

df1 = df.loc[df['Model_Num']=='Model_RANGE_o_orderkey']

X = df1.iloc[:,4:6]
Y = df1.iloc[:,11]

X_train, X_test, Y_train, Y_test = train_test_split (X, Y, test_size = 0.2, random_state = 42)

pipelines   = []
results     = []
names       = []

pipelines.append(('ScaledLR',       Pipeline([('Scaler', StandardScaler()),('LR',   LinearRegression())])))
pipelines.append(('ScaledLASSO',    Pipeline([('Scaler', StandardScaler()),('LASSO',Lasso())])))
pipelines.append(('ScaledEN',       Pipeline([('Scaler', StandardScaler()),('EN',   ElasticNet())])))
pipelines.append(('ScaledKNN',      Pipeline([('Scaler', StandardScaler()),('KNN',  KNeighborsRegressor())])))
pipelines.append(('ScaledCART',     Pipeline([('Scaler', StandardScaler()),('CART', DecisionTreeRegressor())])))
pipelines.append(('ScaledGBM',      Pipeline([('Scaler', StandardScaler()),('GBM',  GradientBoostingRegressor())])))

for name, model in pipelines:
    kfold       = KFold(n_splits=10, random_state=21)
    cv_results  = cross_val_score(model, X_train, Y_train, cv=kfold, scoring='r2')
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
    print(msg)

scaler      = StandardScaler().fit(X_train)
rescaledX   = scaler.transform(X_train)
#param_grid  = dict(n_estimators=np.array([50,100,200,300,400]))
#model       = GradientBoostingRegressor(random_state=21)
params  = {'min_samples_split':[2, 3, 5, 7, 11]}
model       = DecisionTreeRegressor()
kfold       = KFold(n_splits=10, random_state=21)
grid        = GridSearchCV(estimator=model, param_grid=params, scoring='r2', cv=kfold)
grid_result = grid.fit(rescaledX, Y_train)

means       = grid_result.cv_results_['mean_test_score']
stds        = grid_result.cv_results_['std_test_score']
params      = grid_result.cv_results_['params']

for mean, stdev, param in zip(means, stds, params):
    print("%f (%f) with: %r" % (mean, stdev, param))

print("Best: %f using %s" % (grid_result.best_score_, grid_result.best_params_))


scaler              = StandardScaler().fit(X_train)
rescaled_X_train    = scaler.transform(X_train)
model               = DecisionTreeRegressor()
model.fit(X_train, Y_train)

rescaled_X_test     = scaler.transform(X_test)
predictions         = model.predict(X_test)
print(r2_score(Y_test, predictions))

compare = pd.DataFrame({'Prediction': predictions, 'Test Data' : Y_test})
print(compare)
