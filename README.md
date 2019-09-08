**********************************************ReadMe File****************************************************
1.There are four python scripts that were developed in the completion of this project - main.py, sql_query_gen.py , utils.py, regression.py
2. The sql_query_gen.py is an automated SQL query generator, that models the SQL query and executes them on MySQL database taking into consideration the user inputs for distribution type, query counts, query iterations, selectivity value, random seeds, zipf distribution parameter, scale factor of the datset, output names of file(to store the output results) when run from the main file.
3. The database hostname, database name prefix, database user ID and password are all specified in sql_query_gen.py
4. The utils.py contains all the check functions to validate the user inputs of the above mentioned variables. If the inputs given by the user do not match the expected values of these variables, error will be thrown. 
5. The main.py is the main program which starts the working of both - sql_query_gen.py and utils.py. The following are the inputs that the user can pass through -D for distribution type, -A for setting the zipf distribution parameter, -C for number of query instances per query type to be executed, -I number of iterations for the same query instance, -S the value of selectivity, -F for the scale factor of the dataset, -O for specify the name of the output file. -R for setting the random seeds. 
6. The main.py calls both the scripts sql_query_gen.py and utils.py for its execution. 
main.py can be executed on both the command line as well as any python IDLE. But make sure all the files are in the same directory. 
7. The output results will be saved by the name of the file specified by the user in the command line ('-O') or by default it is saved as test.csv. In my case for a zipf distribution, scale factor 1 with 5% selectivity, the output file was named as 'SF1_Zipf_5.csv'.
8. While running the main.py file several intermediary files would be generated, which stores the list of values and permutations for each domain, so that during the next runtime those values do not have to be computed again. 
9. The output file retrieved on running the main.py file are used as input file for the regression.py file. The different outputs retrieved for different selectivity values but for the same type of distribution can be combined for the regression algorithm to work. 
10. The algorithm works on each query model to predict results on the Returned Result Set or Average execution time. To predict for Returned Result Set or Average Execution time, the Y parameter has to be changed - for Average Execution Time prediction set 'Y = df1.iloc[:,11]', for Returned Result Set Prediction set 'Y = df1.iloc[:,6]'.
11. There are 8 query models, the script needs to be specified on which model the prediction is to be performed by changing the values in 'df1 = df.loc[df['Model_Num']=='Model_RANGE_l_extendedprice']'. The acceptable values are - Model_RANGE_o_orderkey,Model_JOIN_o_totalprice, Model_RANGE_o_totalprice, Model_RANGE_l_extendedprice, Model_JOIN_l_extendedprice, Model_JOIN_l_orderkey, Model_JOIN_o_orderkey and Model_RANGE_l_orderkey.
12. The comparative results of actual and predicted values will be printed on the console. Also the performance of the regression model will be printed in terms of the metric scores for R2 and Explained Variance. These metrics give an overview of how the model performs on the dataset.


Important Notes- 
a)During my study, all the query models did not have equal no. of query instances due to the volume of the data on which it had to be computed from. 
b) I have commented out the lines for merging the selectivity values for each distribution type, as I had already merged them once.
c) Appropriate packages must be imported to run the scripts. I had imported the packages which are compatible with Python 3.7.
d) I have compiled a sample of the regression model's prediction output to the actual output which appeared on the console.
e) For regression of my model I had taken reference from the following site : https://www.kaggle.com/junkal/selecting-the-best-regression-model. 
