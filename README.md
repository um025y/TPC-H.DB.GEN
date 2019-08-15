# TPC-H.DB.GEN
Creating a Random Query Generator to measure query processing time 
read_data_order_table.py and read_data.py files are used to import my CSV files from db gen and import it to already created Database tables in MySQL.
scratch.py file is used in getting the range limits from specified columns which are used for query generation.
query_gen.py is the first hand tried script for random query generation, it is more like a first script, which was not appropriate.
query_gen2.py is the second hand improvements from query_gen.py, however it was to just improve the random generators which were not properly implemented in the previous script.
test1.py and Test2.py are the more improved versions, where Test2.py is the latest version, and working as per new implementations. There are still a few changes required that need to be pulled in these versions.
test1.py file is almost the final version of the query generator. 
Uploaded all my CSV result files. There are 12 different query result .csv files and one .csv file which does all the time statistics calculations. 
I have renamed 'test1.py' to 'Random_Uniform.py', and 'Test2.py' to 'Random_Zipf.py'.
'Random_Zipf.py' introduces Zipf Distribution to generate random distribution results to be used as min and max range of constraints for my queries.
'Random_Gen_2.py' and 'Random_Zipf_2.py' are the 2 scripts which are modified versions of 'Random_Uniform.py' and 'Random_Zipf.py' and their .csv result files are stored as 'Random_Gen_1_Results.csv' and 'Random_Zipf_Results.csv'. 
'Random_Gen_Zipf_Test.py' is the third and hopefully the final version of result generation script. I have uploaded the uniform random generation results 'Random_Gen_1_1_Results.csv'. Currently my program is generating the Zipf Distribution results for the same. 
