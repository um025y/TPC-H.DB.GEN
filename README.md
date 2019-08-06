# TPC-H.DB.GEN
Creating a Random Query Generator to measure query processing time 
read_data_order_table.py and read_data.py files are used to import my CSV files from db gen and import it to already created Database tables in MySQL.
scratch.py file is used in getting the range limits from specified columns which are used for query generation.
query_gen.py is the first hand tried script for random query generation, it is more like a first script, which was not appropriate.
query_gen2.py is the second hand improvements from query_gen.py, however it was to just improve the random generators which were not properly implemented in the previous script.
test1.py and Test2.py are the more improved versions, where Test2.py is the latest version, and working as per new implementations. There are still a few changes required that need to be pulled in these versions.
test1.py file is almost the final version of the query generator. 
Uploaded all my CSV result files. There are 12 different query result .csv files and one .csv file which does all the time statistics calculations. 
