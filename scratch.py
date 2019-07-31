import MySQLdb
import xlrd
worksheet = xlrd.open_workbook("C:\\Users\\Administrator\\Desktop\\ORDER.xlsx")

database = MySQLdb.connect (host="localhost", user="root", passwd="", db = "LINEITEM")

cursor = database.cursor()

query = """INSERT INTO orders (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK,
O_SHIPPRIORITY, O_COMMENT) VALUES (string, string, string, float, time, string, string, int, string)"""

for r in range(1, sheet.nrows):
    OrderKey = sheet.cell(r,0).value
    CustKey = sheet.cell(r,1).value
    OrderStatus = sheet.cell(r,2).value
    TotalPrice = sheet.cell(r,3).value
    OrderDate = sheet.cell(r,4).value
    OrderPriority = sheet.cell(r,5).value
    Clerk = sheet.cell(r,6).value
    ShipPriority = sheet.cell(r,7).value
    Comment = sheet.cell(r,8).value

    values = (OrderKey, CustKey, OrderStatus, TotalPrice, OrderDate, OrderPriority, Clerk, ShipPriority, Comment)

    cursor.execute(query, values)

cursor.close()

database.commit

database.close()
print ("")
print ("DONE!")
print ("")
columns = str(sheet.ncols)
rows = str(sheet.nrows)

print ("Complete!")

