import pandas as pd
from sqlalchemy import create_engine


column_name=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']


df = pd.read_csv('D:\\UofG\\lineitem.csv', delimiter='|', header=None, names=column_name, index_col=False)
#head_df=df.head()
print(df)


engine = create_engine('mysql://root:050194.Piku@localhost/lineitem')
with engine.connect() as conn, conn.begin():
    df.to_sql('lineitem_table', conn, if_exists='append', index=False)

print ("COMPLETE")