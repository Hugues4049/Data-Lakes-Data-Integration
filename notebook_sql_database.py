# Databricks notebook source
"""%sh
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17"""

# COMMAND ----------

import pandas as pd
import pyodbc
server = 'myserversql1.database.windows.net'
database = 'mydatabaseSQL'
username = 'tito4049'
password = 'TITOroland#4049'  
driver= '{ODBC Driver 17 for SQL Server}'

with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
        row = cursor.fetchone()
        while row:
            print (str(row[0]) + " " + str(row[1]))
            row = cursor.fetchone()


# COMMAND ----------



# COMMAND ----------

query = "SELECT * FROM dbo.stock_table1"


result=cursor.execute(query)
result=cursor.fetchall()



df = pd.DataFrame((tuple(t) for t in result), columns=['Date', 'High', 'Low', 'Open_num', 'Close_num', 'Volume', 'Adj_Close', 'company_name']) 
print(df)
#for x in result:
    #print(x)

#méthode DataFrame en utilisant la liste issue de .fetchall()
#import pandas as pd
#df = pd.DataFrame(res, columns=['Date', 'High', 'Low', 'Open_num', 'Close_num', 'Volume', 'Adj_Close', 'company_name'])
#print("En passant par une DataFrame \n", df.head())

# COMMAND ----------

#Daily return rate : a function that get a stock name, a start and end date and output the daily return date of this stock during this period

def daily_rate(stock_name, ):
    
    return print(tit)

# COMMAND ----------

#Moving average : a function that takes a stock name, a start and end data, and a number of moving points (5 points for example) and return a new dataframe with the applied moving average over the opening price column
def Moving_average(stock_name, date_close, mov_point):
    for companies in stock_name:
        df1 = pd.DataFrame(date_close, columns = ['Close_num',"company_name"])
        df1['SMA30'] = df1['Close_num'].rolling(mov_point).mean()
        df1.dropna(inplace=True)
    return print(df1)

# COMMAND ----------

#test of the function

stock_name = df["company_name"].unique().tolist()
my_col = ['Close_num',"company_name"]
date_close = df[my_col]
mov_point = 5
final = Moving_average(stock_name, date_close, mov_point)
print(final)

# COMMAND ----------


