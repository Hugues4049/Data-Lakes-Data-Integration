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

df.columns

# COMMAND ----------

stock_name = df["company_name"].unique().tolist()
print(stock_name)

# COMMAND ----------

stock_name = df["company_name"].unique().tolist()
my_col = ['Close_num', "Open_num","company_name"]
val1 = df[my_col]
print(val1)

# COMMAND ----------

val1['Score_diff'] = ((val1['Open_num'].sub(val1['Close_num'], axis = 0))/val1['Open_num'])*100
print(val1)

# COMMAND ----------

#Daily return rate : a function that get a stock name, a start and end date and output the daily return date of this stock during this period

def daily_rate(stock_name, date):
    for companies in stock_name:
        df1 = pd.DataFrame(date, columns = ['Close_num', "Open_num","company_name"])
        df1['daily_return'] = ((df1['Open_num'].sub(df1['Close_num'], axis = 0))/df1['Open_num'])*100
        df1.dropna(inplace=True)
    return df1

# COMMAND ----------

#test of the function
stock_name = df["company_name"].unique().tolist()
my_col = ['Close_num', "Open_num","company_name"]
date = df[my_col]
final1 = daily_rate(stock_name, date)
print(final1)

# COMMAND ----------

#Moving average : a function that takes a stock name, a start and end data, and a number of moving points (5 points for example) and return a new dataframe with the applied moving average over the opening price column
def Moving_average(stock_name, date_close, mov_point):
    for companies in stock_name:
        df1 = pd.DataFrame(date_close, columns = ['Close_num',"company_name"])
        df1['SMA5'] = df1['Close_num'].rolling(mov_point).mean()
        df1.dropna(inplace=True)
    return df1

# COMMAND ----------

#test of the function

stock_name = df["company_name"].unique().tolist()
my_col = ['Close_num',"company_name"]
date_close = df[my_col]
mov_point = 5
final = Moving_average(stock_name, date_close, mov_point)
print(final)

# COMMAND ----------

output_df = pd.DataFrame(final)
#output_df.to_csv('mouv.csv')
print(output_df)

# COMMAND ----------

output_df2 = pd.DataFrame(final1)
#output_df.to_csv('mouv.csv')
print(output_df2)

# COMMAND ----------

# Load libraries
from azure.storage.blob import BlobClient
import pandas as pd

# Define parameters
connectionString = "DefaultEndpointsProtocol=https;AccountName=batchstorage4049;AccountKey=mgaTFbGRO+ZZ2xu9GDJ+MXVdbbEwpFHrAOJkmVzwMKU9W7yURF8I4Pa9gC28zN9uywNGyR0f0Ld/+AStDVK99g==;EndpointSuffix=core.windows.net"
containerName = "smavoutput"
outputBlobName1	= "moving_average.csv"
outputBlobName2 = "daily_return.csv"

# Establish connection with the blob storage account
blob1 = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName1)
blob2 = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName2)
# Load iris dataset from the task node
df1 = output_df
df2 = output_df2
# Save the subset of the iris dataframe locally in task node
df1.to_csv(outputBlobName1, index = False)
df2.to_csv(outputBlobName2, index = False)

if blob1.exists():
    print("moving_average.csv already exists in the container")
else:
    print("moving_average.csv does not exist in the container")
    #adding the file to the result container
    with open(outputBlobName1, "rb") as data:
    	blob1.upload_blob(data)
    	print("moving_average.csv uploaded to the container")
        
if blob2.exists():
    print("daily_return.csv already exists in the container")
else:
    print("daily_return.csv does not exist in the container")
    #adding the file to the result container
    with open(outputBlobName1, "rb") as data:
    	blob2.upload_blob(data)
    	print("daily_return.csv uploaded to the container")

# COMMAND ----------

##Data from yahoo finance
from yahoo_fin.stock_info import get_data

# COMMAND ----------

ticker_list = ["amzn", "aapl", "ba"]
historical_datas = {}
for ticker in ticker_list:
    historical_datas[ticker] = get_data(ticker)

# COMMAND ----------

print(historical_datas)

# COMMAND ----------

type(historical_datas)

# COMMAND ----------

historical_datas.keys()

# COMMAND ----------

T1 = historical_datas["aapl"]
T2 = historical_datas['amzn']
T3 = historical_datas['ba']

# COMMAND ----------

yahoo_df = pd.concat([T1, T2, T3])
yahoo_df

# COMMAND ----------


