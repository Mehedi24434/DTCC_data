# DTCC_data  
https://pddata.dtcc.com/gtr/cftc/dashboard.do

## Extracting data from MongoDB:  
### Using function
clone the repo:  
```bash
git clone https://github.com/anchorblock/DTCC_data.git  
```
then in the repo run the following code:
```python
import function as f

database_name='DTCC'
collection_name = 'forex'   #set the asset names (commodities, equities, rates, credits)

f.get_dataframe_from_mongodb(database_name,collection_name,target_date='2023-08-06')
```
it will return a pandas dataframe for the specific dates of the file stored at DTCC. if you want for all dates, keep the target_date argument False  

### Not using the repository function:  
use code like bellow:
```python
import os
import zipfile
import pandas as pd
from pymongo import MongoClient
import os
from datetime import datetime

mongo_host = '3.109.41.82'
mongo_port = 27017
mongo_username = 'admin'
mongo_password = '123456789'

# Create a MongoDB client with authentication
client = MongoClient(
    host=mongo_host,
    port=mongo_port,
    username=mongo_username,
    password=mongo_password
)

database_name='DTCC'
collection_name = 'forex'   #set the asset names (commodities, equities, rates, credits)
target_date = '2023-08-06'  #set the desired date

# Access the specified database
db = client[database_name]

# Access the specified collection
collection = db[collection_name]

# Convert the target date to a datetime object if provided
if target_date:
    target_date = datetime.strptime(target_date, '%Y-%m-%d')

# Query to filter documents by the target date (if provided)
query = {}
if target_date:
    query['date'] = target_date

# Retrieve documents from the collection based on the query
cursor = collection.find(query)

# Convert the cursor to a list of dictionaries
records = list(cursor)

# Create a DataFrame from the list of dictionaries
dataframe = pd.DataFrame(records)
print(dataframe)
```
