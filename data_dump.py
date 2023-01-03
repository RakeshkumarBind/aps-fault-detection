import pymongo
# We need pandas library as we have csv data format
import pandas as pd
# We need json as mongodb uses json format to store data
import json
# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")
# Create the variable to store the link of data upload Not in mongodb 
DATA_FILE_PATH="/config/workspace/aps_failure_training_set1.csv"
DATABASE_NAME="aps"
COLLECTION_NAME="sensor"

if __name__=="__main__":
    df= pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns : {df.shape}")
    

    #drop the unwanted indexes
    df.reset_index(drop=True , inplace=True)
    # To convert the dataframe into json file format to upload in mongodb server
    json_record=list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    # Making connection with the mongodb server and uploading the data in server
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)