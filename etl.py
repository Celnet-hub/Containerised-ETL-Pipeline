### ETL Pipeline Script ######
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3

log_file = "log_file.txt"
final_result = "transformed_internet_users_data.csv"
db_name = "customers.db"
table_name = "internet_users"

# Get columns
columns = pd.read_csv('./internet_users.csv')
print(columns.columns)

###### Extraction ########


def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe


def extract():
    # create an empty dataframe to hold extracted data
    extracted_data = pd.DataFrame(columns=['Location', 'Rate (WB)', 'Year', 'Rate (ITU)', 'Year.1', 'Users (CIA)',
       'Year.2', 'Notes'])

    # begin processing files
    for csvfile in glob.glob("*.csv"):
        if csvfile != final_result:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(
                extract_from_csv(csvfile))], ignore_index=True)

    return extracted_data


############### Transformation ######################

def transform(data):
    # data.columns = data.columns.str.lower()

    # round year to whole number
    year_list = data["Year"].tolist()
    year_one_list = data["Year.1"].tolist()
    year_two_list = data["Year.2"].tolist()


    transformed_year = [np.floor(x) for x in year_list]
    transformed_year_one = [np.floor(x) for x in year_one_list]
    transformed_year_two = [np.floor(x) for x in year_two_list]


    data["Year"] = transformed_year
    data["Year.1"] = transformed_year_one
    data["Year.2"] = transformed_year_two

    data.index.name = "S/N"
    return data


############### Load to CSV and to SQL #################

def load_data(destination, transformed_data):
    transformed_data.to_csv(destination)

    #Load to SQL
    conn = sqlite3.connect(db_name)
    transformed_data.to_sql(table_name, conn, if_exists='replace', index=False) #index= Fasle prevents pandas from writing the dataframe index as column in the database's table
    conn.close()
    print("Loaded to sqlite table")


def log_progress(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as logs:
        logs.write(timestamp + "," + msg + "\n")




######## Begin ETL ###########
log_progress("Beginning ETL process")
extracted_data = extract()

log_progress("Extract completed \n")
log_progress("Beginning transformation \n")

transformed_data = transform(extracted_data)


log_progress("Transformation completed \n")
log_progress("Beginning Load \n")

load_data(final_result, transformed_data)

log_progress("Data loaded to csv file")

