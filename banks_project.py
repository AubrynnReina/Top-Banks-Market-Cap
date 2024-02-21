# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp_str_format = '%Y:%h:%d %H:%M:%S'
    now = datetime.now()
    time_stamp = now.strftime(time_stamp_str_format)

    with open(log_file, 'a') as f:
        f.write(f'{time_stamp} : {message}\n')
    

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('table')
    rows = tables[0].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) == 0:
            continue
        else:
            bank_name = cols[1].contents[2].contents[0]
            market_cap = float(cols[2].contents[0][:-1])
            
            temp_df = pd.DataFrame(data=[[bank_name, market_cap]], columns=table_attribs)
            df = pd.concat([df, temp_df], ignore_index=True)
    
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    for key in exchange_rate_dict.keys():
        df[f'MC_{key}_Billion'] = (df['MC_USD_Billion'] * exchange_rate_dict[key])\
                                  .apply(lambda x: round(x, 2))

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    output = pd.read_sql_query(query_statement, sql_connection)
    print(output)
    
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_file = 'code_log.txt'
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

query_1 = 'SELECT * FROM Largest_banks'
query_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_3 = 'SELECT Name from Largest_banks LIMIT 5'

log_progress('Preliminaries complete. Initiating ETL process')

banks_df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

transformed_banks_df = transform(banks_df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_banks_df, output_path)
log_progress('Data saved to CSV file')

connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(transformed_banks_df, connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

run_query(query_1, connection)
run_query(query_2, connection)
run_query(query_3, connection)
log_progress('Process Complete')

connection.close()
log_progress('Server Connection closed')