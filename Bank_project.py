import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import datetime
import numpy as np


url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=['Name', 'MC_USD_Billion']
db_name='Bank.db'
table_name='Largest_banks'
csv_path='Largest_bank_data.csv'
exchange_rate='exchange_rate.csv'

def log_progress(message):
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open('code_log.txt','a')as f:
        f.write(timestamp+':'+message+'\n')
    
def extract(url, table_attribs):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table_body = soup.find('tbody')
    names = []
    market_caps = []

    for row in table_body.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) >= 2:  # Adjusted column index (to skip first column and find names + market caps)
            name = cols[1].get_text(strip=True)  # Get the text of the name from the second column
            market_cap = cols[2].get_text(strip=True)  # Get the market cap from the fourth column
            names.append(name)
            market_caps.append(market_cap)
    df = pd.DataFrame({
        'Names': names,
        'MC_USD_Billion': market_caps
    })
    return df
    
def transform(df, exchange_rate_path):
    
    exchange_rate_df = pd.read_csv(exchange_rate_path)
    
    '''print("Exchange rate CSV columns:", exchange_rate_df.columns)
    print("Exchange rate CSV head:", exchange_rate_df.head())'''
    
    # Step 2: Convert the exchange rate CSV to a dictionary
    try:
        # Ensure the correct column names in your exchange rate CSV
        exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Exchange Rate']
    except KeyError:
        print("Error: 'Currency' or 'Exchange Rate' column not found in the CSV file.")
        return df  
        
    # Clean the 'MC_USD_Billion' column: Remove commas and convert to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(str(x).replace(',', '').strip()))

    # Add the new columns, scaling the MC_USD_Billion by the exchange rate and rounding to 2 decimal places
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate.get('GBP', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate.get('EUR', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate.get('INR', 1), 2) for x in df['MC_USD_Billion']]

    #print(df['MC_EUR_Billion'][4])
    return df


def load_to_csv(df,csv_path):
    df.index=df.index+1
    df.to_csv(csv_path)
    
def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)
    
def run_query(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,exchange_rate)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Names FROM Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
    