import yfinance as yf
import pyodbc
import numpy as np
import time

# Database connection setup
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=iracing;Trusted_Connection=yes;Encrypt=no'

# Define the list of tickers
tickers = []
query = "SELECT Ticker FROM [iracing].[dbo].[instruments]"
    
with pyodbc.connect(connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute(query)
        
    # Fetch all tickers from the query result
    for row in cursor.fetchall():
        tickers.append(row[0])  # row[0] is the 'Ticker' column value
        time.sleep(1)

# Define the date range
start_date = '2019-12-31'  # Adjust as needed
end_date   = '2024-11-20'    # Adjust as needed

# Download data
data = yf.download(tickers, start=start_date, end=end_date)

# Forward-fill missing values and ensure clean data
data['Adj Close'] = data['Adj Close'].ffill().bfill()
data['Adj Close'] = data['Adj Close'].replace([np.inf, -np.inf], np.nan).fillna(0)  # Handle inf and remaining NaNs

# Process data and insert into the database
    # Initialize pricesID counter
query = "SELECT NULLIF(1,MAX(pricesID)+1) FROM [iracing].[dbo].[prices]"

with pyodbc.connect(connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    pricesID = result[0]  # Get the first column of the result

with pyodbc.connect(connection_string) as conn:
    cursor = conn.cursor()
    

    # Iterate over tickers and extract relevant data
    for ticker in tickers:
        if ticker not in data['Adj Close']:
            print(f"Skipping ticker {ticker}: No data found.")
            continue
        ticker_data = data['Adj Close'][ticker]

        # Prepare SQL insertion for each date
        for date, price in ticker_data.items():
            if np.isfinite(price):  # Ensure the price is a valid float
                sql_price = round(price, 5)

                cursor.execute("""
                    INSERT INTO [iracing].[dbo].[prices] (pricesID, ticker, price, date)
                    VALUES (?, ?, ?, ?)
                """, (pricesID, ticker, sql_price, date))

                pricesID += 1  # Increment pricesID

    # Commit the transaction
    conn.commit()

print("Data inserted successfully!")
