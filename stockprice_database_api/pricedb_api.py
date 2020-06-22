# Importing data management packages:
import pandas as pd
from datetime import datetime
import numpy as np

# Importing Yahoo Finance data api:
import yfinance as yf

# Import database api pacakges:
import sqlite3


# Object that represents the connection in the sqlite3 database:
class price_db_api(object):
    """
    Much like the  pdf_db in the pdf database api this object represents the
    sqlite database.

    It contains all the methods necessary to read and write data to a sqlite
    database determined by a filepath.

    Parameters
    ----------
    db_path : str
        This is the string that represents the path to the sqlite database file.
        It is used to connect to the database via the sqlite3 package. If a path
        is specified and it does not lead to a database file then that file will
        be created at that filepath.
    """
    def __init__(self, db_path):

        # Creating the database connection:
        self.con = sqlite3.connect(db_path)

        # Creating a connection cursor to interact with the database:
        self.c = self.con.cursor()

        # Ensuring that the sqlite3 database supports foreign keys:
        self.c.execute("PRAGMA foreign_keys")

        # Enabling foreign key support if it is disabled:
        if self.c.fetchall()[0][0] == 0:

            self.c.execute("PRAGMA foreign_keys = ON")

        # Creating the main database Summary table if it does not exists:
        self.c.execute(
        """CREATE TABLE IF NOT EXISTS Summary (
            Ticker TEXT Primary Key UNIQUE,
            Last_updated TEXT)"""
            )

        self.con.commit()

# <---------------------------Database Writing Methods------------------------->

    # Method that ensures that a ticker table contains the most recent timeseries information:
    def update_ticker(self, ticker):
        '''
        This is the main method for writing pricing data from the Yahoo Finance
        API to the sqlite database. It does this by either creating a table and
        populating it if it does not exist or, if a ticker_timeseries does exist,
        It compares the most recent data written to the table and writes all
        additional data from the api to the database.

        See Docs for a more detailed description.

        Parameters
        ----------
        ticker : str
            This is the ticker symbol of the security being updated. It is used to
            call data from Yahoo Finance via the api.
        '''
        # Creating the tablename for the ticker:
        ticker_table = f'{ticker}_timeseries'

        # Creating the ticker table if it does not exist:
        self.c.execute(
            f"""CREATE TABLE IF NOT EXISTS {ticker_table} (
                Date TEXT Primary Key,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Volume INTEGER,
                Dividends REAL,
                Stock_Splits REAL,
                Historical_Volatility REAL,
                Annualized_Volatility REAL)""")

        # Writing table changes to database before performing table insertions:
        self.con.commit()

        # Initalizing the Yahoo Finance API Ticker method:
        ticker_obj = yf.Ticker(ticker)

        # Extracting all date values from the database table:
        date_lst = [
            # Using list comprehension to create list of datetime object
            datetime.strptime(date[0], '%Y-%m-%d').date() for date in
            self.c.execute(f"""SELECT Date FROM {ticker_table}""")
            ]

        # today = datetime.now().date()
        # Determining the most recent entry in the database:
        if len(date_lst) != 0: # If the table is not empty:

            most_recent =  max(date_lst)

            # Dropping that row from the table:
            self.c.execute(
                f"""DELETE FROM {ticker_table} WHERE Date=:most_recent""",
                {'most_recent': str(most_recent)})

            # Extracting price data starting from the most recent date:
            most_recent_df = ticker_obj.history(start = most_recent, end = datetime.now().date())

            # Resetting Index so that Datetime object can be converted to date() object:
            most_recent_df.reset_index(inplace=True)
            most_recent_df.Date = most_recent_df.Date.apply(lambda x : x.date())
            most_recent_df.rename(columns = {'Stock Splits': 'Stock_Splits'}, inplace= True)
            most_recent_df.set_index('Date', inplace=True)

            try: # NOTE: Current Error with algorithm to detect duplicate dates if weekend. Try-Catch patch.
                # Writing new slice of timeseries to database:
                most_recent_df.to_sql(ticker_table, con = self.con, if_exists = 'append')

                # Debug print:
                print(f'[WRITTEN]: {ticker}')

                # Updating/Writing data to the Summary Data table:
                self.c.execute(
                    """INSERT OR REPLACE INTO Summary (Ticker, Last_updated)
                    VALUES (:ticker, :Last_updated)""",
                    {'ticker':ticker, 'Last_updated': datetime.now().date()})

                # Writing changes to db:
                self.con.commit()

            except: # Noting the except catch has been triggered:

                raise Exception("""[DATE UPDATE ALGORITHM ERROR]: Conflict
between new and old datetime values. Is it an off trading day?""")


        # If the table was empty populate it with whole timeseries from api:
        else:

            # Creating dataframe of timeseries and formatting it for database:
            df = ticker_obj.history(period = 'max').rename(columns = {'Stock Splits': 'Stock_Splits'})
            df.reset_index(inplace=True)
            df.Date = df.Date.apply(lambda x : x.date())
            df.set_index('Date', inplace = True)

            # Writing dataframe to database:
            df.to_sql(ticker_table, con=self.con, if_exists = 'append')

            # Debug print:
            print(f'[WRITTEN]: {ticker}')

            # Updating/Writing data to the Summary Data table:
            self.c.execute(
                """INSERT OR REPLACE INTO Summary (Ticker, Last_updated)
                VALUES (:ticker, :Last_updated)""",
                {'ticker':ticker, 'Last_updated': datetime.now().date()})

            # Writing changes to db:
            self.con.commit()

    # Method that writes a list of ticker symbols to the database using the update_ticker():
    def update_tickers(self, ticker_lst):
        '''
        Method that ingests a list of ticker strings and iterates through the list
        and writes each individual ticker to the database via the update_ticker()
        method.

        ticker_lst : lst
            A list of ticker strings to be written/maintained to the database.
        '''
        # Iterating through the list of ticker strings:
        for ticker in ticker_lst:

            # Attempting to write each ticker string in the list:
            try:
                self.update_ticker(ticker)

            except:
                pass

# <---------------------------Database Reading Methods------------------------->

    # Method that is used to query a table from the database based on query strings:
    def get_table(self, table_name):
        '''
        Methods that uses the pandas package to extract a database table using the
        pd.read_sql_query() method based on an input table name string.

        Parameters
        ----------
        table_name : str
            A string that represents the name of the table in the database.

        Returns
        -------
        table_df : pandas dataframe
            The dataframe containing data that is pulled from the database table.
        '''
        # Attempting to extract data, if table is not found return None type:
        try:
            # Creating the dataframe:
            table_df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.con)

            return table_df

        except:
            return None

    # Method that is used to query the timeseries data of a specific ticker from the database:
    def get_ticker_table(self, ticker, start_date=None, end_date=None):
        '''
        Method that makes use of the pd.read_sql_query() to extract the timeseries
        data for a specific ticker symbol. It does this by building the name
        of the database table using the input ticker string: '{ticker}_timeseries'.

        If a start and end date are specified then the dataframe that is returned
        is sliced within the range of the start_date to end_date inclusive and
        that slice is returned.

        Parameters
        ----------
        ticker : str
            The string that represents the ticker symbol for which data is being
            queried. It is used to build the string in the form '{ticker}_timeseries'

        start_date : str
            A string representing the start date for the dataframe slice. The date
            must be in the form 'yyyy-mm-dd'. It is by default None.

        end_date : str
            A string representing the end date for the dataframe slice. The date
            must be in the form 'yyyy-mm-dd'. It is by default None.

        Returns
        -------
        ticker_df : pandas dataframe
            This is the dataframe that is retrieved from the pd.read_sql_query().
        '''
        # Building the ticker table name:
        table_name = f'{ticker}_timeseries'

        # Extracting the dataframe from the database:
        ticker_df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.con)

        # Formatting the ticker dataframe:
        ticker_df.Date = ticker_df.Date.apply(
            lambda x : datetime.strptime(x, '%Y-%m-%d').date())

        ticker_df.set_index('Date', inplace=True)

        # Conditional that handles the logic for slicing the dataframe based on
        # start and end date:
        if start_date != None and end_date == None: # If only a start date is provided:

            # Converting the start_date to a datetime object:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

            # Extracting the location of the start_date value from dataframe index:
            start = ticker_df.index.searchsorted(start_date)

            # Slicing the dataframe based on the start value:
            ticker_df = ticker_df.iloc[start:]

            return ticker_df


        elif start_date == None and end_date != None: # If only an end date is provided:

            # Converting the end date string to a datetime object:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

            # Extracting the location of the end_date value from datetime index:
            end = ticker_df.index.searchsorted(end_date)

            # Slicing the dataframe based on the end index value:
            ticker_df = ticker_df.iloc[:end]

            return ticker_df

        elif start_date != None and end_date != None: # If both start and end data are provided:

            # Converting both start and end date to datetime object:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

            # Extracting the start and end location in the dataframe:
            start = ticker_df.index.searchsorted(start_date)
            end = ticker_df.index.searchsorted(end_date)

            # Slicing the dataframe based on the start and end values:
            ticker_df = ticker_df.iloc[start:end]

            return ticker_df

        return ticker_df

# <--------------------------"Helper" Method----------------------------------->

    # Method that ingests a dataframe of price history and calculates Historical Volatility:
    def add_timeseries_technicals(self, dataframe):
        '''
        Method ingests a standard dataframe from the {ticker}_timeseries table in
        the database in the format: Date, Open, High, Low, Close, Dividends, Stock_Splits
        (OHLC).

        It then calculates technical/statistical indicators and inserts them into
        the standard dataframe. The technical indicators that are added to the
        dataframe are as follows:

        - 1-day Historical Volatility using Closing prices.
        - Annualized Historical Volatility
        - 12-Period EMA
        - 26-Period EMA
        - MACD Value
        - RSI
        '''
        # TODO: Write Method.
        # # TODO: Update README Documentation when add_timeseries_technicals is complete.

# Test:
test = price_db_api("/home/matthew/Documents/test_pdf_databases/stockprice_test.db")
test.update_ticker("XOM")
print(test.get_ticker_table('XOM'))
