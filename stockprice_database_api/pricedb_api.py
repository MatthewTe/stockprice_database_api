# Importing data management packages:
import pandas as pd
from datetime import datetime

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
                Stock_Splits REAL)""")

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

            # Writing new slice of timeseries to database:
            most_recent_df.to_sql(ticker_table, con = self.con, if_exists = 'append')

            # Updating/Writing data to the Summary Data table:
            self.c.execute(
                """INSERT OR REPLACE INTO Summary (Ticker, Last_updated)
                VALUES (:ticker, :Last_updated)""",
                {'ticker':ticker, 'Last_updated': datetime.now().date()})

            # Writing changes to db:
            self.con.commit()

        # If the table was empty populate it with whole timeseries from api:
        else:

            # Creating dataframe of timeseries and formatting it for database:
            df = ticker_obj.history(period = 'max').rename(columns = {'Stock Splits': 'Stock_Splits'})
            df.reset_index(inplace=True)
            df.Date = df.Date.apply(lambda x : x.date())
            df.set_index('Date', inplace = True)

            df.to_sql(ticker_table, con=self.con, if_exists = 'append')

            # Updating/Writing data to the Summary Data table:
            self.c.execute(
                """INSERT OR REPLACE INTO Summary (Ticker, Last_updated)
                VALUES (:ticker, :Last_updated)""",
                {'ticker':ticker, 'Last_updated': datetime.now().date()})

            # Writing changes to db:
            self.con.commit()


'''
'/home/matthew/Documents/test_pdf_databases/stockprice_test.db'
test = price_db_api('/home/matthew/Documents/test_pdf_databases/stockprice_test.db')
test.update_ticker('XOM')
test.update_ticker('TSLA')
test.update_ticker('FSLR')
test.update_ticker('ICLN')'''
