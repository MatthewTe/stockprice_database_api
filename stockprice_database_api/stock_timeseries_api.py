# Importing data management packages:
import pandas as pd
from datetime import datetime
import numpy as np

# Importing Yahoo Finance data api:
import yfinance as yf

# Import database api pacakges:
import sqlite3


# Object that represents the connection in the sqlite3 database:
class stock_timeseries_api(object):
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
        API to the sqlite database.

        It does this by either creating a table and populating it if it does not
        exist or, if a ticker_timeseries does exist, It replaces the data in the
        table with the most recent pricing data. It also calculates the timeseries
        technical data associated with the price dataframe and writes that technical
        dataframe to the ticker_technicals table while updating the Summary database
        ticker,

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

        # Creating dataframe of timeseries and formatting it for database:
        df = ticker_obj.history(period = 'max').rename(columns = {'Stock Splits': 'Stock_Splits'})
        df.reset_index(inplace=True)
        df.Date = df.Date.apply(lambda x : x.date())
        df.set_index('Date', inplace = True)

        # Writing dataframe to database:
        df.to_sql(ticker_table, con=self.con, if_exists = 'replace')

        # Debug print:
        print(f'[WRITTEN]: {ticker} Timeseries')

        # Executing the update_timeseries_technicals() method to write technical
        # data to the database:
        self.update_timeseries_technicals(ticker)

        # Updating/Writing data to the Summary Data table:
        self.c.execute(
            """INSERT OR REPLACE INTO Summary (Ticker, Last_updated)
            VALUES (:ticker, :Last_updated)""",
            {'ticker':ticker, 'Last_updated': datetime.now().date()})

        # Writing changes to db:
        self.con.commit()

    # Method that writes the technical indicators from a ticker timeseries to the database:
    def update_timeseries_technicals(self, ticker):
        '''
        Method queries the database for the price timeseries data based on input
        ticker. It uses this historical price data to calculate various technical
        indicators.

        These technical indicators are written into a database table in the format
        {ticker}_technicals as a timeseries. The following technical indicators
        are writtn to the database table:

        - 1-Month Annualized Historical Volatility
        - 3-Months Annualized Historical Volatility
        - 12-Day SMA & EMA
        - 26-Day SMA & EMA
        - 50-Day SMA & EMA
        - 200-Day SMA & EMA
        - MACD Value
        - RSI

        Parameters
        ----------
        ticker : str
            This is the string that represents the ticker symbol of the security
            in the database. In this method the ticker string is used to query
            the price timeseries and write the technicals database table.
        '''
        # Creating the name of the ticker technicals table:
        table_name = f"{ticker}_technicals"

        # Creating database table for the ticker technicals:
        self.c.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} (
                Date TEXT Primary Key,
                Close_Price REAL,
                One_M_Volatility REAL,
                Three_M_Volatility REAL,
                Twelve_SMA REAL,
                Twenty_Six_SMA REAL,
                Fifty_SMA REAL,
                Two_Hundred_SMA REAL,
                Twelve_EMA REAL,
                Twenty_Six_EMA REAL,
                Fifty_EMA REAL,
                Two_Hundred_EMA REAL,
                MACD REAL,
                RSI REAL)""")

        # Attempting to extract a dataframe of historical price data for ticker:
        try:
            price_df = pd.read_sql_query(f"SELECT * FROM {ticker}_timeseries", con = self.con)
            price_df.set_index('Date', inplace=True)

        except: # If the price timeseries does not exist halt the process.

            raise Exception('[ERROR]: Attempting to write technicals with no price data to database')

        # Creating the dataframe of technical indicators to be populated:
        technical_df = pd.DataFrame(
            columns = [
                'Close_Price', 'One_M_Volatility', 'Three_M_Volatility', 'Twelve_SMA',
                'Twenty_Six_SMA', 'Fifty_SMA', 'Two_Hundred_SMA', 'Twelve_EMA', 'Twenty_Six_EMA',
                'Fifty_EMA', 'Two_Hundred_EMA', 'MACD', 'RSI'
                    ],
            index = price_df.index
            )

        # Inserting the price_df Close column into the technicals dataframe:
        technical_df.Close_Price = price_df.Close

        # Calculating and inserting Annualized Volatility data:
        technical_df.One_M_Volatility = technical_df.Close_Price.pct_change().rolling(21).std()*(252**0.5) # 1-Month
        technical_df.Three_M_Volatility = technical_df.Close_Price.pct_change().rolling(63).std()*(252**0.5) # 3-Month

        # Calculating and inserting the Simple Moving Averages:
        technical_df.Twelve_SMA = technical_df.Close_Price.rolling(12).mean() # 12-Day
        technical_df.Twenty_Six_SMA = technical_df.Close_Price.rolling(26).mean() # 26-Day
        technical_df.Fifty_SMA = technical_df.Close_Price.rolling(50).mean() # 50-Day
        technical_df.Two_Hundred_SMA = technical_df.Close_Price.rolling(200).mean() # 200-Day

        # Calculating and inserting the Exponential Moving Averages:
        technical_df.Twelve_EMA = technical_df.Close_Price.ewm(span=12).mean() # 12-Day
        technical_df.Twenty_Six_EMA = technical_df.Close_Price.ewm(span=24).mean() # 24-Day
        technical_df.Fifty_EMA = technical_df.Close_Price.ewm(span=50).mean() # 50-Day
        technical_df.Two_Hundred_EMA = technical_df.Close_Price.ewm(span=200).mean() # 200-Day

        # Calculating and inserting the MACD value:
        technical_df.MACD = (technical_df.Twelve_EMA - technical_df.Twenty_Six_EMA)

        # Calculating and inserting RSI Value with period of 14 days:
        technical_df.RSI = stock_timeseries_api.calc_rsi(technical_df.Close_Price, 14)

        # Writing the technicals dataframe to the database:
        technical_df.to_sql(table_name, con=self.con, if_exists='replace')

        # Debug Print:
        print(f'[WRITTEN]: {ticker} Technicals\n')

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

    # Method that contains all the logic for updating tickers based on Summary table in database:
    def maintain_db(self):
        '''
        This method is designed to 'maintain'/update an existing database by
        extracting the list of ticker's stored in the Summary table and calling
        the update_tickers() method to update each ticker's data in the database.
        '''
        # Creating the list of ticker strings from Summary table via list comprehension:
        ticker_lst = [ticker[0] for ticker in self.c.execute('SELECT Ticker FROM Summary')]

        # Calling the update_tickers() method to update all tickers in the list:
        self.update_tickers(ticker_lst)

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
    def get_ticker_data(self, ticker, start_date=None, end_date=None):
        '''
        Method that makes use of the pd.read_sql_query() to extract the price timeseries
        and the timeseries technical indicator data for a specific ticker symbol. It
        formats these two dataframes and then returns a dictionary containing both
        these dataframes.

        It does this by building the name of the database table using the input
        ticker string: '{ticker}_timeseries' and {ticker}_technicals.

        If a start and end date are specified then the dataframes that are returned
        are sliced within the range of the start_date to end_date inclusive and
        that slice is returned in the dictionary.

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
        ticker_dict : dictionary
            This is the dictionary that contains both pandas dataframes in the
            key-value format of:

            {'price': {ticker}_timeseries_df, 'technicals': {ticker}_technicals_df}
        '''
        # Building the ticker table names:
        timeseries_table_name = f'{ticker}_timeseries'
        technicals_table_name = f'{ticker}_technicals'

        # Extracting the dataframe sfrom the database:
        ticker_timeseries_df = pd.read_sql_query(f"SELECT * FROM {timeseries_table_name}", self.con)
        ticker_technicals_df = pd.read_sql_query(f"SELECT * FROM {technicals_table_name}", self.con)

        # Formatting the dataframes:
        ticker_timeseries_df.Date = ticker_timeseries_df.Date.apply(
            lambda x : datetime.strptime(x, '%Y-%m-%d').date())
        ticker_timeseries_df.set_index('Date', inplace=True)

        ticker_technicals_df.Date = ticker_technicals_df.Date.apply(
            lambda x : datetime.strptime(x, '%Y-%m-%d').date())
        ticker_technicals_df.set_index('Date', inplace=True)

        # Conditional that handles the logic for slicing the dataframes based on
        # start and end date:
        if start_date != None and end_date == None: # If only a start date is provided:

            # Converting the start_date to a datetime object:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

            # Extracting the location of the start_date value from dataframe index:
            timeseries_start = ticker_timeseries_df.index.searchsorted(start_date)
            technicals_start = ticker_technicals_df.index.searchsorted(start_date)

            # Slicing the dataframes based on the start values:
            ticker_timeseries_df = ticker_timeseries_df.iloc[timeseries_start:]
            ticker_technicals_df = ticker_technicals_df.iloc[technicals_start:]


        elif start_date == None and end_date != None: # If only an end date is provided:

            # Converting the end date string to a datetime object:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

            # Extracting the locations of the end_date value from datetime index:
            timeseries_end = ticker_timeseries_df.index.searchsorted(end_date)
            technicals_end = ticker_technicals_df.index.searchsorted(end_date)

            # Slicing the dataframes based on the end index value:
            ticker_timeseries_df = ticker_timeseries_df.iloc[:timeseries_end]
            ticker_technicals_df = ticker_technicals_df.iloc[:technicals_end]

        elif start_date != None and end_date != None: # If both start and end data are provided:

            # Converting both start and end date to datetime object:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

            # Extracting the start and end location in the dataframes:
            timeseries_start = ticker_timeseries_df.index.searchsorted(start_date)
            timeseries_end = ticker_timeseries_df.index.searchsorted(end_date)

            technicals_start = ticker_technicals_df.index.searchsorted(start_date)
            technicals_end = ticker_technicals_df.index.searchsorted(end_date)

            # Slicing the dataframes based on the start and end values:
            ticker_timeseries_df = ticker_timeseries_df.iloc[timeseries_start:timeseries_end]
            ticker_technicals_df = ticker_technicals_df.iloc[technicals_start:technicals_end]

        # Populating the dictionary:
        ticker_dict = {'price':ticker_timeseries_df, 'technicals':ticker_technicals_df}

        return ticker_dict

# <--------------------------"Helper" Method----------------------------------->

    # Method that calculates the RSI technical values from a dataframe column:
    def calc_rsi(data_series, period):
        '''
        This method ingests a column or series of data (intended to be a price
        timeseries) and calculates and returns a series of RSI values associated
        with the input data.

        This algorithm for calculating RSI is solely based on the process outlined
        from Michal Vasulka's article: https://tcoil.info/compute-rsi-for-stocks-with-python-relative-strength-index/

        Parameters
        ----------
        data_series : pandas dataframe column / series
            data is a single column of a pandas dataframe (a pandas series). This
            is the data that is used to calculate the RSI values.

        Returns
        -------
        rsi_series : pandas series
            A series of RSI values that are derived from the input data series.
        '''
        # Creating a series of values that is the difference between each element
        # and its previous element:
        price_difference = data_series.diff(1)

        # Creating series of value 0 with same dimensions as tbe price_difference series:
        up_change = 0 * price_difference
        down_change = 0 * price_difference

        # Adding values to up_change series where price difference is equal to positive difference:
        up_change[price_difference > 0] = price_difference[price_difference > 0]

        # Adding values to down_change series where price difference is equal to negativce difference:
        down_change[price_difference < 0] = price_difference[price_difference < 0]

        # Calculating the ewm of up and down change series with alpah = 1/period
        # for number of periods specified:
        up_change_avg = up_change.ewm(com = period-1, min_periods = period).mean()
        down_change_avg = down_change.ewm(com = period-1, min_periods = period).mean()

        # Converting the result (up_change_avg/down_change_avg) to absoloute value:
        rs_series = abs(up_change_avg/down_change_avg)

        # Re-scalling each value in series to the RSI scale of 0 - 100:
        rsi_series = 100 - 100/(1+rs_series)

        return rsi_series
