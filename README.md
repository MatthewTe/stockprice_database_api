# Stock Price Sqlite Database API
* [Instillation Instructions](https://github.com/MatthewTe/stockprice_database_api#instillation-instructions)
* [Price Database API](https://github.com/MatthewTe/stockprice_database_api#price-database-api)

## Intro
This is a package containing an api for creating/maintaining and interacting with a sqlite database containing price time series data on public stocks. This api mainly serves as a python wrapper for the [`yfinance`](https://pypi.org/project/yfinance/) api which in turn interacts with the Yahoo Finance stock web api (have I met my contractual obligation to use the word api as much as possible yet?).

## Instillation Instructions
This package is relatively straightforward to install. It can be installed via pip. All of the dependencies should be automatically installed however if not they can be installed manually:
```
pip install yfinance
pip install pandas
```

## Price Database API
The script `pricedb_api.py` contains the main method `price_db_api` which contains all the api's necessary to interact with the price database.

### `price_db_api(db_path)`
This is the main object that connects to the database. All subsequent methods require that this method be initialized with the path of the sqlite database file input as a string.

Once initialized, if the sqlite file does not exits it is created and then the `Summary` table is created and written to the database if it does not already exist:
|Ticker|Last_updated|
|------|------------|
|TEXT  | TEXT       |

This is the table that records all the ticker tables that are present in the database and the last time that each table has been updated. `Summary` table is modified/written to in most of the database writing methods.

### `update_ticker(ticker)`
This is the method that writes/updates time series price data of a specific ticker. Once the method is called and the ticker string input is valid, if a table does not already exist, a table for the ticker is created with the naming convention `{ticker}_timeseries` with the following schema:
|Date|Open|High|Low |Close|Volume |Dividends|Stock_Splits|Historical_Volatility|Annualized_Volatility|
|----|----|----|----|-----|-------|---------|------------|---------------------|---------------------|
|TEXT|REAL|REAL|REAL|REAL |INTEGER|REAL     |REAL        |REAL                 |                     |

The `yfinance` `Ticker` object is then initialized with the ticker string. A query is then made to this database for all of the `Date` values and that data is returned as a list.

This date value list (of date objects) as a result of the earlier query is used to determine if table `{ticker}_timeseries` is populated or not.

If it is empty then the entire time series of the stock is extracted as a pandas dataframe using the `yfinance` api and that dataframe is written to the table (via the use of the `pd.to_sql()` method). The `ticker` and `Last_updated` variables are then written to the `Summary` table via an `INSERT OR REPLACE` statement.

If the date value list is not empty then the pandas.to_sql method is wrapped in logic that ensures that only timeseries data that is not contained in the database table is written to the table. This is the main logic that is used to maintain/continually update this database.

It done by determining the most recent date written to the database and then by only writing timeseries data from subsequent dates to the table via the pandas.to_sql method.

The main table values `Date, Open, High, Low, Close, Volume, Dividends and Stock_Splits` are written to the database from the `yfinance` package. The rest of the table columns contain values derived from the core `yfinance` data and are inserted/written to the database via other "helper" methods, mainly the `.add_timeseries_technicals()` method.

**TODO: Describe how all helper methods facilitate writing data to the database.**

### `update_tickers(ticker_lst)`
This method is just an iterative wrapper over the earlier `update_ticker()` method. In ingests a list of ticker strings and for each ticker string calls the `update_ticker()` method with all of its subsequent logic and table update handling.

### `get_table(table_name)`
This is a brute force query method. It queries the database using the `pandas.read_sql_query()` method to return a dataframe of the table specified by the `table_name` parameter. If there is no table in the database with the name specified by `table_name` then it returns a None object.

Example:
```python
# Initializing connection to database:
test = price_db_api('path_to_sqlite_database')

# Calling the timeseries price data for Exxon:
test.get_table('XOM_timeseries')

# Calling the Summary table:
test.get_table('Summary')
```  

### `get_ticker_table(ticker, start_date=None, end_date=None)`
This is a query method that is intended to be used to specifically query timeseries data for a stock from the database (basically all data in this database except for the `Summary` table).

It does this by wrapping the previously used `pandas.read_sql_query()` method in logic that only requires the input of the ticker symbol with optional start and end date values that modify what timeseries is read from the database.

It should be noted that specifying a specific date time does not reduce read times from the database as the method extracts the entire ticker timeseries table, converts the `Date` value to `datetime` objects and then uses those date time objects to slice the entire dataframe into the timeseries specified by the `start_date` and `end_date` values. In other words it is irrelevant either or not dates are specified as a bulk data query is being executed either way.
