import asyncio
import websockets # Can be installed via anaconda environments
import json
import pandas as pd
import datetime as dt
import time
import nest_asyncio # Use anaconda prompt if not installed
nest_asyncio.apply()
import os

async def call_api(msg):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        while websocket.open:
            response = await websocket.recv()
            return response


def async_loop(api, message):
    return asyncio.get_event_loop().run_until_complete(api(message))


def retrieve_historic_data(start, end, instrument, timeframe):
    msg = \
        {
            "jsonrpc": "2.0",
            "id": 833,
            "method": "public/get_tradingview_chart_data",
            "params": {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": timeframe
            }
        }
    resp = async_loop(call_api, json.dumps(msg))

    return resp


# Convert Json to DataFrame

def json_to_dataframe(json_resp):
    res = json.loads(json_resp)

    df = pd.DataFrame(res['result'])

    df['ticks'] = df.ticks / 1000
    df['timestamp'] = [dt.datetime.utcfromtimestamp(date) for date in df.ticks]

    return df


# Define a function to get data on a certain instrument using its name and 2 timestamps, between which this instrument was traded

def get_data(date1, date2, instrument, tf='1'):
    
    # Collect data in daily steps
    
    n_days = (date2 - date1).days
    df_complete = pd.DataFrame()

    d1 = date1
    for _ in range(n_days):
        
        # Assumption: Both dates between which the data is collected are saved/given in datetime.datetime format
        d2 = d1 + dt.timedelta(days=1)

        t1 = dt.datetime.timestamp(d1) * 1000
        t2 = dt.datetime.timestamp(d2) * 1000    
    
        json_resp = retrieve_historic_data(t1, t2, instrument, tf)
    
        temp_df = json_to_dataframe(json_resp)
    
        df_complete = df_complete.append(temp_df)
        
        # Pause for some time to be able to collect further data
        
        print(f'collected data for dates: {d1.isoformat()} to {d2.isoformat()}')
        print('sleeping for 0.3 seconds')
        time.sleep(0.3)
        
        d1 = d2
        
    # Delete unnecessary columns
    
    keep = ['volume', 'cost', 'open', 'low', 'high', 'close', 'timestamp']

    df_complete = df_complete.loc[:,keep]
    
    # Convert timestamps into datetime format
    df_complete['timestamp'] = pd.to_datetime(df_complete['timestamp'], infer_datetime_format=True)

    # Filter out duplicates
    df_complete.drop_duplicates(subset=['timestamp'],
                                keep='first',
                                inplace=True)

    # Set timestamps as index
    df_complete = df_complete.set_index("timestamp")   
    
    # If you don't want to set timestamps as index, just reset the index
    # df_filtered.reset_index(inplace=True)    
        
    return df_complete




##########################################################################################
#%%


# Example #1 (Perpetual contract)

# start: First timestamp to collect data (YYYY, M, D, h, min)
start = dt.datetime(2021, 2, 6, 0, 0)

# end:First timestamp to collect data (YYYY, M, D, h, min)
end = dt.datetime(2021, 2, 10, 0, 0)

# instrument: set the name of the instrument (E.g. "ETH-PERPETUAL", "BTC-13FEB21-40000-C")
# Type "BTC-DMMMYY" to access futures, "BTC-PERPETUAL" for a perpetual contract, "BTC-DMMMYY-STRIKE-K" for options.
# BTC is currency, DMMMYY is expiration date,
# STRIKE is option strike price in USD, K is option kind: C - call options or P - put options.
instrument = "BTC-PERPETUAL"

# tf: set the periodicity of the extracted data (E.g. "1" for 1 minute, "1D" for 1 day)
tf = "1D"

# Create a DataFrame
df_example = get_data(start, end, instrument, tf)

# Change here the name of the csv-file, where the data will be saved
# df_example.to_csv("btc_perp.csv")


# The resulting DataFrame consists of following columns:
    
#  NAME      TYPE                Description    

#  volume ›	 array of number  ›  List of volume bars (in base currency, one per candle), i.e. Trading volume in bitcoin/etherium. 
#                                If data is collected e.g. in daily steps, then each value represents the sum of all trading volumes per day
#  cost   ›	 array of number  ›  List of cost bars (volume in quote currency, one per candle), i.e. Trading volume in USD
#  open   ›	 array of number  ›	 List of prices at open (one per candle)
#  close  ›	 array of number  ›  List of prices at close (one per candle)
#  high   ›  array of number  ›	 List of highest price levels (one per candle)
#  low    ›	 array of number  ›  List of lowest price levels (one per candle)
#  status ›	 string	          ›  Status of the query: ok or no_data
#  ticks  ›	 array of integer ›	 Values of the time axis given in milliseconds since UNIX epoch



#%%
# Example #2 (Future)

start = dt.datetime(2021, 2, 6, 0, 0)
end = dt.datetime(2021, 2, 10, 0, 0)
instrument = "BTC-26MAR21"
tf = "1"

df_example2 = get_data(start, end, instrument, tf)
# df_example2.to_csv("btc_future.csv")


#%%
# Example #3 (Option)

start = dt.datetime(2021, 2, 6, 0, 0)
end = dt.datetime(2021, 2, 10, 0, 0)
instrument = "BTC-26MAR21-36000-P"
tf = "1D"

df_example3 = get_data(start, end, instrument, tf)
# df_example3.to_csv("btc_option.csv")




##########################################################################################
#%%

# =============================================================================
# Getting all Data on Perpetual Contract:    on DAILY BASIS
# =============================================================================
    

start = dt.datetime(2018, 8, 13, 0, 0)  
end = dt.datetime(2021, 3, 31, 0, 0)
instrument = "BTC-PERPETUAL"
tf = "1D"

df_perp = get_data(start, end, instrument, tf)
df_perp.to_csv(r'E:\Perpetual\perpetual_daily.csv')


# The data on perpetual contract starts on 13th of August 2018 (2018-08-13)


#%%

# =============================================================================
# Getting all Data on Perpetual Contract:    on HOURLY BASIS
# =============================================================================


start = dt.datetime(2018, 8, 13, 10, 0)  
end = dt.datetime(2021, 3, 31, 9, 0)
instrument = "BTC-PERPETUAL"
tf = "60"

df_perp = get_data(start, end, instrument, tf)
df_perp.to_csv(r'E:\Perpetual\perpetual_hourly.csv')


#%%

# =============================================================================
# Getting all Data on Perpetual Contract:    on MINUTELY BASIS
# =============================================================================


start = dt.datetime(2018, 8, 13, 10, 0)  
end = dt.datetime(2021, 3, 31, 9, 0)
instrument = "BTC-PERPETUAL"
tf = "1"

df_perp = get_data(start, end, instrument, tf)
df_perp.to_csv(r'E:\Perpetual\perpetual_minutely.csv')










##########################################################################################
#%%
    
    
# =============================================================================
# ### Options: Basic information
# =============================================================================


# Expiration Dates

# Except for the 24-hour period following the introduction of a new expiry, two expiries of daily options, three expiries of weekly options, three expiries of monthly options and four expiries of quarterly options are available to trade.

# 1, 2 daily
# 1, 2, 3 weekly
# 1, 2, 3 monthly
# 3, 6, 9 and 12 months quarterly of the March, June, September, December cycle.

# Daily options expire every day at 08:00 UTC.
# Weekly options expire on each Friday of each week at 08.00 UTC.
# Monthly options expire on the last Friday of each calendar month at 08.00 UTC.
# Quarterly options expire on the last Friday of each calendar quarter at 08.00 UTC.


# Options: Introduction of new expiries
        
# A new options expiry date is added on Thursday immediately prior to expiration Friday except:

# A monthly expiry will not be added it if already exists as a quarterly expiry. Instead, this quarterly expiry will now be considered a monthly expiry.
# A weekly will not be added it if already exists as a monthly expiry. Instead, this monthly expiry will now be considered a weekly expiry

# Daily options are added the day immediately preceding the expiry day ( 08.00 UTC) and have therefore an initial lifetime of two trading days at the time of introduction.



##########################################################################################
#%%

# Function to retrieve various information about a certain instrument (of main importance: creation & expiration dates)

def get_instrument(instrument): 
    msg = \
        {
                "method" : "public/get_instrument",
                "params" : {
                        "instrument_name" : instrument
                        },
                        "jsonrpc" : "2.0",
                        "id" : 0
                }                   
    resp = async_loop(call_api, json.dumps(msg))

    return resp


def json_to_datafr(json_resp):
    res = json.loads(json_resp)
    
    # Simple indexing of a DataFrame to prevent ValueError
    df = pd.DataFrame(res['result'], index = [0])
    
    return df


def instrument_info(instrument):    
    json_inst = get_instrument(instrument)
    df_inst = json_to_datafr(json_inst)
    
    return df_inst


#%%   

# Example to "instrument_info" - function
    
opt_info_traded = instrument_info("BTC-24SEP21-80000-P")

opt_info_expired = instrument_info("BTC-25MAR21-50000-P")


df_info_ex = pd.DataFrame()
df_info_ex = df_info_ex.append(opt_info_traded)
df_info_ex = df_info_ex.append(opt_info_expired)


    

#%%


# =============================================================================
#  Loop to get each overall data on each previously existing instrument (names, creation/expiration timestamps)    
# =============================================================================


# DF including all options in 2018


currency = "BTC"                        # Define trading currency(Bitcoin: "BTC" or Etherium: "ETH")
str_range = range(1000, 61000, 1000)    # Define a range of possible strike prices for an option and a reasonable step


# Create an empty DataFrame to collect data on existing instruments
df_info = pd.DataFrame()


# A loop that changes the art of the option (Put: "P", Call: "C")
for art in ["P","C"]:
    
    # A loop that goes through all possible expiration dates in 2018
    # A loop over all years is also possible, but in that case the code would retrieve data for several days
    for year in range(18,19):
        for month in ['JAN','FEB','MAR', 'APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
            for num in range(1,32):
                day = str(num) + month + str(year)
                
                # A loop that goes through all possible strike prices
                for strike in str_range:
                    
                    # Principle: Check if an option exists, if yes: save its name to a list (DF)                     
                    try: 
                        # Construct an instrument name if one exists for chosen option art, expiration date and strike price
                        instrument = currency + "-" + day + "-" + str(strike) + "-" + art
                        print(instrument)
                        
                        df_info = df_info.append(instrument_info(instrument))   #Add collected data to a DF
                        
                    except KeyError:
                        print("No such instrument")
                                        

df_info.to_csv(r'E:\Options\options_list2.csv')

#%%

# Loop to get each previously or currently existing instrument   

# Possible: List of all option names / dataframe with info to each existing option     


# DF including all options in 2019

currency = "BTC"
str_range = range(1000, 81000, 1000)

# Creating an empty DataFrame to collect data on existing instruments
df_info2 = pd.DataFrame()


for art in ["P","C"]:
    
    for year in range(19,20):
        for month in ['JAN','FEB','MAR', 'APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
            for num in range(1,32):
                day = str(num) + month + str(year)
                
                for strike in str_range:
                    
                    try: 
                        instrument = currency + "-" + day + "-" + str(strike) + "-" + art
                        print(instrument)
                        
                        df_info2 = df_info2.append(instrument_info(instrument))
                        
                    except KeyError:
                        print("No such instrument")
                        

df_info2.to_csv(r'E:\Options\options_list2.csv')


#%%

# DF including all options in 2020

currency = "BTC"
str_range = range(1000, 121000, 1000)

# Creating an empty DataFrame to collect data on existing instruments
df_info3 = pd.DataFrame()


for art in ["P","C"]:
    
    for year in range(20,21):
        for month in ['JAN','FEB','MAR', 'APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
            for num in range(1,32):
                day = str(num) + month + str(year)
                
                for strike in str_range:
                    
                    try: 
                        instrument = currency + "-" + day + "-" + str(strike) + "-" + art
                        print(instrument)
                        
                        df_info3 = df_info3.append(instrument_info(instrument))
                        
                    except KeyError:
                        print("No such instrument")
                        

df_info3.to_csv(r'E:\Options\options_list3.csv')


#%%

# DF including all options 2021

currency = "BTC"
str_range = range(1000, 301000, 1000)

# Creating an empty DataFrame to collect data on existing instruments
df_info4 = pd.DataFrame()


for art in ["P","C"]:
    
    for year in range(21,22):
        for month in ['JAN','FEB','MAR', 'APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
            for num in range(1,32):
                day = str(num) + month + str(year)
                
                for strike in str_range:
                    
                    try: 
                        instrument = currency + "-" + day + "-" + str(strike) + "-" + art
                        print(instrument)
                        
                        df_info4 = df_info4.append(instrument_info(instrument))
                        
                    except KeyError:
                        print("No such instrument")
                        

df_info4.to_csv(r'E:\Options\options_list4.csv')





#%%

# Adjust of the resulting DataFrame for further computations (Definition)

def adjust_df(df):
    
    # Reset index from 0s
    df = df.reset_index()
    
    # Change timestamps to datetme format
    df["creation_timestamp"] = df["creation_timestamp"]/1000
    df["creation_timestamp"] = [dt.datetime.utcfromtimestamp(date) for date in df["creation_timestamp"]]
    
    df["expiration_timestamp"] = df["expiration_timestamp"]/1000
    df["expiration_timestamp"] = [dt.datetime.utcfromtimestamp(date) for date in df["expiration_timestamp"]]
    
    # Drop an additional index column
    df = df.drop(['index'], axis=1)
    
    return df


#%%

# Adjust the resulting DataFrame for further computations (Adjustment)

df_info = adjust_df(df_info)
df_info2 = adjust_df(df_info2)
df_info3 = adjust_df(df_info3)
df_info4 = adjust_df(df_info4)

# Save the adjusted DataFrames without an index column

#df_info.to_csv(r'E:\Options\options_list_2018.csv', index=False)
#df_info2.to_csv(r'E:\Options\options_list_2019.csv', index=False)
#df_info3.to_csv(r'E:\Options\options_list_2020.csv', index=False)
#df_info4.to_csv(r'E:\Options\options_list_2021.csv', index=False)



# Columns of the resulting DataFrame (df_info)


#  base_currency            ›     The underlying currency being traded. (ex. "BTC")
#  block_trade_commission	›     Block Trade commission for instrument
#  contract_size        	›     Contract size for instrument
#  creation_timestamp	    ›     The time when the instrument was first created (milliseconds)
#  expiration_timestamp    	›     The time when the instrument will expire (milliseconds)
#  instrument_name	        ›     Unique instrument identifier. (ex. "BTC-16APR21-40000-P")
#  is_active	            ›     Indicates if the instrument can currently be traded.
#  kind	                    ›     Instrument kind, "future" or "option"
#  leverage                 ›     Maximal leverage for instrument, for futures only
#  maker_commission         ›     Maker commission for instrument
#  min_trade_amount         ›     Minimum amount for trading. For perpetual and futures - in USD units, for options it is amount of corresponding cryptocurrency contracts, e.g., BTC or ETH.
#  option_type              ›     The option type (only for options): "put" or "call"
#  quote_currency           ›     The currency in which the instrument prices are quoted. (ex. "BTC")
#  settlement_period        ›     The settlement period.
#  strike                   ›     The strike value. (only for options)
#  taker_commission         ›     Taker commission for instrument
#  tick_size                ›     specifies minimal price change and, as follows, the number of decimal places for instrument prices


### Yet to find out, what contract size means









###############################################################################
#%%

# Load data for a certain year (Data for each year will be added to a single csv-file eventually)

file = "E:/Options/options_list_2021.csv"

df_info = pd.read_csv(file)

#%%

# Convert strings to datetime format
for i in range(len(df_info.index)):
    df_info.loc[i,"creation_timestamp"] = dt.datetime.strptime(df_info.loc[i,"creation_timestamp"], "%Y-%m-%d %H:%M:%S")
    df_info.loc[i,"expiration_timestamp"] = dt.datetime.strptime(df_info.loc[i,"expiration_timestamp"], "%Y-%m-%d %H:%M:%S")


#%%

# Example: get data for just one option (Same option as in example #4)

tf = "1D"
i = 1350    # instrument = "BTC-26MAR21-36000-P"


start = df_info.loc[i,"creation_timestamp"]
print(start)
end = df_info.loc[i,"expiration_timestamp"]
print(end)
    
instrument = df_info.loc[i,"instrument_name"]
print(instrument)
    
df_option = get_data(start, end, instrument, tf)



#%%

# =============================================================================
# # A loop that collects all data for each instrument in a DataFrame
# =============================================================================

# To get data on each option we use a predefined function "get_data(start, end, instrument, tf)"
# We also use known option names as well as their creation & expiration timestamps

tf = "1D"
df_error = []

for i in range(len(df_info.index)):
    
    try:
        
        start = df_info.loc[i,"creation_timestamp"]
        end = df_info.loc[i,"expiration_timestamp"]
        print(end)
        
        instrument = df_info.loc[i,"instrument_name"]
        
        df_option = get_data(start, end, instrument, tf)
        
        # Cut out expiration date from the instrument name to create a directory for that date
        
        day = instrument[4:11]
        os.makedirs("E:/Options/Daily/"+ day, exist_ok=True)  # Creates a new path for each new expiration date (PROBLEM: Dates below 10th of each month)
        df_option.to_csv(r'E:/Options/Daily/'+ day +'/option-daily_'+ instrument +'.csv')
        
    except KeyError:
        
        print("Error when collecting data")
        df_error += [instrument]


#%%
        
errors = pd.Series(df_error)
errors.to_csv(r'E:/Options/Daily/errors_2021.csv', index= False)














##########################################################################################
#%%

# =============================================================================
# ### Futures:  Basic Information
# =============================================================================


# Expiration Dates

# Expirations always take place at 08:00 UTC, on the last Friday of the month. 
# Currently, there are 3 quarterly futures (Expiring the last Friday of March, June, September, and December).


# Futures: Introduction of new expiries

# A new future with new expiry date will be added the last Friday of each calendar quarter at 08.00 UTC.
# This means there will never be more than three expiries available for trading at the same time.


#%%

# Example 4: Old future contract with expiration on 25th of September (last Friday of September)


start = dt.datetime(2020, 6, 26, 10, 0)
end = dt.datetime(2020, 9, 25, 10, 0)
instrument = "BTC-25SEP20"
tf = "1D"

df_example4 = get_data(start, end, instrument, tf)



#%%

# =============================================================================
# Create a DF which includes all BTC futures names, their creation and expiration timestamps
# =============================================================================

currency = "BTC"

# Creating an empty DataFrame to collect data on existing instruments
df_fut = pd.DataFrame()


for year in range(18,22):
    for month in ['JAN','FEB','MAR', 'APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']:
        for num in range(1,32):
            day = str(num) + month + str(year)
                
            try: 
                instrument = currency + "-" + day
                print(instrument)
                        
                df_fut = df_fut.append(instrument_info(instrument))
                        
            except KeyError:
                print("No such instrument")
                        

df_fut.to_csv(r'E:\Futures\futures_list_raw.csv', index=False)



#%%

# Adjust the resulting DataFrame for further computations (Adjustment)

df_fut = adjust_df(df_fut)

df_fut.to_csv(r'E:\Futures\futures_list.csv', index=False)



##########################################################################################
#%%

# =============================================================================
# Here you can work with a DF created with a loop in rows 678-718 or yoy can directly load the resulting table
# =============================================================================

# Load data for a certain year (Data for each year will be added to a single csv-file eventually)

file = "E:/Futures/futures_list.csv"

df_fut = pd.read_csv(file)

#%%

# Convert strings to datetime format
for i in range(len(df_fut.index)):
    df_fut.loc[i,"creation_timestamp"] = dt.datetime.strptime(df_fut.loc[i,"creation_timestamp"], "%Y-%m-%d %H:%M:%S")
    df_fut.loc[i,"expiration_timestamp"] = dt.datetime.strptime(df_fut.loc[i,"expiration_timestamp"], "%Y-%m-%d %H:%M:%S")


#%%


# =============================================================================
# # A loop that collects all data for each future in a single DataFrame
# =============================================================================

# To get data on each future we use a predefined function "get_data(start, end, instrument, tf)"
# We also use known futures names as well as their creation & expiration timestamps



# =============================================================================
# Get all Data on Futures:    on DAILY BASIS
# =============================================================================


tf = "1D"
df_error = []

for i in range(len(df_fut.index)):
    
    try:
        
        start = df_fut.loc[i,"creation_timestamp"]
        end = df_fut.loc[i,"expiration_timestamp"]
        print(end)
        
        instrument = df_fut.loc[i,"instrument_name"]
        
        df_future = get_data(start, end, instrument, tf)
        
        # Cut out expiration date from the instrument name to create a directory for that date
        
        day = instrument[4:11]
        df_future.to_csv(r'E:/Futures/Daily/'+ day +'/future-daily_'+ instrument +'.csv')
        
    except KeyError:
        
        print("Error when collecting data")
        df_error += [instrument]

        
errors = pd.Series(df_error)
errors.to_csv(r'E:/Futures/Daily/errors_daily.csv', index= False)

#%%

# =============================================================================
# Get all Data on Futures:    on HOURLY BASIS
# =============================================================================


tf = "60"
df_error = []

for i in range(len(df_fut.index)):
    
    try:
        
        start = df_fut.loc[i,"creation_timestamp"]
        end = df_fut.loc[i,"expiration_timestamp"]
        print(end)
        
        instrument = df_fut.loc[i,"instrument_name"]
        
        df_future = get_data(start, end, instrument, tf)
        
        # Cut out expiration date from the instrument name to create a directory for that date
        
        day = instrument[4:11]
        df_future.to_csv(r'E:/Futures/Hourly/future-hourly_'+ instrument +'.csv')
        
    except KeyError:
        
        print("Error when collecting data")
        df_error += [instrument]

        
errors = pd.Series(df_error)
errors.to_csv(r'E:/Futures/Hourly/errors_hourly.csv', index= False)




#%%

# =============================================================================
# Get all Data on Futures:    on MINUTELY BASIS
# =============================================================================



tf = "1"
df_error = []

for i in range(len(df_fut.index)):
    
    try:
        
        start = df_fut.loc[i,"creation_timestamp"]
        end = df_fut.loc[i,"expiration_timestamp"]
        print(end)
        
        instrument = df_fut.loc[i,"instrument_name"]
        
        df_future = get_data(start, end, instrument, tf)
        
        # Cut out expiration date from the instrument name to create a directory for that date
        
        day = instrument[4:11]
        df_future.to_csv(r'E:/Futures/Minutely/future-minutely_'+ instrument +'.csv')
        
    except KeyError:
        
        print("Error when collecting data")
        df_error += [instrument]

        
errors = pd.Series(df_error)
errors.to_csv(r'E:/Futures/Minutely/errors_minutely.csv', index= False)






##########################################################################################
#%%












