import warnings 
warnings.filterwarnings("ignore")

import pandas as pd

# Option Data Preprocessing
def option_data_preparation(path: str, flag: bool = True) -> tuple:

    # Read the data
    data=pd.read_csv(path)

    if flag == True:
        # divide the bid, ask price and strike by 100 since these are in paisa
        data['BidPrice'] = data['BidPrice'].apply(lambda x:x/100)
        data['AskPrice'] = data['AskPrice'].apply(lambda x:x/100)
        data['Strike'] = data['Strike'].apply(lambda x:x/100)

    # filter the call and put
    call_data = data[data['Type'] == ' CE']
    call_data.reset_index(drop = True, inplace=True)
    put_data = data[data['Type'] == ' PE']
    put_data.reset_index(drop = True, inplace=True)

    # set index
    call_data = call_data.set_index("Date Time")
    put_data = put_data.set_index("Date Time")
    
    return call_data, put_data

# load the underlying price 
def load_underlying_price(underlying_path: str, date_col:str = 'Date Time')-> pd.DataFrame:
    # Read Underlying Instrument
    underlying_price = pd.read_csv(underlying_path)
    underlying_price[date_col] = underlying_price['Date'] +str(' ')+ underlying_price['Time'] + str(':00')
    underlying_price = underlying_price.set_index(date_col)

    return underlying_price

# get the underlying price for a specific time
def get_underlying_price(underlying_path: str, time: str, date_col: str = 'Date Time',price_col: str='Close')-> int:
    # Underlying
    underlying_price = pd.read_csv(underlying_path)
    underlying_price[date_col] = underlying_price['Date'] +str(' ')+ underlying_price['Time'] + str(':00')
    underlying_price.set_index(date_col, inplace = True)
    spot = underlying_price.loc[time][price_col]

    return spot
