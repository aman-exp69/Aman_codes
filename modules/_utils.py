import pandas as pd
from datetime import datetime
import json

from modules.global_variables import params


def preprocess_eis_data(data:pd.DataFrame) -> pd.DataFrame:
    """
        This method is specifically written for preprocessing EIS data
    """

    # drop column
    data.drop(['UnixTimefrom 1-1-1980'], axis = 1, inplace = True)
    # convert object type to float type
    float_col_list = ['BidPrice','BidQty','AskPrice', 'AskQty', \
               'TTq','LTP','TotalTradedPrice','Strike']
    
    data[float_col_list] = data[float_col_list].astype('float64')
    # data['ExchToken'] = data['ExchToken'].astype('str') 
    data['ExchToken'] = data['ExchToken'].astype('int64') # as per requirement

    
    #strip white spaces from every cell
    data = data.apply(lambda x: x.str.strip() if type(x) == "<class 'str'>" else x)
    
    # strip the text columns
    for col in data.columns:
        if data[col].dtype == "O":
            data[col] = data[col].astype(str)
            data[col] = data[col].apply(lambda x: x.strip())
    
    # create a new column ExpiryDateTime
    data['ExpiryDate'] = pd.to_datetime(data['ExpiryDate'].str.strip(),format='%d-%m-%Y')
    data['ExpiryDate'] = data['ExpiryDate'].dt.strftime('%Y-%m-%d')
    data['ExpiryDateTime'] = data['ExpiryDate'] + ' ' + data['ExpiryTime']

    #drop rows where type = 'xx', strike = -1, bidprice > askprice, bidqty and askqty = 0
    to_drop = data[(data['Type'] == 'XX') | (data['Strike'] == -0.01) | (data['BidPrice'] > data['AskPrice']) | (data['BidQty'] == 0.0)
    | (data['AskQty'] == 0.0)].index.to_list()

    data.drop(data.index[to_drop], inplace = True)

    #set date time as index
    data.set_index('Date Time', inplace = True)

    # convert as datetime for specific colums such as Date Time, ExpiryDate, ExpiryTime, ExpiryDateTime
    data.index = pd.to_datetime(data.index)
    data['ExpiryDate'] = pd.to_datetime(data['ExpiryDate'])
    data['ExpiryDateTime'] = data['ExpiryDateTime'].astype('str')
    data['ExpiryDateTime'] = pd.to_datetime(data['ExpiryDateTime'])

    
    # divide the bid, ask price and strike by 100 since these are in paisa
    data['BidPrice'] = data['BidPrice'].apply(lambda x:x/100)
    data['AskPrice'] = data['AskPrice'].apply(lambda x:x/100)
    data['Strike'] = data['Strike'].apply(lambda x:x/100)

    # print(data.loc[data['ExchToken']==35023])

    return data

def date_difference(date1: datetime, date2: datetime) -> int:
    '''This function return difference between two dates'''

    # difference between dates in timedelta
    delta = date2 - date1
    return delta.days


def get_market_holidays_by_year(year:int)->list:
    '''
    This function gets the holiday list for a given year
    '''

    with open(f"{params['HOLIDAY_LIST_STORE']}holidays_{year}.json", 'r') as f:
        loaded_list = json.load(f)

    date_list = list()
    for dt in loaded_list:
        date_time = datetime.strptime(dt,params['DATE_TIME_FORMAT'])
        date_list.append(date_time.date())

    return date_list


def load_data(data_file_path:str) -> pd.DataFrame:
    # Read the data
    data=pd.read_csv(data_file_path)

    return data