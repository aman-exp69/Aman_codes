import pandas as pd
from modules._utils import load_data, preprocess_eis_data, date_difference
from datasets import _load_data_file, DATA_PATH
import numpy as np
import datetime

from datetime import datetime
from dateutil.relativedelta import relativedelta, TH
from .global_variables import params
from modules._logger import logger,get_exception_line_no

DEBUG = params['DEBUG']

logger = logger.getLogger('historical_data')

class NoOptionsFound(Exception):
    def __init__(self, error_message: str = "No options found.") -> None:
        super().__init__(error_message)

        self.error_message = error_message

    def __str__(self):
        return self.error_message



class HistoricalData():
    
    """
    Class Description
    ------------------
    Historical Data is a class which contains following methods getName(), getData(), preprocess().

    Parameters
    ----------
    
    source : Source Name # currently eis_data
    name : this is the name of historical data
    instrument : what is historical data consists of?
    start_date : starting datetime
    end_date : ending datetime
    expiry_type : monthly or weekly or nearest weekly or nearest monthly or second_weekly or second_monthly
    
    # expiry_type is  a flag to preprocess weekly or monthly or all


    

    Methods
    -------

    getName() : Return the name of the source

    getData() : Return historical market data

    getSource() : Return the market data

    load_market_data() : load the preprocessed market data.

    get_quote(t, option_type, expiry, strike) : Return 4 tuple values such as bid price, bid qty, ask price, ask qty respectively.

    get_option_detail_from_id(id) : Returns a tuple which consists Strike , ExpiryDateTime, Option Type  respectively.

    get_spot(t) : Returns spot price

    preprocess() : preprocess the market data

          
    """
    def __init__(self, source:str, name : str,underlying_instrument:str, start_date : datetime, end_date: datetime, expiry_type: str = 'all'):
        self._source = source
        self._instrument = underlying_instrument
        self._name = name
        self._start_date = start_date
        self._end_date = end_date
        # Let's not Standarized Right now : column names are specific to EIS DATA only.
        cols = ['Date Time', 'UnixTimefrom 1-1-1980', 'ExchToken', 'BidPrice', 'BidQty',
       'AskPrice', 'AskQty', 'TTq', 'LTP', 'TotalTradedPrice', 'Instrument',
       'ExpiryDate', 'ExpiryTime', 'Strike', 'Type']
        self._data = pd.DataFrame(columns=cols)

        self._expiry_type = expiry_type

        # following features are specifically used during data slicing and 
        # gets populated within self.getSlice()
        self._slice_expiry = None
        self._slice_time = None


    def getSlice(self,t:datetime):
        '''
        returns a historical data object for  smaller time slice also stores current slice
        same methods will work on it
        '''
        slice_data = HistoricalData('','','',t,t)
        slice_data._data = self._data.loc[t]
        slice_data._source = self._source
        slice_data._instrument = self._instrument
        slice_data._name = self._name
        slice_data._expiry_type = self._expiry_type
        slice_data._slice_expiry = slice_data._data["ExpiryDateTime"][0] #this is the ONLY expiry in the slice
        slice_data._slice_time = t

        #Need to fix indexing and then change it across the board in all methods

        return slice_data
                               

    def get_slice_expiry(self):
        return self._slice_expiry
    
    def get_nearest_strike_premium(self, t: datetime, strike: float) -> tuple:

        # TODO: Assumption expiry_type is selected as nearest_weekly or nearest_monthly (i.e, a single expiry)
        if self._source =='eis_data':
            slice_data_obj = self.getSlice(t)
            slice_data = slice_data_obj.getData()

            temp = {abs(nstrike - strike):nstrike for nstrike in slice_data['Strike']}
            value = temp[min(temp)]
            del temp

            return slice_data[slice_data['Strike'] == value][['BidPrice', 'BidQty', 'AskPrice', 'AskQty']].values
        else:
            logger.debug('Select from given source(eis_data)')
    
    def get_exercise_list(self, qtime:datetime) -> list: 
        """ 
        Parameters 
        ---------- 
        t: at time t 
        
        Returns 
        ------- 
        A list of exercise dates, available as of date "t". 
        """ 
        
        if self._source == 'eis_data':
            exercise_dates = list(set(self._data.loc[qtime, "ExpiryDateTime"].to_list()))
            # print(exercise_dates)
            return exercise_dates
        else:
            # TODO: should raise exception
            logger.critical("Please type the source from given names ('eis_data')")

    def get_specific_expiry(self, expiry_list : list, expiry_type :str = 'weekly'):

        """ 
            Parameters 
            ---------- 
            expiry_list: list of expiry

            expiry_type: type of expiry. Default is weekly. (it can be weekly, monthly, nearest_weekly, nearest_monthly, second_weekly, second_monthly)
            
            Returns 
            ------- 
            A list of expiry date, for the given expiry type. 
        """ 

        # return monthly expiry_list 
        def monthly_expiry_dates():
            monthly_expiry = []
            for expiry in expiry_list:

                expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
                cmon = expiry_date.month

                for i in range(1, 6):
                    t = expiry_date + relativedelta(weekday=TH(i))
                    if t.month != cmon:
                        # since t is exceeded we need last one  which we can get by subtracting -2 since it is already a Thursday.
                        t = t + relativedelta(weekday=TH(-2))
                        if t.strftime('%Y-%m-%d') == expiry:
                            monthly_expiry.append(t.strftime('%Y-%m-%d'))
                        break
            return monthly_expiry

        expiry_list.sort() # sort the expiry list in ascending order
        expiry_list = [expiry.strftime('%Y-%m-%d') for expiry in expiry_list]

        # if expriy type is monthly
        if expiry_type.lower() == 'monthly':

            monthly_expiry = monthly_expiry_dates()
            return monthly_expiry

        # if expiry type is weekly
        elif expiry_type.lower() == 'weekly':
            # Uncomment below two lines if we need to exclude last thrusday of month from weekly expiry list
            # need to check the sorting part
            # monthly_expiry = monthly_expiry_dates()
            # expiry_list = list(set(expiry_list) - set(monthly_expiry))
            return expiry_list

        # if expiry type is nearest_weekly return the first expiry date
        elif expiry_type.lower() == 'nearest_weekly':
            # TODO : add logger , checking whether nearest_weekly exists or not ?
            return expiry_list[0]

        # if expiry type is nearest_monthly return the first expiry date from monthly expiry
        elif expiry_type.lower() == 'nearest_monthly':
            # since the expiry_list is already sort , we can just select first date as nearest monthly expiry
            #Cases 1: expiry_list = ['2021-03-10', '2021-06-24'], NOTE : File_date : 2021-03-10
            #         To check the nearest monthly expiry from the expiry list , need to check difference between
            # current date and first monthly expiry from expiry list is less than or equal to 31 not .
            #current date
            try:
                cur_date = self._data.index[0]
                monthly_expiry = monthly_expiry_dates()

                if date_difference(cur_date, pd.to_datetime(monthly_expiry[0])) > 31:

                    logger.critical('Nearest Monthly doest exists.')
                # Logic : elif date_difference(cur_date, monthly_expiry[0]) <= 31: #
                else:
                    return monthly_expiry[0]
            except Exception as e:
                logger.critical(f'Error in nearest_monthly in line : {get_exception_line_no()}, error : {e}')
                raise e
        elif expiry_type.lower() == 'second_weekly':
            # since the expiry_list is already sort , we can just select second date as second weekly expiry
            #Cases 1: expiry_list = ['2021-03-10', '2021-06-24'], NOTE : File_date : 2021-03-10
            #         To check the second weekly expiry from the expiry list , need to check difference between
            # current date and second expiry date from expiry list is 8 or not .

            # Assumption : Difference is 8 because there are cases where we have holiday.
            try:

                cur_date = self._data.index[0]

                if date_difference(cur_date, pd.to_datetime(expiry_list[1])) > 8:
                    
                    logger.critical('Second Weekly doest exists.')
                #Logic : elif date_difference(cur_date, expiry_list[1]) <= 8:
                else:

                    return expiry_list[1]
            except Exception as e:
                logger.critical(f'Error in second_weekly in line : {get_exception_line_no()}, error : {e}')
                raise e
        elif expiry_type.lower() == 'second_monthly':

            # monthly_expiry = monthly_expiry_dates()
            # return monthly_expiry[1]

            cur_date = self._data.index[0]
            monthly_expiry = monthly_expiry_dates()
            # case 1: monthly_expiry list consist only one expiry
            #         So, second monthly doest not exists.
            try:
                if len(monthly_expiry) == 1:
                    logger.critical('Second monthly doest exists.')
                else:
                    if date_difference(cur_date, pd.to_datetime(monthly_expiry[1])) > 62:
                        logger.critical('Second monthly doest exists.')
                    # Logic : elif date_difference(cur_date, monthly_expiry[1]) <= 62:
                    else:
                        
                        return monthly_expiry[1]
            except Exception as e:
                logger.critical(f'Error in second_monthly in line : {get_exception_line_no()}, error : {e}')
                raise e

        else:
            logger.debug('This Expiry Type is not available. Please Select from the list:[weekly, monthly, nearest_weekly, nearest_monthly]')

    def load_market_data(self):
        """
        Method Description
        ------------------
        
        method inside historical data object
    
        Return : return a object filled with market dataframe 
        """
        
        # eis data 
        if self._source =='eis_data':
            dates = pd.date_range(start=self._start_date,end=self._end_date).strftime('%Y-%m-%d').to_list()
            # final_df = pd.DataFrame(columns=self._data.columns)
            self._data = pd.DataFrame(columns=self._data.columns)
            for date in dates:
                    
                    # assumption date format : 2021-03-10 09:16:00
                    year = date[:4]
                    month = date[5:7]
                    day = date[8:10]
                    
                    date = year+month+day
                
                    data_np, columes = _load_data_file(DATA_PATH, self._instrument +'_'+ date + '_Intraday.csv')
                    df = pd.DataFrame(data_np,columns=self._data.columns)
                    
                    # final_df = pd.concat([final_df, df], axis = 0)
                    self._data = pd.concat([self._data, df], axis = 0)
        elif self._source == 'refinitiv':
            logger.debug("This source is not available")
        else:
            # TODO: should raise exception
            logger.debug("Please type the source from given names ('eis_data')")

        self.preprocess()
        
        # add expiry_type to filter data
        if self._expiry_type.lower() == 'all':
            # do nothing
            pass
        elif self._expiry_type.lower() == 'weekly':
            # weekly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            weekly_expiry_list = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'weekly')
            self._data = self._data.loc[self._data['ExpiryDate'].isin(weekly_expiry_list)]
        elif self._expiry_type.lower() == 'monthly':
            # monthly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            monthly_expiry_list = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'monthly')
            self._data = self._data.loc[self._data['ExpiryDate'].isin(monthly_expiry_list)]

        elif self._expiry_type.lower() == 'nearest_weekly':
            # nearest_weekly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            nearest_weekly_expiry = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'nearest_weekly')
            self._data = self._data.loc[self._data['ExpiryDate'] == nearest_weekly_expiry]

        elif self._expiry_type.lower() == 'nearest_monthly':
            # nearest_monthly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            nearest_monthly_expiry = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'nearest_monthly')
            self._data = self._data.loc[self._data['ExpiryDate'] == nearest_monthly_expiry]

        elif self._expiry_type.lower() == 'second_weekly':
            # second_weekly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            second_weekly_expiry = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'second_weekly')
            self._data = self._data.loc[self._data['ExpiryDate'] == second_weekly_expiry]
        elif self._expiry_type.lower() == 'second_monthly':
            # second_monthly expiry filter
            expiry_list = list(set(self._data['ExpiryDateTime']))
            second_monthly_expiry = self.get_specific_expiry(expiry_list = expiry_list, expiry_type = 'second_monthly')
            self._data = self._data.loc[self._data['ExpiryDate'] == second_monthly_expiry]
        else:
            logger.debug('This Expiry Type is not available. Please Select from the list:[weekly, monthly, nearest_weekly, second_weekly,nearest_monthly,second_monthly]')




    def get_quote(self, t, option_type, expiry, strike)-> tuple:

        '''
        Parameters:
        ---------
                 t: time 
                 option_type : type of option (e.g, 'CE', 'PE')
                 expiry : expiry date time (e.g, '2021-06-24 15:30:00')
                 strike : Strike Price
        Return 4 tuple values such as bid price, bid qty, ask price, ask qty respectively.
        '''
        if self._source == 'eis_data':

            quote_data = self._data.loc[t]
            quote_data = quote_data[(quote_data['Type'] == option_type) & (quote_data['ExpiryDateTime'] == expiry) & (quote_data['Strike'] == strike)]
            if len(quote_data) == 1:
                return np.array(quote_data[['BidPrice', 'BidQty', 'AskPrice', 'AskQty']]).flatten()
            
            elif len(quote_data) == 0:
                # TODO: need to check why for some options quote_data is empty
                logger.debug(f'Data Not available for opion_type={option_type}, expiry={expiry}, strike={strike}')
                raise NoOptionsFound(f'Data Not available for opion_type={option_type}, expiry={expiry}, strike={strike}')
        else:
            logger.debug('Select from given source(eis_data)')

    def get_quote_by_id(self, t, instrument_id:int)-> tuple:

        '''
        Parameters:
        ---------
                 t: time 
                 option_type : type of option (e.g, 'CE', 'PE')
                 expiry : expiry date time (e.g, '2021-06-24 15:30:00')
                 strike : Strike Price
        Return 4 tuple values such as bid price, bid qty, ask price, ask qty respectively.
        '''
        if self._source == 'eis_data':

            # quote_data = self._data.loc[t]
            quote_data = self._data.loc[self._data['ExchToken'] == instrument_id]
            if len(quote_data) == 1:
                return np.array(quote_data[['BidPrice', 'BidQty', 'AskPrice', 'AskQty']]).flatten()
            
            elif len(quote_data) == 0:
                # TODO: need to check why for some options quote_data is empty
                logger.debug(f'Data Not available for opion_id={instrument_id}')
                raise NoOptionsFound(f'Data Not available for opion_id={instrument_id}')
        else:
            logger.debug('Select from given source(eis_data)')


    def get_option_detail_from_id(self, id: int) -> tuple:
        """
        parameters:
        ---------
           id : ExchangeID

        Returns:
                returns a tuple which consists Strike , ExpiryDateTime, Option Type  respectively.

        Note :
        ------
            Right Now 'ExchToken' is considered as id for EIS DATA
        
        """
        if self._source == 'eis_data':

            option_detail_from_id_data = self._data[self._data['ExchToken'] == id]

            if option_detail_from_id_data.shape[0] == 0:
                raise NoOptionsFound(error_message=f'No options found with id={id}')
            
            strike = option_detail_from_id_data['Strike'].unique()[0]
            expiry = option_detail_from_id_data['ExpiryDateTime'].unique()[0]
            type = option_detail_from_id_data['Type'].unique()[0]

            return strike, expiry, type

        else:
            logger.debug('Select from given source(eis_data)')


    def get_option_dtls_from_id_list(self, id_list: list) -> list:
        """
        parameters:
        ---------
           id : ExchangeID

        Returns:
                returns a tuple which consists Strike , ExpiryDateTime, Option Type  respectively.

        Note :
        ------
            Right Now 'ExchToken' is considered as id for EIS DATA
        
        """
        if self._source == 'eis_data':

            tmp_df = self._data.loc[self._data['ExchToken'].isin(id_list),['Strike','ExpiryDateTime','Type']].drop_duplicates()

            if tmp_df.shape[0] == 0:
                raise NoOptionsFound(error_message=f'No options found with id={id_list}')
            
            return tmp_df.values.tolist() #

        else:
            logger.debug('Select from given source(eis_data)')


    def get_max_expiry_from_options(self, id_list:list) -> datetime:
        if self._source == 'eis_data':

            max_expiry = self._data.loc[self._data['ExchToken'].isin(id_list),['ExpiryDateTime']].max()
            return max_expiry

        else:
            logger.debug('Select from given source(eis_data)')


    def get_spot_v2(self, ctime:datetime):
        '''

        '''
        # group the dataframe by "Type","ExpiryDateTime", "Strike", "BidPrice", "AskPrice"
        grp_df = self._data.loc[ctime].groupby(["Type","ExpiryDateTime", "Strike", "BidPrice", "AskPrice"], as_index=False).count()

        # since we need both the CE and PE separately, break them into two dfs
        ce_df = grp_df.loc[grp_df['Type']=='CE',['Type','ExpiryDateTime','Strike','BidPrice','AskPrice']]
        pe_df = grp_df.loc[grp_df['Type']=='PE',['Type','ExpiryDateTime','Strike','BidPrice','AskPrice']]

        # then merge both CE and PE dfs to put side by side so that we can compute row wise
        merged_df = pd.merge(ce_df,pe_df,on=['ExpiryDateTime','Strike'],how='inner')

        # compute bid and ask spots
        merged_df['spot_bid'] = merged_df['Strike'] + merged_df['BidPrice_x'] - merged_df['AskPrice_y']
        merged_df['spot_ask'] = merged_df['Strike'] + merged_df['AskPrice_x'] - merged_df['BidPrice_y']

        # finally return spot
        return (merged_df['spot_bid'].max() + merged_df['spot_ask'].min())/2


    def get_spot(self, t:datetime) -> float:
        spot_value = 0
        # market data at time t
        if self._source == 'eis_data':
            # groupby expiry
            # select each expiry and all strikes under that expiry
            # calculate spot bid and ask using above formulas and take the average of (highest bid + lowest ask)
            # spot bid and ask store

            spot_Bid, spot_Ask = [], []

            for z, y in self._data.loc[t].set_index(["ExpiryDateTime", "Strike"]).index.unique():
                # query the dataframe at expiry z and strike y
                temp = self._data.loc[t].set_index(["ExpiryDateTime", "Strike"]).loc[(z, y)]

                # using a try catch block to avoid errors due to missing columns -> for eg: PE or CE both not be present for all strikes at the same time.
                try:
                    # check if CE and PE both exists.
                    if len(temp[temp.Type=="PE"]) == 0 or len(temp[temp.Type=="CE"]) == 0:
                        continue
                    
                    # calculate the spot bid and ask using formula
                    spot_bid = (y + temp[temp.Type=="CE"]["BidPrice"] - temp[temp.Type=="PE"]["AskPrice"]).values[0]
                    spot_ask = (y + temp[temp.Type=="CE"]["AskPrice"] - temp[temp.Type=="PE"]["BidPrice"]).values[0]

                    # store them in a list or array
                    spot_Bid.append(spot_bid)
                    spot_Ask.append(spot_ask)

                # Just pass for now (can be customised)
                except:
                    pass

            # Return final spot value
            spot_value = (max(spot_Bid) + min(spot_Ask)) / 2
        else:
            logger.debug('Source is not available !Please select from given source :(eis_data)')

        return spot_value
    
    
    # TODO: underlying and expiry need to be incorporated properly 
    def get_atm_option(self, qtime:datetime, underlying:str, expiry:datetime, option_type:str):
        """
            steps:
                1. get all the available options at the query time (qtime) from the market data based on option type
                2. 
        """
        logger.debug('within get_atm_option')
        if self._source == 'eis_data':
            try:
                # get the market data
                # tmp_data = self._data
                # get the options at current time
                spot = self.get_spot_v2(qtime)
                logger.debug(f'atm_spot={spot}')
                
                # TODO: do we need the following step anymore since data is already time sliced?
                data = self._data.loc[qtime]
                if data.shape[0] == 0:
                    # raise if no option found
                    raise NoOptionsFound(f'no options found  at time{qtime}')
                    # print(f'no options found with spot={spot} and type={option_type} at time{qtime}')
                

                # TODO: try to combine the following filter with qtime filter to optimize
                # get the data based on option type
                data = data[(data['Type']==option_type) & (data['ExpiryDateTime']==expiry) & (data['Instrument']==underlying)]

                # print(type(data))
                if data.shape[0] == 0:
                    raise NoOptionsFound(error_message=f'no options found of type={option_type}, expiry={expiry}, underlying={underlying}')
                
                # get the upper and lower limit            
                u = (data[data['Strike'] > spot]['Strike']).min()
                l = (data[data['Strike'] < spot]['Strike']).max()

                strike = 0
                if (u - spot) < (spot - l):
                    strike = u
                else:
                    strike = l

                # if the strike is certain percentage away from the spot
                # we wont take the code
                strike_tolerance = params['STRIKE_TOLERANCE']
                if (abs(spot - strike)/spot) > strike_tolerance:
                    raise NoOptionsFound(error_message=f'no options found of type={option_type} within specified {strike_tolerance*100}% strike tolerance. spot={spot},strike={strike}')

                # print(strike)
                # print(data[data['Strike']==strike])
                strike, expiry, id, instrument = data[data['Strike']==strike][['Strike', 'ExpiryDateTime', 'ExchToken', 'Instrument']].values.flatten()
                
                # print(strike, expiry, id, instrument)
                # TODO: check the output parameters whether they are required
                return strike, expiry, id, instrument
                    
            except Exception as e:
                logger.critical(f'WARNING:{e}')
                raise e

        else:
            logger.debug('Source is not available !Please select from given source :(eis_data)')


    # (self, qtime, underlying, expiry, option_type)
    # TODO: this method added by TR. need approval for this inclusion. 
    def get_otm_option(self, qtime:str, atm_strike:float, underlying:str, expiry:datetime, option_type:str, pct:float):
        """
            given the moneyness type (otm or itm) and by how much pct(%)

            Parameters:
            TODO: Q: should we keep moneyness? or use sign of pct to decide itm or otm?

            get the market data
            get the options available at the current time (qtime)
            filter again by option_type
            compute the itm or otm strike based on requirement
                otm put 
        """

        '''
            get the market data
            get the options available at the current time (qtime)
            filter again by option_type

        '''
        logger.debug('within get_otm_option')

        if self._source == 'eis_data':
            try:
                # TODO: need to check the following logic
                # atm_strike = self.get_spot(t=qtime)
                # if DEBUG:
                logger.debug(f'atm_strike={atm_strike}, expiry={expiry}, option_type={option_type}')
                # tmp_data = self._data
                data = self._data.loc[qtime]
                if data.size == 0:
                    # raise NoOptionsFound(error_message=f'No options found at the current time {ctime}')
                    logger.critical(f'No options found at the current time {qtime}')
                data = data[(data['Type']==option_type) & (data['ExpiryDateTime']==expiry)]
                if data.size == 0:
                    # raise NoOptionprint(sFound(error_message=f'No options found of type={option_type} at time={ctime}')
                    logger.critical(f'No options found of type={option_type} at time={qtime}')

                '''compute the itm or otm strike based on requirement'''
                otm_spot = 0
                if option_type == 'CE': # TODO: constants like OPTION_TYPE_CALL should be used
                    otm_spot = atm_strike + round((atm_strike * pct)/100)
                elif option_type == 'PE': # TODO: constants like OPTION_TYPE_PUT should be used
                    otm_spot = atm_strike - ((atm_strike * pct)/100)

                u = (data[data['Strike'] > otm_spot]['Strike']).min()
                l = (data[data['Strike'] < otm_spot]['Strike']).max()

                logger.debug(f'otm_spot={otm_spot}')
                # logger.debug('Strike < otm_spot')
                # logger.debug((data.loc[data['Strike'] <= otm_spot,['Strike']]))
                # logger.debug('Strike > otm_spot')
                # logger.debug((data.loc[data['Strike'] >= otm_spot,['Strike']]))
                # logger.debug(f'u={u} and l={l}')

                strike = 0
                if (u - otm_spot) < (otm_spot - l):
                    strike = u
                else:
                    strike = l

                # if the strike is certain percentage away from the spot
                # we wont take the code
                strike_tolerance = params['STRIKE_TOLERANCE']
                if (abs(otm_spot - strike)/otm_spot) > strike_tolerance:
                    raise NoOptionsFound(error_message=f'no options found of type={option_type} within \
                                         specified {strike_tolerance*100}% strike tolerance otm_spot={otm_spot} and strike={strike}')


                # logger.debug(f'strike={strike}')
                # logger.debug('data[data[Strike]==strike]')
                # logger.debug(data[data['Strike']==strike])

                strike, expiry, id, instrument = data[data['Strike']==strike][['Strike', 'ExpiryDateTime', 'ExchToken', 'Instrument']].values.flatten()
                return strike, expiry, id, instrument

            except Exception as ex:
                logger.critical(ex)

        else:
            logger.debug('Source is not available !Please select from given source :(eis_data)')


    def preprocess(self) -> pd.DataFrame:

        # if the data source is eis data , call specific preprocess method
        if self._source.lower() == 'eis_data':
            self._data = preprocess_eis_data(self._data)

        # if the data source is Refinitive , call specific preprocess method
        elif self._source.lower() == 'Refinitive'.lower():
            logger.debug('This source is not available')
        else:
            logger.debug('please select from the available from sources :(eis_data)')


    # getters and setters

    def getName(self)->str:
        return self._name

    def getData(self)->pd.DataFrame:
        return self._data

    def getSource(self)->str:
        return self._source
    
    def getInstrument(self)->str:
        return self._instrument
    
    def getStartDate(self)->datetime:
        return self._start_date
    
    def getEndDate(self)->datetime:
        return self._end_date

    def setName(self,name:str):
        self._name = name

    def getExpiryType(self)->str:
        return self._expiry_type

    def getSliceExpiry(self)->datetime:
        return self._slice_expiry

    def getSliceTime(self)->datetime:
        return self._slice_time

    def setSource(self,source:str):
        self._source = source
    
    def setInstrument(self, instrument):
        self._instrument = instrument
    
    def setStartDate(self, stdate:datetime):
        self._start_date = stdate
    
    def setEndDate(self,edate:datetime):
        self._end_date = edate

    def setExpiryType(self,exp:str):
        self._expiry_type = exp
    
    def setSliceExpiry(self,slice_exp:datetime):
        self._slice_expiry = slice_exp

    def setSliceTime(self, slice_time:datetime):
        self._slice_time = slice_time
