from datetime import datetime,timedelta
import os
import sys
import numpy as np
import pandas as pd
from ._instrument import Cash
from ._instrument import Instrument, Options
from ._instrument import get_options_from_id_list,  get_option_from_instrument_id#, : NEW method implemented in portfolio with slicing
from ._trade import Trade
from ._historical_data import HistoricalData
# from modules import Options, Instrument, Cash
from .global_variables import params
from modules._logger import logger,get_exception_line_no
from modules._backtest import Backtest
import copy


# TODO: to be moved to yaml file
CASH_ID = params['CASH_ID']
logger = logger.getLogger('portfolio')


class Portfolio:

    # Constructor
    def __init__(self,
                 portfolio_id = 1, 
                 currency:str = 'INR',
                 owner:str = None,
                 description:str = None,
                 limits:dict = dict(),
                 initial_cash = 0,
                 init_from_file: bool = False,
                 file_name: str = "",
                 time_stamp = None
                 ) -> None:

        self._currency = currency 
        self._owner = owner
        self._description = description
        self._limits = limits # Will populate this later.
        self._latest_timestamp = time_stamp #_trade.getTime()
        self._portfolio_id = portfolio_id
        #TODO : Do I need this??
        self._is_init_cash_added = False
        self._file_name = file_name

        if init_from_file:
            self._portfolio_df = self.load_portfolio_data_from_file(file_name=self._file_name)
        else:
            self._portfolio_df = pd.DataFrame(
                columns=[   "instrument_object",
                            "position",
                            "current_price",
                            #"weighted_avg_price",
                            "value"
                            # "delta",
                            # "trade_id_list",
                            # "mark_to_market"
                            # "realized_pnl",
                            #TODO : add the features required to create instrument : option_type, strike,, expiry
                        ])
            self._portfolio_df.index.name = 'instrument_id'

            initial_cash_dict = {"instrument_object" : Cash(),
                                       "position":initial_cash,
                                       "current_price":1,
                                       "value":initial_cash,
                                      }
            self._portfolio_df.loc[CASH_ID]=initial_cash_dict
            logger.info(f'{self.getCash()} {self.getCurrency()} cash added to portfolio.')
            
    def getDF(self):
        return self._portfolio_df
    
    def update_latest_timestamp(self,t:datetime)->None:
        '''
        This function will update the latest timestamp in the trade.
        '''
        try:
            self._latest_timestamp = t

        except Exception as e:
            logger.warning(f'Error in update_latest_timestamp in line {get_exception_line_no()}, error : {e}')
            raise e


    def is_instrument_id_in_portfolio(self, instrument_id) -> bool:
        ''' 
        This function will check if an instrument id exists in the portfolio df
        '''
        try : 
            ret_val = False
            if instrument_id in self._portfolio_df.index.to_list():
                ret_val = True
            return ret_val
        
        except Exception as e:
            logger.critical(f'Error in is_instrument_id_in_portfolio in line {get_exception_line_no()}, error : {e}')
            raise e
            

    def get_portfolio_option_list(self, )->list:
        '''
        This function gets the list of options from list of ids.

        Returns:
        List of Option objects
        '''
        try:
            id_list = self._portfolio_df.loc[self._portfolio_df.index != CASH_ID].index
            return get_options_from_id_list(id_list)

        except Exception as e:
            logger.critical(f'Error in get_portfolio_option_list in line : {get_exception_line_no()}, error : {e}')
            raise e

    def get_max_expiry_among_instruments(self,mkt_data:HistoricalData)->datetime:

        try:
            # Going to return the ONLY expiry in our portfolio, no other expiries exist
            #id_list = self._portfolio_df.loc[self._portfolio_df.index != CASH_ID].index
            return mkt_data.get_slice_expiry()
            #return self.getDF().iloc[1]['instrument_object'].getExpiry()
        
            #return self.get_time_slice_mkt_data(qtime).get_max_expiry_from_options(id_list=id_list)
        
        except Exception as e:
            logger.critical(f'Error in get_max_expiry_among_instruments in line : {get_exception_line_no()}, error : {e}')
            raise e


    # def get_time_slice_mkt_data(self, qtime:datetime) -> HistoricalData:
    #     '''
    #     Returns a sliced copy of this object. The dataframe is sliced on the time input.

    #     Parameters:
    #     qtime: time at which the data will be sliced.
    #     '''
    #     obj_copy = copy.deepcopy(mkt_data)
    #     obj_copy._data = obj_copy._data.loc[qtime]
    #     return obj_copy
    
    def update(self, trade_time:datetime,  mkt_data : HistoricalData, trade_list:list=list()) -> float:
        """
        This function updates the portfolio_df for each trade.
        parameters: time, tradelist
        return: portfolio value
        """
        # Algo : 
        # - This function will receive a trade_list from the blotter, where trade_list 
        #   is a list of trade objects(trade) and will be updated in portfolio_df.
        # - portfolio_df contains : 'instrument_id','trade_list','portfolio_id','position','last_traded_price',
        #   'current_price','weighted_avg_price','mark_to_market','realized_pnl'
        # - each trade object contains : 'instrument_id','trade_list','portfolio_id','position','last_traded_price',
        # - If a trade happens or trade is populated:
        #       'instrument_id','trade_list','portfolio_id','position','last_traded_price','current_price',
        #       are stored in the portfolio_df
        #       'weighted_avg_price' is calculated and stored in portfolio df
        #       'mark to market' is calculated and stored in portfolio_df.
        #       'realized pnl' is left blank unitl trade stopped.
        #       realized pnl is calculated for the respective instrument,when the position of the instrument is 0.
        # - elif when trade is empty :
        #       update the current price and aslo calculate the pnl.

        try:
            # TODO: Q: Should we update the current price for each instrumnet in the portfolio here ?
            self.update_current_prices_values(utime=trade_time, mkt_data=mkt_data)
            logger.info(f"Updated all the Current price at {trade_time}")
            # Checking if trade list is populated with trade
            if len(trade_list) != 0 : #non empty trade_list
                accumulated_cash = 0

                for trade in trade_list:

                    trade_instr_id = trade.getInstrumentId()
                    trade_position = trade.getPosition()
                    quote_type =  trade.getQuoteType()#'ask' if trade_position < 0 else 'bid'
                    trade_price = trade.getPrice()
                    trade_time = trade.getTime()

                    accumulated_cash += trade.getCash()

                    if (trade_instr_id in self._portfolio_df.index):
                        self.getDF().at[trade_instr_id,"position"] += trade_position
                        # TODO: we can call get_quote_by_id() instead of get_quote()
                        self.getDF().at[trade_instr_id,"current_price"] = self.getDF().at[trade_instr_id,'instrument_object'].get_quote(t=trade_time, q_type=quote_type, mkt_data=mkt_data)[0]
                        self.getDF().at[trade_instr_id,"value"] = self.getDF().at[trade_instr_id,"position"]*self.getDF().at[trade_instr_id,"current_price"] 
                    else:
                        instr_obj = get_option_from_instrument_id(id = trade_instr_id,mkt_data=mkt_data)
                        # TODO: we can call get_quote_by_id() instead of get_quote()
                        curr_price,_ = instr_obj.get_quote(t=trade_time, q_type=quote_type, mkt_data=mkt_data)
                        trade_dict ={  "instrument_object": instr_obj,
                                        "position": trade_position,
                                        'current_price': curr_price,
                                        "value": trade_position*curr_price # TODO: Q: will it be curr_price or trade_price
                                    }
                        self._portfolio_df.loc[trade_instr_id] = trade_dict

                        logger.info(f'New instrument added to Portfolio_df with ID : {trade_instr_id}')
                        
                # update cash when trades executed
                logger.info(f'updating cash by {accumulated_cash} to {self.getCash()}')
                self.getDF().at[params['CASH_ID'],'position'] += ((-1)*accumulated_cash)
                self.getDF().at[params['CASH_ID'],'value'] += ((-1)*accumulated_cash)
                logger.debug(f'portfolio value at time {trade_time} is {self.get_portfolio_value()}')
        
            else: #empty trade_list - only update prices and values
                # self.update_current_prices_values(trade_time)
                logger.info(f'Encountered an empty list but the current price is already updated.')
                # logger.info(f'Current price updated for each instrument_id')

            if params['DEBUG']:
                print(f'\n')
                print('-'*80)
                sum = self._portfolio_df['value'].sum()
                # print(f'trade_instr_id = {trade_instr_id}\ntrade_position = {trade_position}\nquote_type = {quote_type}\ntrade_price = {trade_price}\ntrade_time = {trade_time}')
                print(f"Total portfolio value at time {trade_time} is : {round(sum,2)}")
                print(f'\n')
                print('-'*80)
                print('Portfolio df after each trade')
                print(self)
                print(f'\n')
                print(f'Timestamp : {trade_time}')
                print(self._portfolio_df[['current_price','position','value']])
                # print(self.getPortfolio_df()[['position','current_price', 'value']])
                print(f'\n')

            # self.getDF().at[params['CASH_ID'],'position'] += accumulated_cash
            # self.getDF().at[params['CASH_ID'],'value'] += accumulated_cash

            # if params['DEBUG']:
            #     print(f"Portfolio Cash : {round(self._portfolio_df.at[CASH_ID,'value'],2)}")
            #     print('-'*80)     

            self.update_latest_timestamp(trade_time)
            return self.get_portfolio_value() #returns the value of the portfolio

        except Exception as e:
            logger.critical(f'Error in update (portfolio) in line : {get_exception_line_no()}, error : {e}')
            raise e


    def update_cash(self, amount):
        '''
        updates the cash position and value in the portfolio
        '''
        try:
            if amount != 0 : 
                # its assumed that there will be only one row for cash with instrumentid as CASH_ID
                if params['DEBUG']:
                    print('Current Cash is {0}'.format(self._portfolio_df.loc[CASH_ID]['position']))
                    print('Additional Cash is {}'.format(amount))
                self.getDF().at[params['CASH_ID'],'position'] += ((-1)*amount)
                self.getDF().at[params['CASH_ID'],'value'] += ((-1)*amount)
                if params['DEBUG']:
                    print('Updated cash to {0}'.format(self._portfolio_df.loc[CASH_ID]['position']))
                #logger.info(f'updated cash to {self._portfolio_df.loc[CASH_ID]['position']}')

        except Exception as e:
            logger.warning(f'Error in update_cash at line {get_exception_line_no()}, error : {e}')
            raise e


    def update_current_prices_values(self,utime:datetime,mkt_data:HistoricalData):
        '''
        Updates the current prices and corresponding values of the instruments in the portfolio
        '''
        try:
            cash_val_change = 0
            for id in self._portfolio_df.index:
                if id == CASH_ID:
                    # TODO: need to change CASH_ID
                    continue
                q_type = 'ask' if self._portfolio_df.at[id,'position'] < 0 else 'bid'
                # TODO: we can call get_quote_by_id() instead of get_quote()
                # curr_price,_ = self._portfolio_df.at[id,'instrument_object'].get_quote(t=utime, q_type=q_type, mkt_data=mkt_data)
                try:
                    curr_price,_ = self._portfolio_df.at[id,'instrument_object'].get_quote_by_id(t=utime, instrument_id=id, q_type=q_type, mkt_data=mkt_data)
                    if curr_price != None:
                        if curr_price > 0:
                            self._portfolio_df.at[id,'current_price'] = curr_price
                            self._portfolio_df.at[id,'value'] = curr_price * self._portfolio_df.at[id,'position']
                            logger.debug(f'Current price and value updated for the instrument_id : {id} and with price : {curr_price}')
                except Exception as e:
                    logger.critical(f'Error in : update_current_prices_values in line : {get_exception_line_no()} with error : {e}')

        except Exception as e:
            logger.critical(f'Error in : update_current_prices_values in line : {get_exception_line_no()} with error : {e}')
            raise e

    def load_portfolio_data_from_file(self, file_name:str):
        '''
        This function will load the existing portfolio_df.
        '''
        try:
            file = os.path.join(params['OBJ_STORE'],file_name)
            df = pd.read_csv(file)
            return df

        except Exception as e:
            print("Couldn't get the stored portfolio_df")
            logger.critical(f'Error in load_portfolio_data_from_file in line {get_exception_line_no()} with error : {e}')
            raise e


    def serialize(self, file_name:str):
        '''
        This funciton will save the portfolio_df in a specific directory.
        '''
        try:
            dt_now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            file_path = os.path.join(params['OBJ_STORE'],file_name)
            os.makedirs(params['OBJ_STORE'], exist_ok=True)
            with open(file_path, "w"):
                pass
            self._portfolio_df.to_csv(file_path)
            logger.info(f'Portfolio_df stored in file={file_name} at time {dt_now}')

        except Exception as e:
            logger.warning(f'Error in serialize in line : {get_exception_line_no()}, with error : {e}')
            raise e


    def get_portfolio_delta(self, qtime, mkt_data):
        '''
        This function returns the portfolio delta at a particular given time

        Parameters:
        qtime : time 

        pseudo code:
            get the instrument object by id if not cash
            from the instrument object call calculate_delta()
        '''
        # TODO: current assumption is that, this portfolio will contain only options

        # TODO: Q: Do we need to send time as well while getting portfolio_delta? 
        #          i.e we should get_delta at a particular time?
        #          OR
        #          or anytime this method is called?

        def compute_delta(obj,mkt_data):
            # row_delta = self.getDF().at[idx,'instrument_object'].calculate_delta_by_id(instrument_id=idx,t=qtime,q_type='mid',mkt_data=mkt_data)*self.getDF().at[idx,'position']
            pos = self.getDF().at[obj.getId(),'position']
            if pos==0:
                row_delta = 0
            else:
                try:
                    qtype = 'bid' if pos > 0 else 'ask'
                    row_delta = obj.calculate_delta_by_id(instrument_id=obj.getId(),t=qtime,q_type=qtype,mkt_data=mkt_data)*(-1)*pos
                except Exception as e:
                    logger.critical(f'error while computing delta for instrument_id={obj.getId()}. setting delta for this id to 0# {e}')
                    row_delta = 0
            logger.debug(f'delta for instrument_id={obj.getId()} is computed as {row_delta} with position={pos}')
            return row_delta

        try:
            # self._portfolio_df.loc[self._portfolio_df.index[1]].index
            # delta_sum = self._portfolio_df.index.to_series().apply(compute_delta, args=(mkt_data,)).sum()
            delta_sum = self._portfolio_df.iloc[1:]['instrument_object'].apply(compute_delta, args=(mkt_data,)).sum()
            #logger.info('Delta computed as : {0} at {1}'.format(delta_sum,qtime))
            return delta_sum
        except Exception as e:
            logger.critical(f'Error in get_portfolio_delta() in line : {get_exception_line_no()}, error : {e}')
            # raise e


    # This function will return thne unwind list
    def get_unwind_list(self, timestep:datetime)->list:
        ''' 
        This function will return the options that are to expire from the portfolio_df
        '''
        try:
            unwind_list = []
            valid_instrument_list = self._portfolio_df.index.to_list()[1:]
            for id in valid_instrument_list:
                opt,pos = self._portfolio_df.loc[id,['instrument_object','position']]
                # opt = get_option_from_instrument_id(id,mkt_data=self.get_time_slice_mkt_data(timestep))
                if self.is_expiry_time(at_time_t = timestep, expiry= opt.getExpiry(), buffer = 45): # WE can change the buffer
                    unwind_list.append((opt,pos))
            logger.debug(f'length of unwind_list is {len(unwind_list)}')
            return unwind_list
        
        except Exception as e:
            logger.warning(f'Error in get_unwind_list() in line : {get_exception_line_no()}, error : {e}')
            raise e


    # calculate the time left to expiry
    def is_expiry_time(self, at_time_t:datetime, expiry, buffer)->bool: 
        '''
        Parameters
        ----------
        at_time_t: current timestamp
        expiry : expiry time of the option

        Returns
        -------
        True : if expiry time < buffer else False
        '''
        try:
            if isinstance(at_time_t, np.datetime64):
                at_time_t = datetime.utcfromtimestamp(at_time_t.astype('O')/1e9)

            # check expiry datatype
            if isinstance(expiry, np.datetime64):
                expiry = datetime.utcfromtimestamp(expiry.astype('O')/1e9)

            time_delta = ((expiry - at_time_t).total_seconds())/60
            is_time = False
            # if time delta is less than unwind time
            logger.debug(f'time_delta={time_delta},at_time={at_time_t},expiry{expiry}')
            if time_delta < params['UNWIND_TIME']:
                is_time = True

            if is_time:
                logger.info(f'{at_time_t}# it is unwind time')
            else:
                logger.info(f'{at_time_t}# not a unwind time')

            return is_time

        except Exception as e:
            logger.warning(f'Error in is_expiry_time() in line : {get_exception_line_no()}, error : {e}')
            raise e


    def get_portfolio_value(self, update=False, utime=0):
        '''
        This method will return the present portfolio value
        '''
        # TODO: if update :
        #           update the portfolio at the utime
        #       return the portfolio value
        try:
            if update:
                self.update_current_prices_values(utime=utime)
            portfolio_val = self._portfolio_df['value'].sum()
            return portfolio_val

        except Exception as e:
            logger.critical(f'Error in get_portfolio_value() in line : {get_exception_line_no()}, error : {e}')
            raise e


    def view(self) -> None:
        """
        This function is used to view the portfolio_df at the end of each trade cycle.
        This function is only Test specific (Debugging)
        """
        try:
            print("\n", "*" * 30)
            print("portfolio state")
            print("-" * len("portfolio state"))
            print(self)
            print("*" * 30)

        except Exception as e:
            logger.warning(f'Error in : view : {e}')


    # Dunder methods __str__ and __repr__

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f' \n Portfolio Value : {self.get_portfolio_value()} Cash : {self.getCash()} \n \
                Timestamp : {self.get_latest_timestamp()}\n\n'+self.getPortfolio_df()[['position','current_price', 'value']].__repr__()

  
    # All Getter methods

    def getCash(self):
        return self.getDF().at[params['CASH_ID'],'position']
     
    
    def getCurrency(self)->str:
        '''
        This function will get the currency from the constructor.
        '''
        return self._currency

    def getOwner(self)->str:
        '''
        This function will get the owner from the constructor.
        '''
        return self._owner

    def getDescription(self)->str:
        '''
        This function will get the description from the constructor.
        '''
        return self._description

    def getLimits(self)->dict:
        '''
        This function will get the limits from the constructor.
        '''
        return self._limits
    
    def getPortfolio_id(self)->int:
        '''
        This function will get the limits from the constructor.
        '''
        return self._portfolio_id
    
    def get_latest_timestamp(self):
        '''
        This function will get the limits from the constructor.
        '''
        return self._latest_timestamp
    
    def getPortfolio_df(self):
        return self._portfolio_df
    



# All setter methods

    def setCash(self, amount):
        # TODO: 
        pass

    def setCurrency(self,currency)->None:
        '''
        This function will set the currency.
        '''
        self._currency = currency

    def setOwner(self,owner)->None:
        '''
        This function will set the owner.
        '''
        self._owner = owner    

    def setDescription(self,description)->None:
        '''
        This function will set the description.
        '''
        self._description = description

    def setLimits(self,limits)->None:
        '''
        This function will set the limits.
        '''
        self._limits = limits