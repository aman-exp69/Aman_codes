import numpy as np
from datetime import datetime
import copy
import os,sys
from modules._instrument import Instrument,get_atm_options,get_otm_options
from modules._instrument import Options
from modules._trade import Trade
from modules._portfolio import Portfolio
from modules._historical_data import HistoricalData
from modules._instrument import get_synthetic_futures
from .global_variables import params
# from modules._utils import get_hft_logger
# from modules._logger import get_hft_logger
from modules._logger import logger,get_exception_line_no



OPTION_TYPE_CALL='CE'
OPTION_TYPE_PUT='PE'
DEBUG = params['DEBUG']

# logger = get_hft_logger('strategy')
logger = logger.getLogger('strategy')

class Strategy():
    def __init__(self,
                 underlying_instrument:str, # TODO: need to verify its validity
                 strategy_type:str='condor', 
                 param_list=[5,5,2,45,1,1],
                 time_interval_list:list=[]) -> None:

        self._strategy_type = strategy_type
        self._param_list = param_list
        
        # paramlist = [percentage of otm, trade interval, hedge interval, unwind time, unit_qty, is_mkt_maker]
        self._percentage_otm = param_list[0] # percentage of otm
        self._trade_interval = param_list[1] # trade interval
        self._hedge_interval = param_list[2] # hedge interval
        self._unwind_time = param_list[3] # unwind time
        self._unit_size = param_list[4] # unit size
        self._is_market_maker = True if param_list[5] else False # is_mkt_maker 1=>market_maker, 0=>not_mkt_maker
        self._underlying_instrument = underlying_instrument
        self._time_interval_list = time_interval_list

        # Assumption: TODO:  if not market_maker then the corresponding code is not coded yet

        # This is to keep track of the quantity if any. this may occur 
        self._unfilled_quantity = 0

        # the time step counter which must be incremented at each to
        # is_trading_time, is_hedging_time and is_unwind_time
        self._time_interval_cntr = 0
        
        # following attributes are only operated within is_trading_time()
        self._take_trade_at_first_time = True # true when trade should be taken at the first time interval
        self._trade_skip = self._trade_interval - 1 # how many time steps to skip before taking trade
        self._trade_skip_cntr = 0 # to keep track of how many time steps to skip before taking the next trade
      
        # following attributes are only operated within is_trading_time()
        self._hedge_skip = self._hedge_interval - 1 # how many time steps to skip before taking trade
        self._hedge_skip_cntr = 0 # to keep track of how many time steps to skip before taking the next trade

        # go and check whether its unwind time till actual unwind time reaches.
        # when actual unwind time arrives make self._unwind_taken_place = True
        self._unwind_taken_place = False
        self._unwind_day = True


    def unwind_taken_place(self)->bool:
        '''
        Returns True if unwind has already taken palce else false
        '''
        return self._unwind_taken_place

    def _is_it_time(self, skip_counter:int, skip_count:int):
        '''
        This function should only be called from other functions dealing with time interval.
        Because this function updates the "self._time_interval_cntr" and keep track of the time interval

        Parameters:
        skip_counter (int): the counter which is keeping track of the skip
        skip_count (int): how many time steps to skip

        Reurns:
        is_it_time (bool): True means time to take action else False means time to skip
        
        '''
        # check whether skip counter is 0
        is_it_time = False
        if skip_counter == 0:
            # when skip counter is 0 take the trade
            is_it_time = True
            # update the skip counter to actual skip trade count
            skip_counter = skip_count
        else:
            # reduce the counter
            skip_counter -= 1

        # increate the time interval counter
        self._time_interval_cntr += 1

        return (is_it_time, skip_counter)


    # new logic
    def is_trading_time(self,qtime:datetime,active:bool)->bool:
        '''
        Returns whether its trading time and keeps track of next trading time

        Parameters:
        qtime (datetime): the query date time
        active (bool): True-> actively querying to take trade and update interval, else False-> querying present state no update

        Returns:
        take_trade (bool): True-> take trade, False->dont take trade
        '''
        # TODO: this logic needs to be updated since each time it is called 
        # time_interval_cntr is updated. thats not correct

        take_trade = False
        if active:
            if self._take_trade_at_first_time & (self._time_interval_cntr == 0):
                # since we have to take trade for the first time, sending count as 0
                # set the trade skip counter
                take_trade,self._trade_skip_cntr = self._is_it_time(skip_counter=0,skip_count=self._trade_skip)
                # after taking the first trade, take the next hedge after specific time
                self._hedge_skip_cntr = self._hedge_skip + 1
            else:
                take_trade,self._trade_skip_cntr = self._is_it_time(skip_counter=self._trade_skip_cntr,
                                                                    skip_count=self._trade_skip)
        else:
            take_trade = True if (self._trade_skip_cntr == 0) else False

        return take_trade


    def is_unwind_time(self,qtime:datetime,active:bool):
        '''
        Returns whether the current time is a unwind time and
        '''
        logger.debug(f'within is_unwind_time() with qtime={qtime}')
        is_time = False
        if (not self._unwind_taken_place) and (self._unwind_day):
            # TODO: we should save the trading endtime for current day and save to refer whenever required
            str_end_day = qtime.strftime('%Y-%m-%d') + ' ' + params['TRADING_END_TIME']
            trading_end_time = datetime.strptime(str_end_day,'%Y-%m-%d %H:%M:%S')

            # get the time delta
            time_delta = ((trading_end_time - qtime).total_seconds())/60
            # if time delta is less than unwind time
            if time_delta < params['UNWIND_TIME']:
                is_time = True
                # if unwind is happening at this time step, make sure that it wont happen in next time steps
                # should be updated only after complete unwind
                # self._unwind_taken_place = True
            
            if is_time:
                logger.info('it is unwind time')
            else:
                logger.info('not an unwind time')
        else:
            logger.info('unwind already taken place')

        return is_time


    def is_hedging_time(self,qtime:datetime,active:bool)->bool:
        # TODO: to be implemented
        '''
        Returns whether the current time is hedging time and keeps track of next hedging time

        Parameters:
        taking_hedge (bool): when True the counters will get updated otherwise it will return same value

        '''
        # TODO: this logic needs to be updated since each time it is called 
        # time_interval_cntr is updated. thats not correct.

        take_hedge = False
        # checking whether delta_hedging is turned on 
        if params['DELTA_HEDGE']:
            if active:
                take_hedge,self._hedge_skip_cntr = self._is_it_time(skip_counter=self._hedge_skip_cntr,
                                                                    skip_count=self._hedge_skip)
            else:
                take_hedge = True if (self._hedge_skip_cntr == 0) else False
            
        return take_hedge


    def unwind_to_reduce_txn_cost(self, qtime, portfolio, mkt_data:HistoricalData):
        '''
        EIS unwind logic: TODO: tobe implemented
        '''
        logger.info('*'*100)
        logger.info(f'{qtime}# initiating unwind_to_reduce_txn_cost')
        logger.info('-'*100)
        trade_list = list()
        try:
            obj_instr_list = portfolio.get_unwind_list(qtime)
            logger.info(f'found {len(obj_instr_list)} intruments to unwind')
            # get the current spot
            current_spot = mkt_data.get_spot_v2(qtime)
            for instr,qty in obj_instr_list:
                if qty == 0:
                    # TODO: Q: if quantity is 0, what to do?
                    logger.warning(f'instrument quanity is 0 for id={instr.getId()}')
                    continue
                
                # TODO: need to complete
                # if instr.getOptionType() == 'PE':
                #     if instr.getStrike() < current_spot:
                # TODO: we can call get_quote_by_id() instead of get_quote()
                price, _ = instr.get_quote(t=qtime,q_type='ask',mkt_data=mkt_data)
                trade = Trade(instr_id=instr.getId(),trade_price=price,trade_time=qtime,pos=qty)
                trade_list.append(trade)

        except Exception as ex:
            logger.critical(ex)
            # raise ex

        return trade_list


    def generate_unwind_strategy(self, qtime, portfolio:Portfolio, mkt_data):
        '''
        parameters : qtime, portfolio
        returns : trade list
        '''
        logger.info('*'*100)
        logger.info(f'{qtime}# initiating unwind strategy')
        logger.info('-'*100)
        trade_list = list()
        try:
            expiry_list = []
            unwind_list = portfolio.get_unwind_list(qtime)
            logger.info(f'found {len(unwind_list)} intruments to unwind')
            for instr,qty in unwind_list:
                if qty == 0:
                    # TODO: Q: if quantity is 0, what to do?
                    logger.warning(f'instrument quanity is 0 for id={instr.getId()}')
                    continue
                expiry_list.append(instr.getExpiry())
                # TODO: Q: will it always use the 'ask' price while unwind?
                # TODO: we can call get_quote_by_id() instead of get_quote()
                try:
                    price, _ = instr.get_quote_by_id(t=qtime,instrument_id=instr.getId(),q_type='ask',mkt_data=mkt_data)
                    trade = Trade(instr_id=instr.getId(),trade_price=price,trade_time=qtime,pos=-qty)
                    trade_list.append(trade)
                except Exception as e:
                    logger.critical(f'Error in : generate_unwind_strategy() at line : {get_exception_line_no()} with error : {e}')

            # logic to determine whether there is any expiry today
            # TODO: assuming for now that an empty list means no expiry for today. 
            #       later we can implement the above logic. 
            #       should be moved to function
            if len(unwind_list) == 0:
                self._unwind_day = False
            else:
                for idx in range(len(expiry_list)):
                    if isinstance(expiry_list[idx], np.datetime64):
                        expiry_list[idx] = datetime.utcfromtimestamp(expiry_list[idx].astype('O')/1e9)

                min_expiry = min(expiry_list, key=lambda d: d.date())
                day_delta = min_expiry - qtime
                # if today is not an expiry day, from next time step onwards will not check for unwind
                if day_delta.days == 0:
                    self._unwind_day = True
                else:
                    self._unwind_day = False

            return trade_list
        except Exception as ex:
            logger.critical(f'exception within generate_unwind_strategy() at line={get_exception_line_no()}: {ex}')
            raise ex        

    def create_instrument_name(self, inst_type, instrument, expiry, strike)->str:
        return instrument + '_' + str(strike) + '_' + inst_type + '_' + expiry.strftime('%Y-%m-%d %H:%M:%S')

    def create_option_trade(self, 
                            obj_opt:Options, 
                            trade_time:datetime,
                            trade_qty:float,
                            mkt_data_slice:HistoricalData)->Trade:
        '''
        Parameters:
            obj_opt: Option object, 
            instru_name: intrument name , 
            trade_time: trading time,
            trade_type: trade type (sell/buy)
        '''
        # TODO: need to change the logic here to cater the requirement for changing the 
        #       strategy to be market maker at trade time and non-market maker during hedge.
        #       there should be three options 'mid', 'mkt_maker', 'customer'.
        #           mid         mkt_maker       customer
        #           ++++++++++++++++++++++++++++++++++++
        #           +mid        +bid            +ask
        #           +mid        -ask            -bid

        trade = None
        if trade_qty < 0:
            # TODO: we can call get_quote_by_id() instead of get_quote()
            askprice, _ = obj_opt.get_quote(trade_time, 
                                            q_type='ask',
                                            mkt_data=mkt_data_slice)

            trade = Trade(instr_id=obj_opt.getId(),
                          trade_price=askprice,
                          trade_time=trade_time,
                          pos=trade_qty,
                          quote_type='ask'
                         )
            # print(f'{trade_time}# creating atm put sell with id={obj_opt.getId()} and price={askprice}')

        elif trade_qty > 0:
            # TODO: we can call get_quote_by_id() instead of get_quote()
            bidprice, _ = obj_opt.get_quote(trade_time,
                                            q_type='bid',
                                            mkt_data=mkt_data_slice)

            # TODO: do we need to check the expiration also?
            trade = Trade(instr_id=obj_opt.getId(),
                        trade_price=bidprice,
                        trade_time=trade_time,
                        pos=trade_qty,
                        quote_type='bid'
                        )
            # print(f'{trade_time}# creating atm put sell with id={obj_opt.getId()} and price={bidprice}')
        else:
            logger.warning(f'trade quantity is 0, returning trade as none.')
        
        return trade


    def skip_trade(self, skip_prob=0.05):
        # TODO: this method to be used as skip strategy
        # skip probability, sets flag = True if above a certain value
        # currently its assuming default 5% skip
        # skip_prob = 0.05
        # skip_flag = np.random.random() < skip_prob

        # TODO: now sending false but later can use the above

        return False
    

    def generate_trade_strategy(self, trade_time:datetime,mkt_data:HistoricalData)->list:
        """
        Parameter:
            trade_time
        Returns: trade_list

        Logic:
            Our strategy is a condor strategy, sell atm call and put, buy otm call and put
            1. Extract exercise date from historical data - for each exercise date do the following:
                2. create 4 instruments
                    calculate 3 strikes
                3. generate 4 trades
            4. return trade list
            5. we have to code the skip logic later by adding the skip_prob
        """

        # equity = Instrument(type_of_instrument='equity', param_list=["default", "BANKNIFTY"])
        # # spot = equity.get_spot(t=trade_time, mkt_data=self._mkt_data.getData())
        # spot = equity.get_spot(t=trade_time, mkt_data=self._hist_data)
        


        logger.info('*'*100)
        logger.info(f'{trade_time}# generating trade strategy')
        logger.info('-'*100)

        try:
            trade_list = list()
            expiry = mkt_data.get_slice_expiry() #
            #  use a single expiry
            try:
                atm_call, atm_put = get_atm_options(t=trade_time, 
                                                        underlying=self._underlying_instrument,
                                                        expiry=expiry,
                                                        mkt_data=mkt_data)
            except Exception as ex:
                # if no options found then continue
                logger.critical(f'{ex} at {get_exception_line_no()} with # {expiry}, {self._underlying_instrument}, {trade_time}')
                raise ex

            try:
                otm_call, otm_put = get_otm_options(t=trade_time, 
                                                        atm_strike=atm_call.getStrike(),
                                                        underlying=self._underlying_instrument,
                                                        expiry=expiry,
                                                        pct=self._percentage_otm,
                                                        mkt_data=mkt_data)
            except Exception as ex:
                # if no options found then continue
                logger.warning(f'{ex} at {get_exception_line_no()} with # {expiry}, {self._underlying_instrument}, {trade_time}')
                raise ex

            # creating an atm call sell trade
            trade_list.append(self.create_option_trade(atm_call,trade_time,-(self._unit_size),mkt_data_slice=mkt_data))
            logger.info(f'{trade_time}# creating atm call sell with id={atm_call.getId()} expiry={atm_call.getExpiry()} strike={atm_call.getStrike()}')

            # creating an atm put sell trade
            trade_list.append(self.create_option_trade(atm_put,trade_time,-(self._unit_size),mkt_data_slice=mkt_data))
            logger.info(f'{trade_time}# creating atm put sell with id={atm_put.getId()} expiry={atm_put.getExpiry()}')

            trade_list.append(self.create_option_trade(otm_call,trade_time,self._unit_size,mkt_data_slice=mkt_data))
            logger.info(f'{trade_time}# creating otm call buy with id={otm_call.getId()} expiry={otm_call.getExpiry()}')

            trade_list.append(self.create_option_trade(otm_put,trade_time,self._unit_size,mkt_data_slice=mkt_data))
            logger.info(f'{trade_time}# creating otm put buy with id={otm_put.getId()} expiry={otm_put.getExpiry()}')
                
            return trade_list
        
        except Exception as ex:
            logger.critical(ex)


    def generate_hedge_strategy(self, trade_time:datetime, portfolio:Portfolio,mkt_data:HistoricalData):
        '''
            This method will generate the hedging strategy at time trade_time

        Steps:
            1.  get the sythetic spot from the equity object
            2.  get the delta at the current trading time for the entire portfolio 
            3.  create synthetic future 
            4.  create a tradelist of synthetic futures
        '''
        # port = portfolio.list_of_all_instruments
        # get delta from each instrument in the portfolio
        logger.info('*'*100)
        logger.info(f'{trade_time} # initiating hedge strategy')
        logger.info('-'*100)
        try:
            trade_list = list()

            delta = portfolio.get_portfolio_delta(qtime=trade_time,mkt_data=mkt_data)
            # TODO: Q: should we use ceil() instead of round()?
            # moreover if we use round() or ceil() will it be logical to add one extra trade?
            delta = round(delta) # rounding up
            logger.info(f'Pre Hedge delta computed as {delta}')

            expiry = mkt_data.getSliceExpiry()
            logger.debug(f'expiry={expiry}')
            try:
                # get_synthetic_futures will return a list with one call and a put
                # so they are replicating an index future with two options one is a call and a put
                call,put = synth_future_list = get_synthetic_futures(underlying=self._underlying_instrument,
                                                        trade_time=trade_time,
                                                        expiry=expiry,
                                                        mkt_data=mkt_data)
                logger.info('got synthetic futures')
            except Exception as ex:
                logger.critical(ex)
                raise ex

            # while going long, buy calls and selling puts 
            if delta: # delta is non zero
                trade_list.append(self.create_option_trade(obj_opt=call,
                                                        # instru_name=ce.getUnderlyingName(),
                                                        trade_time=trade_time,
                                                        # trade_qty=-(delta), TODO: this one or the below??
                                                        trade_qty=delta,
                                                        mkt_data_slice=mkt_data
                                                        ))
                trade_list.append(self.create_option_trade(obj_opt=put,
                                                        # instru_name=pe.getUnderlyingName(),
                                                        trade_time=trade_time,
                                                        # trade_qty=delta, # TODO: this one or the below??
                                                        trade_qty=-(delta),
                                                        mkt_data_slice=mkt_data
                                                        ))
                logger.info(f'{trade_time}# hedging with atm call(id={call.getId()}) buy and put(id={put.getId()}) sell')
            else:
                logger.warning(f'{trade_time}# cannot take hedge since delta is 0')

            
            return trade_list

        except Exception as ex:
            logger.critical(f'exception at line={get_exception_line_no()}, while taking hedge:{ex}')
            raise ex
