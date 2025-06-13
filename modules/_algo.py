"""
    The Algo class which take care of all the iterations
"""

import pandas as pd
import time
from modules._strategy import Strategy
from modules._blotter import Blotter
from modules._trade import Trade
from modules._backtest import Backtest
from modules._logger import logger,get_exception_line_no
from modules._portfolio import Portfolio
from ._historical_data import HistoricalData
from .global_variables import params

logger = logger.getLogger('algo')

class Algo():
    # Assumptions:  
    #   1. The Blotter is attached to the Algo
    # 
    #   3. The Strategy is attached to the Algo
    #  
    #   5. The Blotter and the Strategy share the same Portfolio
    #   6. Algo works only on market data inside Strategy right now we can expnad to live data later on.
    #   7. Currently Algo is designed to run for a single trading day

    def __init__(self, time_window, strategy:Strategy, blotter:Blotter):
        self._time_window = time_window
        self._strategy = strategy
        self._blotter = blotter
        


    def driver(self,backtest:Backtest, portfolio:Portfolio, hist_data:HistoricalData):
        """
            The main driver method which will run over the time windows provided.
        """
    
        # print(self._time_window)
        for t in self._time_window:
            #Generate slice data
            mkt_data = hist_data.getSlice(t)
            logger.debug(f'at time {t} slice expiry is {mkt_data.getSliceExpiry()}')
            try:
                portfolio_value = portfolio.get_portfolio_value()
                # check whether its a unwind time
                if self._strategy.is_unwind_time(qtime = t,active=True): # TODO: need to update with time interval as well
                    trade_list=[]
                    trade_list = self._strategy.generate_unwind_strategy(qtime=t,portfolio=portfolio,mkt_data=mkt_data)
                    if len(trade_list) !=0 : #if non empty trade list generated
                        for trade in trade_list:
                            trade.execute()
                        if not params['DISABLE_BLOTTER_UPDATE']:
                            self._blotter.add(trade_list=trade_list, trade_time=t)
                        portfolio_value = portfolio.update(trade_list=trade_list, trade_time=t,mkt_data=mkt_data)
                        logger.info(f'Portfolio delta post unwind {portfolio.get_portfolio_delta(qtime=t, mkt_data=mkt_data)}')
                        logger.info(f'portfolio cash post unwind {portfolio.getCash()}')
                        sum = portfolio.getDF().iloc[1:]['position'].sum()
                        logger.info(f'Portfolio total positions post unwind {sum}')
                        if params['SIMPLE_UNWIND']:
                            # removing all other positions except the cash position
                            portfolio._portfolio_df.drop(portfolio._portfolio_df.index[1:],inplace=True)
                            logger.debug(portfolio._portfolio_df)
                        else:
                            # TODO: code logic for complex unwind
                            pass

                        self._strategy._unwind_taken_place = True
                    else:

                        logger.warning('unwind list is empty')

                    # TODO: add logic for last 45mins of trading on expiry day
                    if self._strategy.unwind_taken_place():
                        # TODO: should we remove here the instruments whose position is zero?
                        #       or we should wait till eod to remove those?
                        logger.info('loading next expiry data after unwind')
                        # load the next expiry after unwind
                        # TODO: tried the following 2 lines of code to implement this logic but getting error while slicing
                        #       need to check later because this will be more efficient code
                        # hist_data.setExpiryType('second_weekly') # TODO: should try to make expiry_type selection more generic
                        # hist_data.load_market_data()
                        # mkt_data = hist_data.getSlice(t)

                        hist_data = HistoricalData(source=hist_data.getSource(),
                                                   name=hist_data.getName(),
                                                   underlying_instrument=hist_data.getInstrument(),
                                                   start_date=hist_data.getStartDate(),
                                                   end_date=hist_data.getEndDate(),
                                                   expiry_type='second_weekly') # TODO: should try to make this more generic
                        hist_data.load_market_data()
                        mkt_data = hist_data.getSlice(t)
                        logger.info('next expiry data loaded after unwind')

                if self._strategy.is_trading_time(qtime=t,active=True):
                    trade_list=[]
                    trade_list = self._strategy.generate_trade_strategy(t,mkt_data=mkt_data)
                    if len(trade_list) !=0 : #if non empty trade list generated
                        for trade in trade_list:
                            trade.execute()
                        if not params['DISABLE_BLOTTER_UPDATE']:
                            self._blotter.add(trade_list=trade_list, trade_time=t)
                        portfolio_value = portfolio.update(trade_list=trade_list, trade_time=t,mkt_data=mkt_data)
                        logger.info(f'Portfolio Updated with {portfolio_value}')
                    logger.info('trading strategy generated')

                if self._strategy.is_hedging_time(qtime=t,active=True):
                    # TODO: at a time when both trade and hedge happens trade list may get inserted twice because of +=
                    # trade_list += self._strategy.generate_hedge_strategy(trade_time=t,portfolio=portfolio,mkt_data=mkt_data)
                    trade_list=[]
                    trade_list = self._strategy.generate_hedge_strategy(trade_time=t,portfolio=portfolio,mkt_data=mkt_data)
                    if len(trade_list) !=0 : #if non empty trade list generated
                        for trade in trade_list:
                            trade.execute()
                        if not params['DISABLE_BLOTTER_UPDATE']:
                            self._blotter.add(trade_list=trade_list, trade_time=t)
                        portfolio_value = portfolio.update(trade_list=trade_list, trade_time=t,mkt_data=mkt_data)
                        logger.info(f'Portfolio Updated with {portfolio_value}')
                    logger.info(f'Trade Strategy List {trade_list} being sent')

                backtest.update(values=(t,portfolio_value))

            except Exception as ex:
                logger.critical(f'error while executing time step {t} at lineno={get_exception_line_no()} # {ex}')
        
        self._blotter.serialize(start_time=self._time_window[0],end_time=self._time_window[len(self._time_window)-1])
        # print(backtest._backtest_array)

        # print backtest
        backtest.save(backtestdatetime=self._time_window[0])
