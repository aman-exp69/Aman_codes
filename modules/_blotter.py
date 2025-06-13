
import os
import pandas as pd
from datetime import datetime

from ._trade import Trade
from ._portfolio import Portfolio
from ._historical_data import HistoricalData
from ._trade import Trade
from .global_variables import params
from modules._logger import logger

logger = logger.getLogger('blotter')

import numpy as np

# TODO: need to increment as string (e.g.'000001' -> '000002')
# SEQ_INIT=params['LAST_SEQ']

class Blotter():
    """
        This class will keep track of all the trades happening
    """
    def __init__(self):
        # initialize the dataframe where the trades will be stored
        self._blotter_df = pd.DataFrame(columns=['trade_id','time','instrument_id','position','price'])
        # store the instrument object here
        # inititize the sequence for the trades
        self.id_sequence = params['LAST_SEQ']

    def __repr__(self):
        repr_str = 'Blotter \n'
        rep_str = 'Unique Intrument IDs : {0}\n'.format(len(self._blotter_df['instrument_id'].unique()))
        rep_str += f'Number of Trades : {self._blotter_df.shape[0]}\n'
        repr_str += self._blotter_df.__repr__()
        return repr_str

    def get_next_sequence(self)->int:
        # seq = str(self.id_sequence)
        last_seq = self.id_sequence
        self.id_sequence += 1
        return last_seq

    def add(self,
            trade_list:list(), 
            trade_time):
        '''
        This method will add

        Parameters:
            trade_list: list of trade objects
            trade_time: time at which trade is happening. this is important even when no trade 
                        takes place because we have to update the prices in the portfolio.
        '''
       
        logger.info(f'blotter update with {trade_list}')
        
        #portfolio_trade_list = []
        end_date_updated = False
        for trade in trade_list:
            logger.info(f'blotter:dealing with {trade}')
            
            instrument_id, price, time, position, trader_id, portfolio_id = trade.decompose()
            dict = {'instrument_id':instrument_id,
                    'time':trade_time,
                    'position':position,
                    'trade_id':self.get_next_sequence(),
                    'price':price
                    }

            # trade.set_trade_id(dict['trade_id'][0])
            # self.data = self.data.append(pd.DataFrame([instrument_id,time,position,trader_id],index=['instrument_id','time','position','trade_id']).T)
            # self.data = self.data.append(dict,ignore_index=True)
            # self._blotter_df = self._blotter_df.append(pd.DataFrame(dict), ignore_index=True)
            self._blotter_df.loc[dict['trade_id']] = dict

        if params['DEBUG']:
            print('*'*100)
            print('blotter state')
            print('-'*100)
            print(self)
            print('-'*100)


    def serialize(self, start_time:datetime, end_time:datetime):
        '''
        This funciton will save the blotter_df in a specific directory.
        '''
        try:
            if not os.path.exists(params['BLOTTER_STORE']):
                os.makedirs(params['BLOTTER_STORE'])
            st_time = start_time.strftime('%Y%m%d%H%M%S')
            en_time = end_time.strftime('%Y%m%d%H%M%S')
            file_name = f'blotter_{st_time}_{en_time}.csv'
            file_path = os.path.join(params['BLOTTER_STORE'],file_name)
            self._blotter_df.to_csv(file_path, index = False)
        
        except Exception as e:
            logger.critical(f'Error in : serialize : {e}')

    
    def view(self):
        print('*'*30)
        print('blotter state')
        print('-'*len('blotter state'))
        print('Unique Intrument IDs : {0}\n'.format(len(self._blotter_df['instrument_id'].unique())))
        print(f'Number of Trades : {self._blotter_df.shape[0]}\n')
        
        print(self)
        print('*'*30)
