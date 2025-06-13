'''
    The trade object
'''

import datetime
from .global_variables import params

# from modules._utils import get_hft_logger
from modules._logger import logger

# logger = get_hft_logger('trade')
logger = logger.getLogger('trade')

class Trade():
    def __init__(self, 
                 instr_id:int,
                 trade_price:float, 
                 trade_time:datetime, 
                 pos:float, 
                 trader:int=0, # TODO:
                 portfolio:int=0,
                 quote_type:str='mid'):
        '''
        Args:
            a (int): The first integer to be added.
            b (int): The second integer to be added.

        Returns:
            int: The sum of a and b.

        '''
        # TODO: currently working with only portfolio

        # Assumption: keeping only the instrument_id (ExchId) since the instrument class can be heavy
        # TODO: Question: do we need to keep the Intrument object here or only the instrument id will suffice?
        self._instrument_id = instr_id
        self._price = trade_price # traded_price
        self._time = trade_time
        self._position = pos # this position is assumed to be (+)ve or (-)ve indicating buy and sell
        self._trader_id = trader
        self._portfolio_id = portfolio
        self._cash = 0 #Cash generated in the trade
        self._quote_type  = quote_type # which side of the market did we trade

        # trade id is assigned by the blotter after its successfully executed
        self._trade_id = 0


    def __repr__(self):
        return f'ID :{self.getInstrumentId()} Position: {self.getPosition()} Price: {self.getPrice()} Cash:{self.getCash()}'

    def execute(self):
        '''
        # TODO:
        # decide to execute or skip the trade depending on the skip logic
        # decide to trade on bid or ask depending on mkt_maker flag
        # execute module be called after the trade list has ben generated

        Parameters:
        apply_commission: -> whether to apply commission or not. True->apply and False->do not apply
        '''
        self.setCash(self.getPosition()*self.getPrice())

        if params['TXN_COST_FLAG'] :
            self.updateCash(params['TXN_COST']) #need to update with commission amount
        msg = f'trade executed with instrument id: {self._instrument_id} at time {self._time} , position {self._position}'
        logger.info(msg)

        # TODO: sending hard coded true need implement later
        return True

    def getCash(self):
        return self._cash
    
    def setCash(self,cash_amount):
        self._cash = cash_amount

    def updateCash(self,update_amount):
        self.setCash(self.getCash()+update_amount)

    def getQuoteType(self):
        return self._quote_type
    
    def setQuoteType(self,quote_type):
        self._quote_type = quote_type

    def set_trade_id(self, id:int):
        self._trade_id = id

    # def get_value(self):
    #     return self._value
    
    def decompose(self):
        return self._instrument_id, self._price, self._time, self._position, self._trader_id, self._portfolio_id

    def getTradeId(self,):
        return self._trade_id

    def getInstrumentId(self):
        return self._instrument_id

    def getPrice(self):
        # the price at which trade is executed
        return self._price
    
    def getTime(self):
        # the trade execution time
        return self._time

    def getPosition(self):
        # the trade position 
        return self._position

    def getTraderId(self):
        # the trader id
        return self._trader_id

    def getPortfolioId(self):
        # the portfolio id
        return self._portfolio_id
    
