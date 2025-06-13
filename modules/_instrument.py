import warnings
warnings.filterwarnings("ignore")

import math
import numpy as np
import pandas as pd
import py_vollib_vectorized as pv
import pandas_market_calendars as mcal

from yaml import load, Loader
from datetime import datetime,timedelta
from .global_variables import params
from scipy import optimize
from modules._historical_data import HistoricalData, NoOptionsFound
from abc import ABC, abstractclassmethod
from modules._black_scholes import implied_volatility_options, black_scholes
from modules._logger import logger

logger = logger.getLogger('instrument')



def get_nearest_strike_premium(t:datetime, strike:int, mkt_data:HistoricalData) -> tuple:
    """
    t: at time instant t
    strike: strike of the option
    mkt_data: market data instance

    returns: tuple (bidprice, bidqty, askprice, askqty)
    """
    # get the price and qty of the nearest strike option available on that day
    bidprice, bidqty, askprice, askqty = mkt_data.get_nearest_strike_premium(t=t, strike=strike)

    return bidprice, bidqty, askprice, askqty

def get_option_from_instrument_id(id:int,  mkt_data:HistoricalData) -> object:
    '''
    Get option or cash instance given an unique identifier, underlying name, dividend and HistoricalData object.

    Parameters
    ----------
    id : Unique Identifier of an Option.
    underlying_name: Name of the underlying 
    mkt_data: market data instance (HistoricalData object)
    dividend: rate of dividend

    Returns
    -------
    object of Option Class or Cash Class
    '''

    
    if id != params["CASH_ID"]:
        # strikes expiry and option_type
        # Receive the strike price , expiry date and option type information from given id

        strikes, expiry_date, type_of_option = mkt_data.get_option_detail_from_id(id=id)

        # load dividend and underlying from params
        dividend = params["DIVIDEND"]
        underlying_name = params["UNDERLYING"]

        # Create a unique name for the instrument
        instrument_name = str(underlying_name) + "_" + str(strikes) + "_" + str(expiry_date) + "_" + str(type_of_option)

        # create the option object and return the object
        # Returns Options object
        opts = Options(param_list=[id, instrument_name, type_of_option, strikes, expiry_date, underlying_name, dividend])
    
    elif id == params["CASH_ID"]:
        opts = Cash()

    return opts

def get_options_from_id_list(id_list:list,  mkt_data:HistoricalData) -> list:
    '''
    Get option list given a list of unique identifier, and HistoricalData object.

    Parameters
    ----------
    id_list: Unique Identifier list.
    mkt_data: market data instance (HistoricalData object)

    Returns
    -------
    List of Option objects
    '''

    # strikes expiry and option_type
    # Receive the strike price , expiry date and option type information from given id list

    tmp_list = mkt_data.get_option_dtls_from_id_list(id_list=id_list)
    opt_list = list()
    for dtls in tmp_list:

        strikes, expiry_date, type_of_option = dtls

        # load dividend and underlying from params
        dividend = params["DIVIDEND"]
        underlying_name = params["UNDERLYING"]

        # Create a unique name for the instrument
        instrument_name = str(underlying_name) + "_" + str(strikes) + "_" + str(expiry_date) + "_" + str(type_of_option)

        # create the option object and return the object
        # Returns Options object
        opts = Options(param_list=[id_list, instrument_name, type_of_option, strikes, expiry_date, underlying_name, dividend])
        opt_list.append(opts)

    return opt_list


def get_atm_options(underlying:str, expiry:datetime, t:datetime, mkt_data:HistoricalData) -> tuple:
    """
    Returns two Options object.

    parameter
    ---------
    t: at timestep t
    mkt_data: HistoricalData object

    returns: a tuple containing two ATM option class objects 
    """

    logger.debug('within get_atm_options()')

    # Get all informations related to ATM options using mkt_data object (both CE and PE)
    strike_ce, expiry_ce, id_ce, instrument_ce = mkt_data.get_atm_option(underlying=underlying, expiry=expiry, qtime=t, option_type="CE")
    strike_pe, expiry_pe, id_pe, instrument_pe = mkt_data.get_atm_option(underlying=underlying, expiry=expiry, qtime=t, option_type="PE")

    # Dividend - reading from params 
    dividend = params["DIVIDEND"]

    # Instrument Names
    instrument_name_ce = str(instrument_ce) + "_" + str(strike_ce) + "_" + str(expiry_ce) + "_" + "CE"
    instrument_name_pe = str(instrument_pe) + "_" + str(strike_pe) + "_" + str(expiry_pe) + "_" + "PE"

    # Creating -> Objects for ATM Call and Put (return objects)
    opt_atm_ce, opt_atm_pe = Options(param_list=[id_ce, instrument_name_ce, "CE", strike_ce, expiry_ce, instrument_ce, dividend]), Options(param_list=[id_pe, instrument_name_pe, "PE", strike_pe, expiry_pe, instrument_pe, dividend])

    return opt_atm_ce, opt_atm_pe


def get_otm_options(underlying:str, atm_strike:float, expiry:datetime, t:datetime, pct:float, mkt_data:HistoricalData) -> tuple:
    """
    Returns two Options object.

    parameter
    ---------
    t: at timestep t
    mkt_data: HistoricalData object

    returns: a tuple containing two OTM option class objects 
    """
    logger.debug('within get_otm_options()')
    # Get all informations related to ATM options using mkt_data object (both CE and PE)
    strike_ce, expiry_ce, id_ce, instrument_ce = mkt_data.get_otm_option(underlying=underlying, atm_strike=atm_strike, expiry=expiry, qtime=t, option_type="CE", pct=pct)
    strike_pe, expiry_pe, id_pe, instrument_pe = mkt_data.get_otm_option(underlying=underlying, atm_strike=atm_strike, expiry=expiry, qtime=t, option_type="PE", pct=pct)

    # Dividend - reading from params 
    dividend = params["DIVIDEND"]

    # Instrument Names
    instrument_name_ce = str(instrument_ce) + "_" + str(strike_ce) + "_" + str(expiry_ce) + "_" + "CE"
    instrument_name_pe = str(instrument_pe) + "_" + str(strike_pe) + "_" + str(expiry_pe) + "_" + "PE"

    # Creating -> Objects for ATM Call and Put (return objects)
    opt_otm_ce, opt_otm_pe = Options(param_list=[id_ce, instrument_name_ce, "CE", strike_ce, expiry_ce, instrument_ce, dividend]), Options(param_list=[id_pe, instrument_name_pe, "PE", strike_pe, expiry_pe, instrument_pe, dividend])

    return opt_otm_ce, opt_otm_pe



# get synthetic futures -> returns a call and a put
def get_synthetic_futures(underlying:str, trade_time:datetime, expiry:datetime, mkt_data:HistoricalData) -> tuple:
    # Implemented
    return get_atm_options(underlying, expiry, trade_time, mkt_data)
    


# Class initiating the type of instrument and it's properties.
class Instrument:

    # Constructor
    def __init__(self, type_of_instrument:str, currency:str="INR", issuer:str=None, size:int=None, limit:int=None, exchange:str="NSE", param_list=["", ""]) -> None: # param_list = ["id", "instrument_name"]

        # Properties
        self.instrument_type = type_of_instrument  # Type of instrument (Equity, Option, Bonds, Futures)
        self.currency = currency
        self.issuer = issuer
        self.size = size
        self.limit = limit
        self.exchange = exchange
        self.id = param_list[0]   # id of instrument
        self._instrument_name = param_list[1] # Instrument Name


    # Dunder methods __str__ and __repr__
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "Instrument base class"


    # get_spot() -> returns the spot calculated using (highest bid + lowest ask)/2 across all strikes,
    # where spot bid = strike + (bid call - ask put)
    # spot ask = strike + (ask call - bid put)

    def get_spot(self, t:datetime, mkt_data:HistoricalData) -> float:
        """
        t: time 
        mkt_data: Historical Data object

        returns spot 
        """
        # Spot Value Calculations
        self.spot_value = mkt_data.get_spot_v2(ctime=t)

        return self.spot_value

    # Instrument Delta Method
    def calculate_delta(self,t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=0.02) -> float:
        """
        Calculates and Returns the Delta of the Instrument.
        """
        # if instrument type is equity, futures then delta = 1, if cash then delta = 0, options for call and put we have
        # different delta -> the delta method in Options class.
        if self.instrument_type == "cash":
            delta = 0
        elif self.instrument_type == "futures" or self.instrument_type == "equity":
            delta = 1
        else:
            pass # other types of securities can be included such as bonds etc.
        
        return delta
    
    # Instrument Gamma Method
    def calculate_gamma(self,t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=0.02) -> float:
        """
        Calculates the Gamma of the Instrument
        """
        if self.instrument_type == "cash":
            gamma = 0
        elif self.instrument_type == "futures" or self.instrument_type == "equity":
            gamma = 0
        else:
            pass # other types of securities can be included such as bonds etc.
        
        return gamma
    
    # Instrument Theta Method
    def calculate_theta(self,t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=0.02) -> float:
        """
        Calculates the Theta of the Instrument
        """
        if self.instrument_type == "cash":
            theta = 0
        elif self.instrument_type == "futures" or self.instrument_type == "equity":
            pass # doubt -> d/dt(U)
        else:
            pass # other types of securities can be included such as bonds etc.
        
        return theta


    # Abstract Class (get_quote)
    @abstractclassmethod
    def get_quote(self):
        raise NotImplementedError(f"get_quote method not implemented")


    # getter methods
    def getCurrency(self) -> str:
        return self.currency

    def getIssuer(self) -> str:
        return self.issuer

    def getSize(self):
        return self.size

    def getLimit(self):
        return self.limit

    def getExchange(self) -> str:
        return self.exchange

    def getId(self) -> str:
        return self.id

    def getInstrumentname(self) -> str:
        return self._instrument_name
    
    def getInstrumenttype(self) -> str:
        return self.instrument_type


    # setter methods
    def setCurrency(self, currency) -> None:
        self.currency = currency

    def setIssuer(self, issuer) -> None:
        self.issuer = issuer

    def setSize(self, size) -> None:
        self.size = size

    def setLimit(self, limit) -> None:
        self.limit = limit

    def setExchange(self, exchange) -> None:
        self.exchange = exchange

    def setId(self, id) -> None:
        self.id = id

    def setInstrumentname(self, name_of_instrument) -> None:
        self._instrument_name = name_of_instrument

    def setInstrumenttype(self, type) -> None:
        self.instrument_type = type




class Cash(Instrument):

    def __init__(self, currency:str="INR", issuer:str=None, size:int=None, limit:int=None, exchange:str="NSE", param_list=[0]) -> None: # [id]

        # Inherits from super class's constructor 
        super().__init__(type_of_instrument="cash", currency=currency, issuer=issuer, size=size, limit=limit, exchange=exchange)
        
        # set id and name of Instrument
        self.id = params['CASH_ID']
        self._instrument_name = "Cash"

    def get_quote(self, t:datetime, q_type:str, mkt_data:HistoricalData) -> tuple:
        """
        t: current timestep
        q_type: quote type
        mkt_data: HistoricalData instance

        Returns a tuple 
        """
        return (1, np.inf)

    # Dunder methods __str__ and __repr__
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "Cash"

    




class Options(Instrument):

    # Constructor of Option Class
    def __init__(self, currency:str="INR", issuer:str=None, size:int=None, limit:int=None, exchange:str="NSE", param_list=["", "", "", "", "", "", ""]) -> None: # [id, "instrument_name", option_type, strike, expiry, underlying, dividend_rate]

        # Inherits from super class's constructor and also sets strike, expiry, underlying attributes
        super().__init__(type_of_instrument="Options", currency=currency, issuer=issuer, size=size, limit=limit, exchange=exchange, param_list=param_list)
        self.option_type = param_list[2]
        self.strike = int(param_list[3])
        self.expiry = param_list[4]
        self.underlying = param_list[5]
        self.dividend_rate = param_list[6]


    # Dunder methods __str__ and __repr__
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        expiry = self.expiry
        if isinstance(expiry, np.datetime64):
            expiry = datetime.utcfromtimestamp(expiry.astype('O')/1e9)

        tmp_dict = {"type":self.option_type,
                    "underlying":self.underlying,
                    "expiry":expiry.strftime("%Y-%m-%d %H:%M:%S"),
                    "strike":self.strike
                    }
        return str(tmp_dict)

    # get quotes for instruments 
    def get_quote(self, t:datetime, q_type:str, mkt_data:HistoricalData) -> tuple: 
        """
        t: time
        q_type: type of quote - bid or ask
        mkt_data: HistoricalData Object

        returns quote price and quantity.
        """
        bidprice, bidqty, askprice, askqty = mkt_data.get_quote(t=t, option_type=self.getOptionType(), expiry=self.getExpiry(), strike=self.getStrike()) # have a query method on HistData which at time t mkt_data.get_price(time_t, option_type, expiry, strike) -> return (bid and ask price and qty)

        try:
            # q_type check 
            # if type is bid return bid price and qty , if type is ask return ask price and qty
            if q_type.lower() == "bid":
                price, qty = bidprice, bidqty
            elif q_type.lower() == "ask":
                price, qty = askprice, askqty
            elif q_type.lower() == "mid":
                price, qty = ((bidprice + askprice) / 2), None

            return price, qty

        # ValueError Otherwise
        except:
            raise ValueError("Options type should be either a CE or a PE")

    def get_quote_by_id(self, t:datetime, instrument_id:int, q_type:str, mkt_data:HistoricalData) -> tuple: 
        """
        t: time
        q_type: type of quote - bid or ask
        mkt_data: HistoricalData Object

        returns quote price and quantity.
        """
        bidprice, bidqty, askprice, askqty = mkt_data.get_quote_by_id(t=t, instrument_id=instrument_id)

        try:
            # q_type check 
            # if type is bid return bid price and qty , if type is ask return ask price and qty
            if q_type.lower() == "bid":
                price, qty = bidprice, bidqty
            elif q_type.lower() == "ask":
                price, qty = askprice, askqty
            elif q_type.lower() == "mid":
                price, qty = ((bidprice + askprice) / 2), None

            return price, qty

        # ValueError Otherwise
        except:
            raise ValueError("Options type should be either a CE or a PE")

    # calculate the time left to expiry
    def calculate_time_to_expiry(self, at_time_t:datetime, total_trading_holidays:int=16) -> float: 
        '''
        Parameters
        ----------
        at_time_t: current time 
        total_trading_holidays: Total number of trading holidays in a year

        Returns
        -------
        Annualised time (in minutes)
        '''
        # calculates the time difference between expiry and present time
        # send in minutes
        total_trading_days_in_year = 252
        number_of_trading_mins_day = (6.25 * 60)


        # caluclating the time left between two given days and converting the total time left into minutes
        time_left, days_left = (pd.to_datetime(self.expiry) - pd.to_datetime(at_time_t)), (pd.to_datetime(self.expiry) - pd.to_datetime(at_time_t)).days
        minutes_left = (time_left - timedelta(days=days_left)).seconds // 60
        time_left_in_mins = (days_left * number_of_trading_mins_day) + minutes_left
        
        # Annualised Time Left
        annualised_time_left = time_left_in_mins / ((total_trading_days_in_year - total_trading_holidays) * number_of_trading_mins_day) # total time left / total trading minutes in a year 

        return annualised_time_left


    # Delta method in options class
    def calculate_delta(self, t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=params["RISK_FREE_RATE"]) -> float:
        '''
        Parameters
        ----------
        t: current time when the strategy is being executed
        rf: risk-free rate
        q_type: bid or ask
        mkt_data: HistoricalData object

        Returns
        -------
        Calculated Delta value 
        '''

        time_to_expire = self.calculate_time_to_expiry(at_time_t=t)
        # try to get the quote of the option, but if it does not exist in the database, get the price of the option with the nearest strike
        try:
            bidprice, bidqty, askprice, askqty = mkt_data.get_quote(t, option_type=self.getOptionType(), expiry=self.getExpiry(), strike=self.getStrike())
        except NoOptionsFound:
            bidprice, bidqty, askprice, askqty = get_nearest_strike_premium(t=t, strike=self.getStrike(),mkt_data=mkt_data)

        spot = self.get_spot(t, mkt_data=mkt_data) # make get_spot in HistData()

        if q_type.lower() == "bid":
            premium = bidprice
        elif q_type.lower() == "ask":
            premium = askprice
        elif q_type.lower() == "mid":
            premium = (bidprice + askprice) / 2

        # ============================= calculating the sigma (implied volatility) ===========================
        def norm_cdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

        def black_scholes_implied_volatility(option_price, S, K, T, r, q=0):
            def error_function(sigma):
                return black_scholes(S, K, T, r, q, sigma, self.getOptionType()) - option_price
            return optimize.brentq(error_function, 0.0001, 10)
        # ================================== Returns the implied volatility ==================================


        # calculating implied volatility
        sigma = implied_volatility_options(price=premium, S=spot, K=self.getStrike(), t=time_to_expire, r=rf, q=self.getDividend(), option_type=self.getOptionType())[0]
        
        # now it returns a positive delta for calls and negative for puts
        if self.option_type == "CE":

            # Calculates the delta for call options
            delta = pv.greeks.delta(
                        flag='c',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )           
        elif self.option_type == "PE":

            # Calculates the delta for put options
            delta = pv.greeks.delta(
                        flag='p',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )

        return delta[0]
    
    def calculate_delta_by_id(self, t:datetime, instrument_id:int, q_type:str, mkt_data:HistoricalData, rf:float=params["RISK_FREE_RATE"]) -> float:
        '''
        Parameters
        ----------
        t: current time when the strategy is being executed
        rf: risk-free rate
        q_type: bid or ask
        mkt_data: HistoricalData object

        Returns
        -------
        Calculated Delta value 
        '''

        # TODO : Check with DDG if rf should be moved to param file as a global
        time_to_expire = self.calculate_time_to_expiry(at_time_t=t)
        try:
            bidprice, bidqty, askprice, askqty = mkt_data.get_quote_by_id(t=t,instrument_id=instrument_id)
        except NoOptionsFound:
            bidprice, bidqty, askprice, askqty = get_nearest_strike_premium(t=t, strike=self.getStrike(),mkt_data=mkt_data)

        spot = self.get_spot(t, mkt_data=mkt_data) # make get_spot in HistData()

        if q_type.lower() == "bid":
            premium = bidprice
        elif q_type.lower() == "ask":
            premium = askprice
        elif q_type.lower() == "mid":
            premium = (bidprice + askprice) / 2

        # ============================= calculating the sigma (implied volatility) ===========================
        def norm_cdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

        def black_scholes_implied_volatility(option_price, S, K, T, r, q=0):
            def error_function(sigma):
                return black_scholes(S, K, T, r, q, sigma, self.getOptionType()) - option_price
            return optimize.brentq(error_function, 0.0001, 10)
        # ================================== Returns the implied volatility ==================================


        # calculating implied volatility
        sigma = implied_volatility_options(price=premium, S=spot, K=self.getStrike(), t=time_to_expire, r=rf, q=self.getDividend(), option_type=self.getOptionType())[0]
        
        # now it returns a positive delta for calls and negative for puts
        if self.option_type == "CE":

            # Calculates the delta for call options
            delta = pv.greeks.delta(
                        flag='c',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )           
        elif self.option_type == "PE":

            # Calculates the delta for put options
            delta = pv.greeks.delta(
                        flag='p',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )

        return delta[0]

    def calculate_gamma(self, t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=params["RISK_FREE_RATE"]) -> float:
        '''
        Parameters
        ----------
        t: current time when the strategy is being executed
        rf: risk-free rate
        q_type: bid or ask
        mkt_data: HistoricalData object

        Returns
        -------
        Calculated Gamma Value 
        '''
        time_to_expire = self.calculate_time_to_expiry(at_time_t=t)
        # try to get the quote of the option, but if it does not exist in the database, get the price of the option with the nearest strike
        try:
            bidprice, bidqty, askprice, askqty = mkt_data.get_quote(t, option_type=self.getOptionType(), expiry=self.getExpiry(), strike=self.getStrike())
        except NoOptionsFound:
            bidprice, bidqty, askprice, askqty = get_nearest_strike_premium(t=t, strike=self.getStrike(),mkt_data=mkt_data)

        spot = self.get_spot(t, mkt_data=mkt_data) 

        if q_type.lower() == "bid":
            premium = bidprice
        elif q_type.lower() == "ask":
            premium = askprice
        elif q_type.lower() == "mid":
            premium = (bidprice + askprice) / 2

        # =============================== calculating the sigma (implied volatility) ======================
        def norm_cdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

        def black_scholes_implied_volatility(option_price, S, K, T, r, q=0):
            def error_function(sigma):
                return black_scholes(S, K, T, r, q, sigma, self.getOptionType()) - option_price
            return optimize.brentq(error_function, 0.0001, 10)
        # =================================== Returns the implied volatility ==============================


        # calculating implied volatility
        sigma = implied_volatility_options(price=premium, S=spot, K=self.getStrike(), t=time_to_expire, r=rf, q=self.getDividend(), option_type=self.getOptionType())[0]
        
        # now it returns a gamma for call and puts .
        if self.option_type == "CE":

            # Calculates the gamma for call options
            gamma = pv.greeks.gamma(
                        flag='c',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )           
        elif self.option_type == "PE":

            # Calculates the gamma for put options
            gamma = pv.greeks.gamma(
                        flag='p',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )

        return gamma[0]
    

    def calculate_theta(self, t:datetime, q_type:str, mkt_data:HistoricalData, rf:float=params["RISK_FREE_RATE"]) -> float:
        '''
        Parameters
        ----------
        t: current time when the strategy is being executed
        rf: risk-free rate
        q_type: bid or ask
        mkt_data: HistoricalData object

        Returns
        -------
        Calculated Theta Value 
        '''
        time_to_expire = self.calculate_time_to_expiry(at_time_t=t)
        # try to get the quote of the option, but if it does not exist in the database, get the price of the option with the nearest strike
        try:
            bidprice, bidqty, askprice, askqty = mkt_data.get_quote(t, option_type=self.getOptionType(), expiry=self.getExpiry(), strike=self.getStrike())
        except NoOptionsFound:
            bidprice, bidqty, askprice, askqty = get_nearest_strike_premium(t=t, strike=self.getStrike(),mkt_data=mkt_data)
            
        spot = self.get_spot(t, mkt_data=mkt_data) 

        if q_type.lower() == "bid":
            premium = bidprice
        elif q_type.lower() == "ask":
            premium = askprice
        elif q_type.lower() == "mid":
            premium = (bidprice + askprice) / 2

        # ===================calculating the sigma (implied volatility)===========================
        def norm_cdf(x):
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

        def black_scholes_implied_volatility(option_price, S, K, T, r, q=0):
            def error_function(sigma):
                return black_scholes(S, K, T, r, q, sigma, self.getOptionType()) - option_price
            return optimize.brentq(error_function, 0.0001, 10)
        # =====================Returns the implied volatility=====================================


        # calculating implied volatility
        sigma = implied_volatility_options(price=premium, S=spot, K=self.getStrike(), t=time_to_expire, r=rf, q=self.getDividend(), option_type=self.getOptionType())[0]
        
        # now it returns theta for call and puts
        if self.option_type == "CE":

            # Calculates the theta for call options
            theta = pv.greeks.theta(
                        flag='c',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )           
        elif self.option_type == "PE":

            # Calculates the theta for put options
            theta = pv.greeks.theta(
                        flag='p',
                        S=spot,
                        K=self.strike,
                        t=time_to_expire,
                        r=rf,
                        sigma=sigma,
                        q=self.getDividend(),
                        model='black_scholes',
                        return_as='numpy'
                    )

        return theta[0]


    # getter methods for strike, expiry, option_type, underlying_name, dividend rate
    def getStrike(self) -> int:
        return self.strike

    def getExpiry(self) -> datetime:
        return self.expiry

    def getOptionType(self) -> str:
        return self.option_type

    def getUnderlyingName(self) -> str:
        return self.underlying
    
    def getDividend(self) -> float:
        return self.dividend_rate

    # setter methods for strike, expiry, option_type, underlying_name, dividend rate
    def setStrike(self, strike) -> None:
        self.strike = strike

    def setExpiry(self, expiry) -> None:
        self.expiry = expiry

    def setOptionType(self, option_type) -> None:
        self.option_type = option_type

    def setUnderlyingName(self, underlying_name) -> None:
        self.underlying = underlying_name

    def setDividendrate(self, dividend) -> None:
        self.dividend_rate = dividend