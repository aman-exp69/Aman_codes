import warnings
warnings.filterwarnings('ignore')

import numpy as np
import py_vollib_vectorized.implied_volatility as sigma

from scipy.stats import norm


def implied_volatility_options(price:float, S:float, K:int, t:float, r:float, q:float, option_type="CE") -> np.array:
    """
    Calculates the implied volatility (IV) of an options contract.

    Parameters
    ----------
    price: (float)
        option price/premium for the contract
    K: array (float)
        Strike price of the option contract
    S: float
        The spot price of the underlying asset
    r: float
        risk-free rate
    t: float
        annualised time to maturity
    q: float
        Dividend rate of the underlying
    option_type: array
        Indicating the type of the option - (call/put)

    Returns
    -------
    array: Implied Volatility of the Option

    """
    if option_type == "CE":
        flag = "c"
    elif option_type == "PE":
        flag = "p"

    volatility = sigma.vectorized_implied_volatility(price=price, S=S, K=K, t=t, r=r, flag=flag, q=q, model='black_scholes_merton', return_as='numpy')

    return np.nan_to_num(volatility)



def black_scholes(S:float, K:int, T:float, r:float, q:float, sigma:float, option_type:str) -> float:
    """
    Calculates the option premium using the Black-Scholes model .

    Parameters
    ----------
    K: array (float)
        Strike price of the option contract
    S: float
        The spot price of the underlying asset
    r: float
        risk-free rate
    t: float
        annualised time to maturity
    q: float
        Dividend rate of the underlying
    option_type: str
        Indicating the type of the option - (call/put)
    sigma: float
        Implied volatility of the underlying security

    Returns
    -------
    float: Underlying's call/put premium
    """
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'CE':
        price = S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == 'PE':
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)
    else:
        raise ValueError('Option type must be either "CE" or "PE".')

    return price
