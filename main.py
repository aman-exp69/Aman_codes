
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import argparse

from modules import Algo
from modules import Strategy
from modules import Blotter
from modules import Algo
from modules import Portfolio
from modules import HistoricalData
from modules import load_data,preprocess_eis_data
from datasets import load_market_data
from modules._file_paths import UNDERLYING, OPTION_DATA
from modules._backtest import Backtest
from modules.global_variables import params
from modules._logger import logger,get_exception_line_no
from modules._utils import get_market_holidays_by_year

# from modules._data_loader import option_data_preparation, get_underlying_price

def get_time_interval(start_datetime:datetime, 
                      end_datetime:datetime,
                      time_delta_type,
                      time_delta_value
                    #   time_delta_type:str='minute',
                    #   time_delta:float=1
                      ):
    """
        Generates time window of trading.

        Parameters:
            start_datetime: str in 'yyyy-mm-dd hr:mi' format
            end_datetime: str in 'yyyy-mm-dd hr:mi' format
    """

    start_time = datetime(start_datetime.year, 
                      start_datetime.month,
                      start_datetime.day,
                      start_datetime.hour,
                      start_datetime.minute)

    # dt_obj = datetime.datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')
    end_time = datetime(end_datetime.year, 
                        end_datetime.month,
                        end_datetime.day,
                        end_datetime.hour,
                        end_datetime.minute)
    # end_time = datetime(2021, 3, 10, 9, 26) # Replace with your desired end time

    # delta = datetime.timedelta(time_delta_type=1)
    delta = timedelta(**{time_delta_type:time_delta_value})


    date_list = list()
    # date_list.append(start_time.strftime('%Y-%m-%d %H:%M:%S'))
    date_list.append(start_time)
    while start_time < end_time:
        # print(start_time.strftime('%Y-%m-%d %H:%M:%S'))
        start_time += delta
        # date_list.append(start_time.strftime('%Y-%m-%d %H:%M:%S'))
        date_list.append(start_time)

    # print(date_list)
    return date_list


def holiday(q_date_time:datetime):
    '''
    # TODO: to be implemented to check whether new_start_date is a holiday
    '''

    logger.info(f'checking whether the trading day ({q_date_time.weekday()}) is a holiday')
    holiday_list = get_market_holidays_by_year(q_date_time.year)
    # print(holiday_list)

    is_holiday = False
    if q_date_time.weekday() == 5:
        is_holiday = True
        logger.info('#'*100)
        logger.info(f'requested trading day {q_date_time} is a saturday and not a trading day')
        logger.info('#'*100)
    elif q_date_time.weekday() == 6:
        is_holiday = True
        logger.info('#'*100)
        logger.info(f'requested trading day {q_date_time} is a sunday and not a trading day')
        logger.info('#'*100)
    elif q_date_time.date() in holiday_list:
        is_holiday = True
        logger.info('#'*100)
        logger.info(f'requested trading day {q_date_time} is a holiday and not a trading day')
        logger.info('#'*100)
    else:
        is_holiday = False

    return is_holiday

def get_day_difference(start_day:datetime, end_day:datetime):
    time_delta = (end_day - start_day)
    day_difference = time_delta.days

    time_diff_seconds = time_delta.total_seconds()

    hours, min_sec_rem = divmod(time_diff_seconds, 3600)
    days, hours= divmod(hours, 24)

    # TODO: need to check this logic again
    if hours >=0 :
        day_difference += 1

    return day_difference


def main(args):
   
    try:
        date_time_format = "%Y-%m-%d %H:%M:%S" # TODO: need to be moved to yaml # yyyy-mm-dd hh:mi:ss format
        # start_date = datetime.datetime.strptime('2021-03-10 09:16:00', "%Y-%m-%d %H:%M:%S")
        try:
            start_date_time = datetime.strptime(args.start_date_time, date_time_format) 
            end_date_time = datetime.strptime(args.end_date_time, date_time_format) 
        except Exception as ex:
            print(f'date format should be in specified format {date_time_format}')
            print(ex)
            exit(1) # exiting with invalid date format

        day_difference = get_day_difference(start_day=start_date_time, end_day=end_date_time)
        '''
            Assumptions:
                1. The for loop will run for a single day
                2. at each iteration a new data file for a new day should be loaded
        '''
        logger.info('#'*100)
        logger.info('starting main')
        logger.info('initializing portfolio')
        portfolio = Portfolio(initial_cash=args.initial_cash)
        print('#'*100)
        print(f"trade simulator is running for {day_difference} days")
        print('#'*100)
        for day in np.arange(day_difference + 1):
            try:
                # if (day_difference > 0):
                # TODO: assuming in start_date time is from 09:16:00(hh:mi:ss).
                #       if in start_date time is other than 09:16:00(hh:mi:ss), we have to take care it separately 
                new_start_date_time = start_date_time + timedelta(days=int(day))

                # check whether new_start_date is a holiday
                if holiday(new_start_date_time):
                    continue

                if day == day_difference:
                    new_end_date_time = end_date_time
                else:
                    # adding seconds=22440 to start_date (e.g. 2021-03-10 09:16:00) as 
                    # will push the date to trade end_date (e.g. 2021-03-10 15:30:00)
                    new_end_date_time = datetime.strptime(new_start_date_time.strftime('%Y-%m-%d') + ' ' + '15:30:00',date_time_format)
                    # new_end_date_time = new_start_date_time + timedelta(seconds=22440) #TODO: hardcoding to be moved to yaml

                logger.info('#'*100)
                logger.info('initiating new trading day for date {0}'.format(new_start_date_time.strftime('%Y-%m-%d')))
                logger.info('#'*100)
                print('#'*100)
                print(f"new_start_time={new_start_date_time} and new_end_time={new_end_date_time}")
                print('#'*100)
                try:
                    eis_data = HistoricalData(source=args.data_source,
                                            name=args.data_name,
                                            underlying_instrument=args.underlying,
                                            start_date=new_start_date_time.strftime('%Y%m%d'),
                                            end_date=new_end_date_time.strftime('%Y%m%d'),
                                            expiry_type=args.expiry_type)
                    eis_data.load_market_data()
                    logger.info('market data loaded for {0}-{1}'.format(new_start_date_time.strftime('%Y-%m-%d'),new_end_date_time.strftime('%Y-%m-%d')))
                except Exception as ex:
                    print(f"error while loading market data. {ex}")
                    logger.critical(f"error while loading market data at line={get_exception_line_no()}. {ex}")
                    continue

                # print(get_one_minute_interval())
                time_interval_list = get_time_interval(start_datetime=new_start_date_time,
                                                    end_datetime=new_end_date_time,
                                                    time_delta_type=args.interval_type,
                                                    time_delta_value=args.interval_value)

                backtest = Backtest(rows=375,name='condor strategy',mode='algo')

                # portfolio = Portfolio(initial_cash=args.initial_cash)
                #print('portfolio initalized')

                strategy = Strategy(strategy_type=args.strategy_type, 
                                    param_list=[args.otm_percentage,
                                                args.trade_interval,
                                                args.hedge_interval,
                                                args.unwind_time,
                                                args.unit_size,
                                                args.is_mkt_maker], # [otm_percentage,trade_interval,hedge_interval,unwind_time,unit_size,is_mkt_maker]
                                    underlying_instrument=args.underlying,
                                    time_interval_list=time_interval_list) # TODO: input arguement will be used later
                
                #print('strategy initialized')
                if not params['DISABLE_BLOTTER_UPDATE']:
                    blotter = Blotter()

                algo = Algo(time_window=time_interval_list,
                            strategy=strategy,
                            blotter=blotter
                            )
                #print('algo initialized')
                print('firing up driver')
                algo.driver(portfolio=portfolio, backtest=backtest, hist_data=eis_data)

                print(portfolio)
                
                if not params['DISABLE_BLOTTER_UPDATE']:
                    print(blotter)
                    print(f'Rows in Portfolio {portfolio.getDF().shape[0]}')


                #blotter.serialize()
                #portfolio.serialize()
                # TODO: backtest to implement serialize()
                # backtest.serialize()
            except Exception as ex:
                logger.critical(f"exception within main at line={get_exception_line_no()}. {ex}")
        
        # serialize the portfolio
        str_start_time = start_date_time.strftime('%Y%m%d%H%M%S')
        str_end_time = end_date_time.strftime('%Y%m%d%H%M%S')
        portfolio_filename = f'EIS_portfolio_{str_start_time}_{str_end_time}.csv'
        portfolio.serialize(portfolio_filename)
    except Exception as ex:
        print(f"error while running the main. {ex}")
        logger.critical(f"error while running the main at line={get_exception_line_no()}. {ex}")
       

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='TradeSimulator')
    parser.add_argument('-ds', '--data_source', type=str, help='The data source',required=False)
    parser.add_argument('-dn', '--data_name', type=str, help='Data name',required=False)
    parser.add_argument('-u', '--underlying', type=str, help='The underlying instrument',required=False)
    # parser.add_argument('-d', '--trading_date', type=str, help='Trading date',required=False)
    parser.add_argument('-s', '--start_date_time', type=str, help='Trading start date and time (yyyy-mm-dd hh:mi:ss)',required=False)
    parser.add_argument('-e', '--end_date_time', type=str, help='Trading end date and time (yyyy-mm-dd hh:mi:ss)',required=False)
    parser.add_argument('-it', '--interval_type', type=str, help='Time interval type (e.g. hours,minutes,seconds)',required=False)
    parser.add_argument('-iv', '--interval_value', type=int, help='Time interval value (e.g. 2 (i.e. 2 minutes))',required=False)
    parser.add_argument('-ic', '--initial_cash', type=float, help='Initial cash',required=False)
    parser.add_argument('-st', '--strategy_type', type=str, help='Strategy type',required=False)
    parser.add_argument('-otm', '--otm_percentage', type=float, help='OTM percentage',required=False)
    parser.add_argument('-ti', '--trade_interval', type=int, help='Trade interval in minutes',required=False)
    parser.add_argument('-hi', '--hedge_interval', type=int, help='Hedge interval in minutes',required=False)
    parser.add_argument('-uw', '--unwind_time', type=int, help='Unwind time in minutes',required=False)
    parser.add_argument('-us', '--unit_size', type=float, help='Unit size',required=False)
    parser.add_argument('-mm', '--is_mkt_maker', type=int, help='Is market maker',required=False)
    parser.add_argument('-ex', '--expiry_type', type=str, help='Expiry type (possible values weekly|monthly|all)',required=False)
    
    args = parser.parse_args()

    print('starting main')
    main(args)
