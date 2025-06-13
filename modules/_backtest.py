import os
import glob
import datetime
import numpy as np
import pandas as pd

from modules.global_variables import params
from modules._logger import logger,get_exception_line_no
from visualizer.cc_financial_statistics import financial_summary
from visualizer.cc_financial_plots import return_plots, max_drawdown_plots, rolling_volatility_plots, general_plots, plot_returns_heatmap, rv_distribution_scatter_plots

logger = logger.getLogger("backtest")

class Backtest:
    # init - constructor
    def __init__(self, rows=375, name="Condor", mode="algo", currency="INR", asset_class="Derivates") -> None:
        self._name = name # sets name of the strategy
        self._mode = mode.lower() # analyse or algo
        self._pointer = -1 # pointer
        self._currency = currency # currency
        self._asset_class = asset_class # asset class

        if self._mode == "algo": # check if algo initialize array or else initialize dataframe
            self._backtest_array = np.zeros((rows, ), dtype=[('Timestamp', 'M8[s]'), ('Value', '<f4')]) # array initialized
        elif self._mode.lower() == "analyse":
            self._backtest_df = pd.DataFrame() # df initialized


    def save(self, backtestdatetime:datetime) -> None:       # save numpy file as csv
        """
        backtestdatetime: unique name
        """
        try:
            os.makedirs(params['BCKTST_STORE'], exist_ok=True)

            if self._mode == "algo":
                file_name = f"{backtestdatetime.strftime('%Y%m%d')}" # creating file name
                self._backtest_array = self._backtest_array[:self._pointer + 1] # may have some unfilled blocks , so taking the part upto which it has been filled
                pd.DataFrame(self._backtest_array).to_csv(f"{params['BCKTST_STORE']}{file_name}.csv", index=False) # save to csv .
            else:
                print("Not compatible with this mode of backtest")
        except Exception as e:
            logger.info(f"f'Error in save() in line : {get_exception_line_no()}, error : {e}'")


    def update(self, values:tuple) -> None: # updates the backtest array at each timestamp
        """
        Updates the Backtest Array - given a tuple (timestamp, Value)
        """
        try:
            if self._mode == "algo":
                self._pointer = self._pointer + 1 # updates pointer
                self._backtest_array[self._pointer] = values # insert values in array
            else:
                print("Not compatible with this mode of backtest")
        except Exception as e:
            logger.info(f"f'Error in update() in line : {get_exception_line_no()}, error : {e}'")


    def read(self, path="", start_time="", end_time="", use_date_slicing=False) -> None: # read the files of backtest and generate a dataframe.
        try:
            if self._mode == "analyse":

                if use_date_slicing:
                    # if using date slicing load default storage path
                    file_names = np.array(glob.glob(pathname=params["BCKTST_STORE"]+"*.csv")) # load all files in location
                    file_names.sort(kind="stable")    # using insertion sort on array (fastest sorting method on almost sorted array)
                    file_names = pd.Series(file_names).apply(lambda x: x.split("/")[-1].split(".")[-2]).values # taking only the date part and removing Backtest/ and .csv 
                    
                    start, end = "".join(start_time.split("-")), "".join(end_time.split("-")) # pre-process dates to format 20200101 -> receive dates in format "2020-01-01"
                    file_names = file_names[(file_names >= start) & (file_names < end)] # take all files in the included date
                    
                    # create df
                    self._backtest_df = pd.concat([pd.read_csv(params["BCKTST_STORE"] + file_path + ".csv") for file_path in file_names], axis=0) # loading and concatenating all files into a single dataframe
                else:
                    file_names = np.array(glob.glob(pathname=path+"*.csv")) # sorting all files at a particular location
                    file_names.sort(kind="stable")    # using insertion sort on array (fastest sorting method on almost sorted array)
                    self._backtest_df = pd.concat([pd.read_csv(file_path) for file_path in file_names], axis=0) # loading and concatenating all files into a single dataframe

                # timestamp as datetime
                self._backtest_df["Timestamp"] = pd.to_datetime(self._backtest_df["Timestamp"])
                self._backtest_df.sort_values(by="Timestamp", inplace=True)
            else:
                print("Not compatible with this mode of backtest")
        except Exception as e:
            logger.info(f"f'Error in read() in line : {get_exception_line_no()}, error : {e}'")


    # Plots All Charts
    def plotCharts(self, freq="D", agg="last", chart_width=900, chart_height=450, window=10) -> None:
        """
        frequency: Frequency of Resampling df.
        agg: Aggregation Method.
        chart_width: Width of Charts
        chart_height: Height of Charts
        window: rolling window for charts
        """
        try:
            if self._mode == "analyse":
                # plot-df
                plot_df = self.__resample_df(frequency=freq, agg=agg)

                # cumulative returns plot
                return_plots(df=plot_df, frequency=freq, strategy=self.getName(), graph_width=chart_width, graph_height=chart_height, yaxis="Cumulative Return (%)",
                                                                                                                xaxis="Timestamp", title="Cumulative Returns", x_shift=25).show()


                # Plots the max drawdown
                max_drawdown_plots(df=plot_df, frequency=freq, strategy=self.getName(), graph_width=chart_width, graph_height=chart_height, yaxis=" Maximum Drawdown (%)",
                                                                                                                xaxis="Timestamp", title="Max Drawdown").show()


                # volatility charts
                rolling_volatility_plots(df=plot_df, frequency=freq, windows=window, strategy=self.getName(), graph_width=chart_width, graph_height=chart_height, yaxis="Volatility (%)",
                                                                                                                xaxis="Timestamp", title="Rolling Volatility", x_shift=25).show()

                # Cumulative PnL
                plot_df['Cumulative PnL'] = self.__calculate_pnl(plot_df['Value'])
                general_plots(df=plot_df[['Cumulative PnL']].dropna(), frequency=freq, strategy=self.getName(), graph_width=chart_width, graph_height=chart_height, y_title=f"Cumulative PnL ({self._currency})",
                                                                                                                x_title="Timestamp", chart_title=f"Cumulative PnL", x_shift=25).show()

                # monthly returns heatmap
                temp, z_val = self.__heatmap_monthly_return_data(portfolio=self.__resample_df(frequency=freq, agg=agg), date_col="Timestamp", value_col="Value")
                plot_returns_heatmap(temp, z_val, graph_height=chart_height, graph_width=chart_width).show()


                # Distribution of Returns
                # rv_distribution_scatter_plots(data=plot_df[['Value']].reset_index(), date_col="Timestamp", price_col="Value", frequency=freq, annualised=True, only_return_dist=True).show()

                # kill unnecessary dfs
                del plot_df, temp
            else:
                print("Not compatible with this mode of backtest")
        except Exception as e:
            logger.info(f"f'Error in plotCharts() in line : {get_exception_line_no()}, error : {e}'")


    # Monthly data preparation for Heatmap
    def __heatmap_monthly_return_data(self, portfolio, date_col, value_col):
        # Make a copy and reset index 
        monthly_ret_table = portfolio.copy()
        monthly_ret_table = monthly_ret_table.resample("M").last()
        monthly_ret_table.reset_index(inplace=True)

        # convert date column to datetime and calculate returns
        monthly_ret_table[date_col] = pd.to_datetime(monthly_ret_table[date_col])
        monthly_ret_table['returns'] = monthly_ret_table[value_col].pct_change()

        # calculating year and months
        monthly_ret_table['year'] = monthly_ret_table[date_col].dt.year
        monthly_ret_table['month'] = monthly_ret_table[date_col].dt.month
        
        # returns and months and year extracted from dataframe
        monthly_ret_table = monthly_ret_table[['year', 'month', 'returns']]
        monthly_ret_table['returns'] = monthly_ret_table['returns'] * 100
        monthly_ret_table['returns'] = monthly_ret_table['returns'].round(2)

        zmax = max(monthly_ret_table['returns'].max(), abs(monthly_ret_table['returns'].min()))

        # create a pivot table 
        monthly_ret_table = monthly_ret_table.set_index(['year', 'month']).unstack(level = -1)
        monthly_ret_table.columns = monthly_ret_table.columns.droplevel()

        monthly_ret_table.columns.name = None

        dict_mapper = {"1":"Jan", "2":"Feb", "3":"Mar", "4":"Apr", "5":"May", "6":"Jun",
                       "7":"Jul", "8":"Aug", "9":"Sep", "10":"Oct", "11":"Nov", "12":"Dec"}
        
        columns = []
        for col in range(len(monthly_ret_table.columns)):
                columns.append(dict_mapper[str(monthly_ret_table.columns[col])])
        
        # Name of the columns
        monthly_ret_table.columns = columns
        
        # Transpose
        monthly_ret_table = monthly_ret_table.transpose()
        monthly_ret_table.index = pd.Series(monthly_ret_table.index).apply(lambda x: str(x)) # string conversion
        monthly_ret_table.columns = pd.Series(monthly_ret_table.columns).apply(lambda x: str(x)) # string conversion

        return monthly_ret_table, zmax



    # This is method to be accessed by internal methods only (private)
    def __resample_df(self, frequency="D", agg="last") -> None:
        """
        frequency: Frequency of Resampling df.
        agg: Aggregation Method.
        """
        try:
            if agg == "last":
                return self._backtest_df.set_index("Timestamp").resample(frequency).last()
            elif agg == "first":
                return self._backtest_df.set_index("Timestamp").resample(frequency).first()
            elif agg == "mean":
                return self._backtest_df.set_index("Timestamp").resample(frequency).mean()

        except Exception as e:
            print(f"Warning: {e}, returns default - daily resampling with aggregation function as last")


    # summary
    def summary(self, frequency="D", agg="last") -> pd.DataFrame:
        try:
            if self._mode == 'analyse':
                return financial_summary(self.__resample_df(frequency=frequency, agg=agg).reset_index(), 
                            frequency=frequency, date_col="Timestamp", col_name_cagr="Value", risk_free_rate=params["RISK_FREE_RATE"], asset_class=self.getAssetclass())
            else:
                print("Not compatible with this mode of backtest")   
        except Exception as e:
            logger.info(f"f'Error in summary() in line : {get_exception_line_no()}, error : {e}'")
        

    # pnl calculation - private method
    def __calculate_pnl(self, port_values) -> np.array:
        return pd.Series(port_values).diff().cumsum().values


    def getData(self): # get data, return a numpy array if phase is "algo" else return "dataframe"
        if self._mode == "algo":
            return self._backtest_array[:self._pointer + 1]
        elif self._mode == "analyse":
            return self._backtest_df

    def getName(self): # returns the name of the backtest instance
        return self._name
    
    def getAssetclass(self): # name of asset class
        return self._asset_class