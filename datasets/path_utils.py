from abc import ABC
import os
import numpy as np
import pandas as pd
from typing import Union

class DataPathNotFound(Exception):
    def __init__(self, error_message: str = "Data path not found exception.") -> None:
        super().__init__(error_message)

        self.error_message = error_message

    def __str__(self):
        return self.error_message

class DataFileNotFound(Exception):
    def __init__(self, error_message: str = "Data path not found exception.") -> None:
        super().__init__(error_message)

        self.error_message = error_message

    def __str__(self):
        return self.error_message

class DataFileFormatNotSupported(Exception):
    def __init__(self, error_message: str = "Data file format not supported.") -> None:
        super().__init__(error_message)

        self.error_message = error_message

    def __str__(self):
        return self.error_message


# the path has to be relative
DATA_PATH = 'datasets/data_files'

# Parent folder source
#rename datasets folder to eis_data

# DATA_PATH = f'source/{source}/data_files'

def _adapt_data(data):

    '''
        this function is required to adapt any data structure to underlying data structure being used
    '''
    
    if isinstance(data, pd.DataFrame):
        transformed_data = data.to_numpy()
        return transformed_data
    
    if isinstance(data, np.array):
        return data


def _load_data_file(data_path:str,
                    file_name:str)->Union[np.array,list]:

    if os.path.exists(data_path):
        data_file = file_name
        data_file_ext = file_name.split('.')[-1]
        data_file_path = os.path.join(data_path,data_file)

        if os.path.exists(data_file_path):
            if data_file_ext == 'csv':
                df = pd.read_csv(data_file_path)
                columns = list(df.columns)
            else:
                raise DataFileFormatNotSupported(f"Data file format \"{data_file_ext}\" not supported")
            
            return _adapt_data(df), columns
        else:
            raise DataFileNotFound(f"Data file not found: {data_file_path}")
    else:
        raise DataPathNotFound(f"Data path not found: {data_path}")


def _get_data_and_label(data:np.array,columns:list,label:str):

    y = data[:,columns.index(label)]
    X = np.delete(data,columns.index(label),axis=1)

    return X,y


# def load_market_data(instrument,date):
#     return _load_data_file(DATA_PATH, instrument +'_'+ date + '_Intraday.csv')