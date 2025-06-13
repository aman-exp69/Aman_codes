
import os
import sys
import datetime
import logging as logger

from modules.global_variables import params

# # Create a logger object
# # logger = logging.getLogger(__name__)

# # Set the log level
# if params['DEBUG'] == 1:
#     logger.setLevel(logging.DEBUG)
# else:
#     logger.setLevel(logging.WARNING)


# # Create a file handler
# dt_now = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
# # TODO: need to change the output file when gets bigger
# handler = logging.FileHandler(f'log/hft_{dt_now}.log')

# # Create a formatter
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # Add the formatter to the handler
# handler.setFormatter(formatter)

# # Add the handler to the logger
# logger.addHandler(handler)

# # # Generate log messages
# # logger.debug('Debug message')
# # logger.info('Info message')
# # logger.warning('Warning message')
# # logger.error('Error message')
# # logger.critical('Critical message')

# def get_hft_logger(module_name:str):
#     return logger

def get_exception_line_no():
    
    _,_, exception_traceback = sys.exc_info()
    line_number = exception_traceback.tb_lineno
    
    return line_number


LOG_FILE_PATH = 'log/'

dt_now = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
LOG_FILE = f"hft_{dt_now}.log"

logs_path = os.path.join(LOG_FILE_PATH,LOG_FILE)

# # TODO: need to change the output file when gets bigger
os.makedirs(LOG_FILE_PATH, exist_ok= True)

logger.basicConfig(
    filename=logs_path,
    format = "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
)

if params['DEBUG']:
    logger.getLogger().setLevel(logger.DEBUG)
else:
    logger.getLogger().setLevel(logger.INFO)

if params['DISABLE_LOG']:
    # TODO: need to find a better way to disable
    logger.disable(2000)

# logger.getLogger().disabled = True

# logger.debug('Debug message')
# logger.info('Info message')
# logger.warning('Warning message')
# logger.error('Error message')
# logger.critical('Critical message')