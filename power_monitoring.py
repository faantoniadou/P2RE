# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 22:12:19 2024

@author: Reaction Engines
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import tinytuya
import logging  # added for logging to track events and errors
import os       # added for checking if the directory exists
import atexit   # added to handle program exit events

# Configure logging
logging.basicConfig(
    filename='data_logging.log',  # Changed to log to a file
    level=logging.INFO,           # Set logging level to INFO to capture detailed logs
    format='%(asctime)s:%(levelname)s:%(message)s'  # Define log message format
)

# Connect to Device
d = tinytuya.OutletDevice(
    dev_id='bfc6abb358246784e5jq1w',
    address= 'Auto',      # Or set to 'Auto' to auto-discover IP address
    local_key='So;Y3sf#M+xsVV+=', 
    version=3.3)




'''
# Provided data
data = {'dps': {'101': 2415, '102': 341, '103': 19, '104': 23, '106': 9800, '111': 2411, 
                '112': 265, '113': 22, '114': 34, '116': 13812, '121': 2411, '122': 323, 
                '123': 36, '124': 45, '126': 20896, '131': 44509, '132': 929, '133': 76, 
                '135': 50, '136': 368, '137': 14, '138': '1'}}
'''


# Columns
custom_columns = ['Timestamp',
                  'Phase A Voltage V',
                    'Phase A current A',
                    'Phase A Active Power W',
                    'Phase A Power Factor',
                    'Phase A Energy Consumed Kw.h',
                    'Phase B Voltage V',
                    'Phase B current A',
                    'Phase B Active Power W',
                    'Phase B Power Factor',
                    'Phase B Energy Consumed Kw.h',
                    'Phase C Voltage V',
                    'Phase C current A',
                    'Phase C Active Power W',
                    'Phase C Power Factor',
                    'Phase C Energy Consumed Kw.h',
                    'Total Energy Consumed Kw.h',
                    'Total Current A',
                    'Total Active Power',
                    'Frequency',
                    'id1',
                    'id2',
                    'id3']


# Define the file path
file_location = 'C:/Users/Reaction Engines/Desktop/'

if not os.path.exists(file_location):  # Check and create directory if not exists
    os.makedirs(file_location)  # Ensure the directory for data logging exists

def extract_data(data):
    # Create a DataFrame from the list from the power meter
    df = pd.DataFrame(list(data['dps'].items()), columns=['ID', 'Value'])
    df = df.set_index('ID').T
    df.insert(0, 'timestamp', datetime.now())    

    print(data) 

    try:                                                                                    # made this shorter and more readable
        # Convert all values to numeric and divide to their respective places
        for key in ['101', '111', '121']:
            df[key] = pd.to_numeric(df[key], errors='coerce') / 10
        for key in ['102', '112', '122', '132']:
            df[key] = pd.to_numeric(df[key], errors='coerce') / 1000
        for key in ['104', '114', '124']:
            df[key] = pd.to_numeric(df[key], errors='coerce') / 100
        for key in ['106', '116', '126']:
            df[key] = pd.to_numeric(df[key], errors='coerce') / 100
        for key in ['103', '113', '123', '131', '133', '135', '136', '137', '138']:
            df[key] = pd.to_numeric(df[key], errors='coerce') / 1

        df.columns = custom_columns
        return df                           # inserted this line here for readability and in case the function cuts off before df is returned 
    except KeyError as e:  # added error handling for missing keys
        logging.error('KeyError during data extraction: %s', str(e))  # log error
        return pd.DataFrame()  # return empty DataFrame on error to prevent crashes

    #print(df)

    return df


def get_next_midnight(current_time):
    """Calculate the timestamp for the next midnight."""
    tomorrow = current_time + timedelta(days=1)
    next_midnight = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    return next_midnight


def get_next_hour(current_time):
    """Calculate the timestamp for the next hour."""
    next_hour = (current_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return next_hour


def write_to_csv(df, filename):
    """Write the DataFrame to a CSV file, appending if the file exists."""
    try:
        if not pd.io.common.file_exists(file_location + filename):
            df.to_csv(file_location + filename, mode='a', header=True, index=False)  # Write with header
        else:
            df.to_csv(file_location + filename, mode='a', header=False, index=False)  # Append without header
    except Exception as e:  # Added error handling for file operations
        logging.error('Failed to write to file: %s', str(e))  # Log file writing errors

def exit_handler():
    """Handler to run on program exit."""
    logging.info("Program terminated. Finalizing data logging.")  # Log exit information

atexit.register(exit_handler)  # Register exit handler to capture program termination


# Writing to csv easy, but only one meter per file!!
# Tried writing to excel, but appending data to sheets is too tricky.
# Can use HDF, but need to enforce tables to allow appending - not bad

# CSV Version 
def main():

    current_time = datetime.now()
    #next_midnight = get_next_midnight(current_time)
    next_hour = get_next_hour(current_time)
    sleep_interval = 1  # added variable to control sleep interval dynamically

    try:
        while True:
            try:
                #Get the data and convert into something loggable
                d.set_socketTimeout(5)  # set a timeout for the device connection to prevent hanging
                data = d.status() 
                logging.info('Data retrieved from device: %s', data)  # log retrieved data
                # print('Debug: data content before extraction:', data)     # commented this out 
                df2 = extract_data(data)

                # filename = current_time.strftime("Meter1_%Y%m%d.csv")

                if not df2.empty and all(df2.columns == custom_columns):  # validate DataFrame
                    filename = current_time.strftime("Meter1_%Y%m%d_%H.csv")   # new file every hour
                    write_to_csv(df2, filename)


                # If a new day, start a new file
                # if datetime.now() >= next_hour:
                #     if not pd.io.common.file_exists(file_location + filename):
                #         df2.to_csv(file_location + filename, mode='a', header=True, index=False)
                #     else:
                #         df2.to_csv(file_location + filename, mode='a', header=False, index=False)

                #     current_time = datetime.now()
                #     next_hour = get_next_hour(current_time)

                # else:
                #     if not pd.io.common.file_exists(file_location + filename):
                #         df2.to_csv(file_location + filename, mode='a', header=True, index=False)
                #     else:
                #         df2.to_csv(file_location + filename, mode='a', header=False, index=False)

                # # Sleep for 1 second
                # time.sleep(1)

                if datetime.now() >= next_hour:
                    current_time = datetime.now()
                    next_hour = get_next_hour(current_time)

                sleep_interval = 1  # reset sleep interval on success

            except tinytuya.TuyaException as e:  # added specific exception handling for device errors
                logging.error('Failed to connect to the device: %s', str(e))  # log device connection errors
                sleep_interval = min(sleep_interval * 2, 60)  # implement exponential backoff on error

            except Exception as e:  # general exception handling for unexpected errors
                logging.error('Unexpected error: %s', str(e))  # log unexpected errors
                sleep_interval = min(sleep_interval * 2, 60)  # implement exponential backoff on error

            time.sleep(sleep_interval)  # sleep for the determined interval

    except KeyboardInterrupt:
        # ensure the current DataFrame is saved if the program is interrupted
        print("Stopping the logging.")



if __name__ == "__main__":
    main()



'''
df = grabData(data, df)

# Used to generate the first file - will overwrite if exists
df.to_hdf(h5_file, key='Meter1', mode='w', format='table')

# Used to append to an existing table
df.to_hdf(h5_file, key='Meter1', mode='a', append=True, format='table')





# Read the data
with pd.HDFStore(h5_file, mode='r') as store:
    print(store['Meter1'])

    df = store['Meter1']
    print(df['Timestamp'])
'''