# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 22:12:19 2024

@author: Reaction Engines
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import tinytuya
import logging                                      # added for logging to track events and errors
import os                                           # added for checking if the directory exists
import atexit                                       # added to handle program exit events

tinytuya.set_debug(True)

# Configure logging
logging.basicConfig(
    filename='data_logging.log',                    # changed to log to a file
    level=logging.INFO,                             # set logging level to INFO to capture detailed logs
    format='%(asctime)s:%(levelname)s:%(message)s'  # define log message format
)


# Initial device configuration
device_id = 'bf6af1e102ace8839abt9a'
local_key = '_>X7-kea-|~*U}wV'
device_version = 3.4

# So;Y3sf#M+xsVV+=

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


df_global = pd.DataFrame(columns=custom_columns)            # global DataFrame to accumulate data

# Define the file path
file_location = 'C:/Users/Faidra/P2RE/'

if not os.path.exists(file_location):                       # check and create directory if not exists
    os.makedirs(file_location)                              # ensure the directory for data logging exists


def discover_device():
    """
        Discover Tuya devices on the local network and return their details.
        This function uses tinytuya.deviceScan() to find the device on the network and obtain updated details such as the local_key if it changes.
    """
    devices = tinytuya.deviceScan()
    for device in devices.values():
        if device['gwId'] == device_id:
            print(f"Devices found: {device}.")
            return device
    return None


def connect_to_device():
    """Function to establish connection to the Tuya device."""
    device = tinytuya.OutletDevice(
        dev_id=device_id,
        address='Auto',
        local_key=local_key,
        version=device_version
    )
    device.set_socketTimeout(5)  # Set a timeout for the device connection
    return device


def extract_data(data):
    
    if 'dps' not in data:
        logging.error('Key "dps" not found in data: %s', str(data))
        return pd.DataFrame()
    
    # Create a DataFrame from the list from the power meter
    df = pd.DataFrame(list(data['dps'].items()), columns=['ID', 'Value'])
    df = df.set_index('ID').T
    df.insert(0, 'timestamp', datetime.now())    

    # print(data)                                                                           # debugging filling up buffer RAM with print statement - disabled for now 

    try:                                                                                    # made this shorter and more readable
        # convert all values to numeric and divide to their respective places
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
    except KeyError as e:                   # added error handling for missing keys
        logging.error('KeyError during data extraction: %s', str(e))  # log error
        return pd.DataFrame()               # return empty DataFrame on error to prevent crashes


def get_next_midnight(current_time):
    """Calculate the timestamp for the next midnight."""
    tomorrow = current_time + timedelta(days=1)
    next_midnight = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    return next_midnight


def get_next_hour(current_time):
    """Calculate the timestamp for the next hour."""
    next_hour = (current_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return next_hour

def get_next_minute(current_time):
    """Calculate the timestamp for the next minute."""
    next_min = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return next_min


def write_to_csv(df, filename):
    """
        Write the DataFrame to a CSV file, appending if the file exists.
    """
    
    global df_global
    df_global = pd.concat([df_global, df], ignore_index=True)  # Append new data to the global DataFrame

    try:
        file_path = os.path.join(file_location, filename)
        df_global.to_csv(file_path, mode='w', header=True, index=False)  # Write the entire global DataFrame
        # if not pd.io.common.file_exists(file_location + filename):
        #     df.to_csv(file_location + filename, mode='a', header=True, index=False)     # write with header
        # else:
        #     df.to_csv(file_location + filename, mode='a', header=False, index=False)    # append without header
    except Exception as e:                                                              # added error handling for file operations
        logging.error('Failed to write to file: %s', str(e))                            # log file writing errors


def save_on_exit():
    """Function to save DataFrame when program exits."""
    filename = datetime.now().strftime("Meter1_%Y%m%d_%H_%M.csv")
    file_path = os.path.join(file_location, filename)
    if not df_global.empty:  # Check if the global DataFrame has data
        df_global.to_csv(file_path, mode='w', header=True, index=False)
        logging.info('Data saved on exit to %s', file_path)

atexit.register(save_on_exit)  # Register the function to save data on exit


def exit_handler():
    """Handler to run on program exit."""
    logging.info("Program terminated. Finalizing data logging.")                        # Log exit information

atexit.register(exit_handler)                                                           # Register exit handler to capture program termination


# Writing to csv easy, but only one meter per file!!
# Tried writing to excel, but appending data to sheets is too tricky.
# Can use HDF, but need to enforce tables to allow appending - not bad

# CSV Version
def main():

    current_time = datetime.now()
    #next_midnight = get_next_midnight(current_time)
    next_hour = get_next_hour(current_time)
    # next_hour = get_next_minute(current_time)                                     # ONLY FOR TESTING
    sleep_interval = 1  # added variable to control sleep interval dynamically
    device = connect_to_device()  # connect to the device initially

    try:
        while True:
            try:
                #Get the data and convert into something loggable
                data = device.status()
                # d.set_socketTimeout(5)                                                          # set a timeout for the device connection to prevent hanging
                # data = d.status() 
                logging.info('Data retrieved from device: %s', data)                            # log retrieved data
                df2 = extract_data(data)


                if not df2.empty and all(df2.columns == custom_columns):                        # validate DataFrame
                    filename = current_time.strftime("Meter1_%Y%m%d_%H_%M.csv")                    # new file every hour
                    write_to_csv(df2, filename)

                if datetime.now() >= next_hour:
                    current_time = datetime.now()
                    next_hour = get_next_hour(current_time)
                    # next_hour = get_next_minute(current_time)
                sleep_interval = 1  # reset sleep interval on success

            except Exception as e:
                logging.error('Error occurred: %s', str(e))
                write_to_csv(df2, "backup.csv")  # Save data in case of error
                
                # Try to discover and reconnect to the device if an error occurs
                device_info = discover_device()
                if device_info:
                    device = tinytuya.OutletDevice(
                        dev_id=device_info['gwId'],
                        address=device_info['ip'],
                        local_key=device_info['local_key'],
                        version=device_version
                    )
                    device.set_socketTimeout(5)
                    logging.info('Reconnected to device: %s', device_info)
                else:
                    logging.error('Failed to rediscover the device.')

                sleep_interval = min(sleep_interval * 2, 60)  # Exponential backoff on error


            except KeyboardInterrupt:
                logging.info("Program interrupted by user. Saving data...")
                filename = datetime.now().strftime("Meter1_%Y%m%d_%H_%M.csv")
                write_to_csv(df_global, filename)  # Save the accumulated data
                print("Stopping the logging and saving data.")
                
            time.sleep(sleep_interval)  # sleep for the determined interval

    except KeyboardInterrupt:
        # ensure the current DataFrame is saved if the program is interrupted
        print("Stopping the logging.")



if __name__ == "__main__":
    main()


#%%
