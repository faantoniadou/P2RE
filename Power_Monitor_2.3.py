# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 22:12:19 2024

@author: Reaction Engines
Version 2.3
Added capability of logging to sql (sqlite) database. 
Is better for this type of logging. Can then use jupyter for reporting, or even add to IFS

Version 2.2
1. Added .strftime('%x %X') to datatime so that Excel reports it correctly
2. Created a dataframe of set structure to add new data to. This reads data, finds identical keys, and sets values before saving
3. Rewrote main code to allow call to receive data, retry if rubbish, and then log, delaying for the remainder of the time
to 1 second.

Power Monitor 1
local_key='*{n!xr#0`0&E?mNR',
dev_id='bf6af1e102ace8839abt9a'

Power Monitor 2
dev_id='bfd460c33b9d34bcd4xvy7'
local_key='f*agOw`GRkl@uRXW'
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import tinytuya
from sqlalchemy import create_engine

# Connect to Device
d = tinytuya.OutletDevice(
    dev_id='bfd460c33b9d34bcd4xvy7',
    address= 'Auto',      # Or set to 'Auto' to auto-discover IP address
    local_key='f*agOw`GRkl@uRXW', 
    version=3.4,
    persist=True)


tinytuya.set_debug(False,False)

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
                    'id3',
                    'err','elapsed time']
                    
default_columns = ['timestamp','101','102','103','104','106','111', 
                '112','113','114','116','121','122', 
                '123','124','126','131','132','133', 
                '135','136','137','138','err','elapsed time']                   

# Define the file path
file_location = 'C:/Users/Faidra/P2RE/Power Monitor Data/'


def get_next_hour(current_time):
    """Calculate the timestamp for the next hour."""
    next_hour = (current_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return next_hour

def extract_data(data , err_count, elapsed_time):
    # Create an empty dataframe with the correct structure for all data expexted
    # Then loop through the data list and update values.
    # Currently removes old data, but could be extended to retain old data by using global dataframe
    df = pd.DataFrame(0, index=range(1), columns=default_columns)
    
    for key, value in data['dps'].items():
        if key in df.columns:
            # Update the value in df_other for the first row
            df.loc[0, key] = value
    
    df['timestamp'] = datetime.now().strftime('%x %X')   #V2.2 Added .strftime('%x %X') so that excel shows it properly
    
    df['err'] = err_count
    df['elapsed time'] = elapsed_time
    
    try:
        
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

    except KeyError:
        
        print('Guess we lost data')
    
    return df


# CSV Version 
def main():
    
    # Create an SQLite database in a file (or connect to one)
    engine = create_engine('sqlite:///PowerMonLog.db')
    
    print(" > Send Request for Status < ")
    d.status(nowait=True)
    
    current_time = datetime.now()
    
    next_hour = get_next_hour(current_time)
    err_count = 0
    elapsed_time = 0
    
    try:
        print(" > Begin Monitor Loop <")
        while(True):
            start_time = time.time()
            
            # See if any data is available
            # data = d.receive() # Dont use - this is much slower for some reason
            
            data = d.status()
            
            # Sometimes not all data comes through, therefore choose one random ID to determine if it exists.
            # If not, repeat loop and try again, else extract and log
            # err_count is set to 1 if data is missing
            specific_dps = data['dps'].get('135', 'DPS not available')
            
            #print(f"DPS 101: {specific_dps}")
            
            # Send keep-alive heartbeat if no data is collected
            if not data:
                print(" > Send Heartbeat Ping < ")
                d.heartbeat()
            
            # Sleep for 1 second only if DPS data was obtained, and log data
            if not specific_dps == 'DPS not available' :
                
                last_elapsed_time = elapsed_time
                
                df2 = extract_data(data, err_count, last_elapsed_time)
                
                # Code for Excel
                if datetime.now() >= next_hour:               
                    current_time = datetime.now()
                    next_hour = get_next_hour(current_time)
                    
                filename = current_time.strftime("Meter1_%Y%m%d%H.csv")
     
                if not pd.io.common.file_exists(file_location + filename):
                    df2.to_csv(file_location + filename, mode='a', header=True, index=False)
                else:
                    df2.to_csv(file_location + filename, mode='a', header=False, index=False)
                
                # Code for database
                df2.to_sql('Meter1_table', con=engine, if_exists='append', index=False)
                
                
                err_count = 0
                # Sleep for the remainder of time so that it keeps closer to 1 second    
                elapsed_time = time.time() - start_time
                if elapsed_time < 1:
                    time.sleep(1 - elapsed_time)
        
            else:
                err_count = err_count + 1
        
    except KeyboardInterrupt:
        # Ensure the current DataFrame is saved if the program is interrupted
        print("Stopping the logging.")
        


if __name__ == "__main__":
    main()
    
   