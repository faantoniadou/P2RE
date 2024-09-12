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


# Configure logging
logging.basicConfig(
    filename='data_logging.log',                    # changed to log to a file
    level=logging.INFO,                             # set logging level to INFO to capture detailed logs
    format='%(asctime)s:%(levelname)s:%(message)s'  # define log message format
)


# Initial device configuration
device_id = 'bf6af1e102ace8839abt9a'
local_key = 'p-j}8A&1MbWMIeT'
device_version = 3.3

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

discover_device()
