# import essential modules
import os 
import sys
import json
import requests
import pandas as pd
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
import datetime
import schedule
init(convert=True)

# import main functionality
from src.Parser import GeoJsonParser
from src.dbcontext import Dbcontext
from src.requester import Requester
from src.utils import UrlBundler, Key


sensor_id_array = [
    "sfm_flow",
    "pm2_5_uart",
    "voc",
    "pm2_5_i2c",
    "temperature",
    "humidity",
    "speed"
]

device_id_array = [
 "11954491464",
 "11913321910",
 "11954942561",
 "11953725576",
 "11890955024",
 "11913505644"
]

CK = "PKRZ1XHS4SS4YEEYHF"

URL = "https://iot.epa.gov.tw/iot/v1/device/{}/sensor/{}/rawdata?start=2021-09-06 08:00:00&end=2021-09-06 23:50:59"

def requestForData(options):
    response = requests.request(
        "GET", 
        options["URL"].format(options["device_id"], options["sensor_id"]), 
        headers={'CK': options["CK"]}
    )
    data = json.loads(response.text)
    print(data)
    print("========================")

if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    for device_id in device_id_array:
        for sensor_id in sensor_id_array:
            requestForData({
                "device_id": device_id,
                "sensor_id": sensor_id,
                "CK": CK,
                "URL": URL
            })
