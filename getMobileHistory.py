#三秒
# import essential modules
import os 
import sys
import json
import requests
import pandas as pd
import numpy as np
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
import datetime
init(convert=True)

# import main functionality
from src.Parser import GeoJsonParser
from src.dbcontext import Dbcontext
from src.requester import Requester
from src.utils import UrlBundler, Key


device_ids = [
    "11890821711",
    "11890764360",
    "11890955024",
    "11891131307",
    "11891367361",
    "11891040633",
    "11891278317",
    "11914716732",
    "11920316818",
    "11913505644",
    "11914919007",
    "11913629180",
    "11914083509",
    "11913957916",
    "11913826023",
    "11914453462",
    "11953389447",
    "11953468211",
    "11953725576",
    "11953537944",
    "11953821971",
    "11953673948",
    "11954000721",
    "11954147133",
    "11954202868",
    "11954368163",
    "11954491464",
    "11954598520",
    "11954650944",
    "11954731227",
    "11954868840",
    "11955147434",
    "11955092004",
    "11890660179",
    "11891473535",
    "11914563346",
    "11914346622",
    "11914215851",
    "11914809843",
    "11913243605",
    "11913766175",
    "11913461161",
    "11914197833",
    "11953264224",
    "11953978294",
    "11891524424",
    "11913321910",
    "11954942561",
    "11914648549",
    "11913148504",
]

sensor_ids = ["sfm_flow", "pm2_5_uart", "pm2_5_i2c",
              "temperature", "humidity", "speed"] 

full_day_interval = [
    {
        "start" : "2021-09-{0:02d} 00:00:00",
        "end"   : "2021-09-{0:02d} 12:00:00"
    },
    {
        "start" : "2021-09-{0:02d} 12:00:01",
        "end"   : "2021-09-{0:02d} 24:00:00"
    }
]


def processRawData(a_day):
    a_sheet = []

    for data in a_day:
        for flow, pm25, pm25_i2c, temp, humi, speed in zip(data["sfm_flow"], data["pm2_5_uart"], data["pm2_5_i2c"], data["temperature"], data["humidity"], data["speed"]):
            flow_value = 0
            pm25_value = 0
            pm25_i2c_value = 0
            temp_value = 0
            humi_value = 0
            speed_value = 0

            try:
                flow_value = flow["value"][0]
            except:
                flow_value = 0

            
            try:
                pm25_value = pm25["value"][0]
            except:
                pm25_value = 0

            
            try:
                pm25_i2c_value = pm25_i2c["value"][0]
            except:
                pm25_i2c_value = 0

            
            try:
                temp_value = temp["value"][0]
            except:
                temp_value = 0

            
            try:
                humi_value = humi["value"][0]
            except:
                humi_value = 0
            
            try:
                speed_value = speed["value"][0]
            except:
                speed_value = 0

            a_sheet.append([
                flow["deviceId"],
                flow["time"],
                flow["time"],
                flow["lat"],
                flow["lon"],
                flow_value,
                pm25_value,
                pm25_i2c_value,
                temp_value,
                humi_value,
                speed_value
            ])
    return a_sheet, len(a_sheet)

colsss = ["Device_Name", "CreatedTime", "Datetime", "Lat", "Lon", "Flow", "PM2_5_UART", "PM2_5_I2C", "Temperature", "Humidity", "Speed"]



if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # This import database context
    # importDbContext = Dbcontext({"user": "postgres", 
    #                         "password": "r2tadmiadc", 
    #                         "host": "localhost", 
    #                         "port": "5432"}, "motcdev")

    # allSensorData = myReq.getMobileData_1156("PKRZ1XHS4SS4YEEYHF", sensor_ids)

    all_device_data = []
    for deviceId in device_ids: #device_ids:  ["11914083509"]
        device_all_day = []
        for i in range(1, 17):

            all_sensor_data_in_a_day = []

            for time in full_day_interval:
                
                all_sensor_data_in_half_day = {}

                for sensorId in sensor_ids: #sensor_ids: ["pm2_5_uart"]

                    data = myReq.getMobileHistoryData(
                        deviceId,
                        sensorId,
                        time["start"].format(i),
                        time["end"].format(i)
                    )

                    all_sensor_data_in_half_day[sensorId] = data

                all_sensor_data_in_a_day.append(all_sensor_data_in_half_day)
            
            day_sheet, sheet_len = processRawData(all_sensor_data_in_a_day)
            if sheet_len != 0:
                day_sheet_np = np.array(day_sheet)
                device_all_day.append(day_sheet_np)

        if len(device_all_day) != 0:
            device_all_day_np = np.concatenate(device_all_day)
            print(device_all_day_np)
            all_device_data.append(device_all_day_np)

    all_device_data_np = np.concatenate(all_device_data)
    print(all_device_data_np)
    print(all_device_data_np.shape)
    df = pd.DataFrame(all_device_data_np, columns=colsss)
    df.to_csv("output.csv", encoding="utf-8")