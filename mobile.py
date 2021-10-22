#三秒
# import essential modules
import os 
import sys
import json
import requests
import pandas as pd
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
import schedule
from datetime import datetime
init(convert=True)

# import main functionality
from src.requester import Requester
from src.Parser import GeoJsonParser
from src.dbcontext import Dbcontext

from src.utils import UrlBundler, Key


sensor_id = "pm2_5"

sensor_ids = ["sfm_flow", "pm2_5_uart", "voc", "pm2_5_i2c",
              "temperature", "humidity", "speed"] 


if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # initialize dbcontext
    # myDBcontext = Dbcontext({"user": "postgres", 
    #                         "password": "r2tadmiadc", 
    #                         "host": "localhost", 
    #                         "port": "5432"}, "sensordata")
    # staticProjectMeta = myDBcontext.getRealTimeDataMeta()


    # This import database context
    importDbContext = Dbcontext({"user": "postgres", 
                            "password": "r2tadmiadc", 
                            "host": "localhost", 
                            "port": "5432"}, "motcdev")
    


    def crawlData():
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # 抓取1156 所有裝置 所有測項的資料
        try:
            allSensorData = myReq.getMobileData_1156("PKRZ1XHS4SS4YEEYHF", sensor_ids)
            importDbContext.parseIn2mobileSensorDB(allSensorData)
            importDbContext.parseIn2mobileSensorHistoryDB(allSensorData)
            print(current_time + "  Complete")
        except:
            print(current_time + "  Connection failed!")

        # allSensorData = myReq.getMobileData_1156("PKRZ1XHS4SS4YEEYHF", sensor_ids)
        # importDbContext.parseIn2mobileSensorDB(allSensorData)
        # importDbContext.parseIn2mobileSensorHistoryDB(allSensorData)


    
    
    
    schedule.every(3).seconds.do(lambda: crawlData())
    while True:
        schedule.run_pending()
        
        