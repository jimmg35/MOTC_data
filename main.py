# import essential modules
import os 
import sys
import json
import requests
import pandas as pd
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
from datetime import datetime
import schedule
init(convert=True)

# 一分鐘

# import main functionality
from src.Parser import GeoJsonParser
from src.dbcontext import Dbcontext
from src.requester import Requester
from src.utils import UrlBundler, Key, printProgressBar

sensor_id = "pm2_5"

sensor_ids = ["pm2_5", "voc"]

if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # initialize dbcontext
    myDBcontext = Dbcontext({"user": "postgres", 
                            "password": "r2tadmiadc", 
                            "host": "localhost", 
                            "port": "5432"}, "sensordata")
    staticProjectMeta = myDBcontext.getRealTimeDataMeta()


    # This import database context
    importDbContext = Dbcontext({"user": "postgres", 
                            "password": "r2tadmiadc", 
                            "host": "localhost", 
                            "port": "5432"}, "motcdev")
    


    def crawlData(staticProjectMeta):
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        

        # 抓取所有專案 所有裝置 所有測項的資料
        try:
            all_projectData = []
            # printProgressBar(0, len(staticProjectMeta), prefix = 'Progress:', suffix = 'Complete', length = 50)
            for index, project in enumerate(staticProjectMeta):
                dataChunk = None
                try:
                    dataChunk = myReq.getRealTimeProjectData(
                        project[1], 
                        sensor_ids
                    )
                except:
                    print(current_time + "  Connection failed!")
                all_projectData.append(dataChunk)
                # print(project)
                # printProgressBar(index, len(staticProjectMeta), prefix = 'Progress:', suffix = 'Complete', length = 50)

            # importDbContext.parseIn2Db(all_projectData)
            print(current_time + "  Complete")
        except:
            print(current_time + "  Connection failed!")





        


    
    # crawlData(staticProjectMeta)
    
    schedule.every(60).seconds.do(lambda: crawlData(staticProjectMeta))
    while True:
        schedule.run_pending()
        
        