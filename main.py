# import essential modules
import os 
import sys
import json
import time
import queue
import requests
import schedule
import threading
import pandas as pd
from os import listdir, stat
from datetime import datetime
from typing import List, Dict
from colorama import init, Fore, Back, Style
init(convert=True)

# 一分鐘

# import main functionality
from src.Parser import GeoJsonParser
from src.dbcontext import Dbcontext
from src.requester import Requester
from src.utils import UrlBundler, Key
from src.MultiThread import FixedRequestWorker, WorkerCollection

sensor_id = "pm2_5"
sensor_ids = [
    "pm2_5",
    "voc", 
    "temperature",
    "humidity", 
    "co",
    "so2",
    "no2"
]
number_of_workers = 5



if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # This import database context
    importDbContext = Dbcontext({"user":"postgres",
                                "password":"r2tadmiadc",
                                "host":"140.122.82.98",
                                "port":"5432"}, "motcdev")
    staticProjectMeta = importDbContext.getRealTimeDataMeta()
    


    # 抓取所有專案 所有裝置 所有測項的資料
    def crawlData(staticProjectMeta):
        all_projectData = []
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # 建立佇列
        project_queue = queue.Queue()

        # 將專案請求參數放入佇列
        for index, project in enumerate(staticProjectMeta):
            # if index < 1:
            project_queue.put({
                "projectId": project[0],
                "projectKey": project[1],
                "sensor_ids": sensor_ids
            })
        
        # 建立Worker陣列
        worker_collection = WorkerCollection[FixedRequestWorker]()
        for i in range(0, number_of_workers):
            my_worker = FixedRequestWorker(project_queue, i)
            worker_collection.add(my_worker)
        
        # 啟動所有Worker
        worker_collection.startAll()

        # 等待佇列中的任務執行完畢
        worker_collection.joinAll()

        # 蒐集output
        all_projectData = worker_collection.gatherOutput()
            
        # 輸入資料進DB
        importDbContext.parseIn2Db(all_projectData)
        # importDbContext.parseIn2DbHistory(all_projectData)
        print("====== " + current_time + "  Complete ======")

    
    # crawlData(staticProjectMeta)
    
    schedule.every(60).seconds.do(lambda: crawlData(staticProjectMeta))
    while True:
        schedule.run_pending()
        
        