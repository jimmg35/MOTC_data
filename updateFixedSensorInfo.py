
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
from src.dbcontext import Dbcontext, MetaContext
from src.utils import UrlBundler, Key
from src.Parser import Parser


if __name__ == "__main__":
    # initialize basic object.
    myKey = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # initialize dbcontext
    myDBcontext = MetaContext({"user":"postgres",
                                "password":"r2tadmiadc",
                                "host":"140.122.82.98",
                                "port":"5432"}, "motcdev")


    # get projects metadata.
    projMeta = myReq.getAllProjectsMeta()
    projectUpdateStatus = myDBcontext.updateProjectInfo(projMeta)
    if(projectUpdateStatus):
        print("project_Info update success!")
    else:
        print("project_Info update fail!")



    projMeta_processed = Parser.parseProjectMeta(projMeta)
    deviceMeta = myReq.getDevicesOfProject(projMeta_processed)
    deviceUpdateStatus = myDBcontext.updateFixedSensorInfo(deviceMeta)
    if(deviceUpdateStatus):
        print("Fixed_Sensor_Info update success!")
    else:
        print("Fixed_Sensor_Info update fail!")
