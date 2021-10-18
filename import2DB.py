#三秒
# import essential modules
import os 
import sys
import json
import pandas as pd
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
init(convert=True)

# import main functionality
from src.dbcontext import Dbcontext



if __name__ == "__main__":

    # This import database context
    importDbContext = Dbcontext({"user": "postgres", 
                            "password": "r2tadmiadc", 
                            "host": "localhost", 
                            "port": "5432"}, "motcdev")

    df = pd.read_csv("output.csv", encoding="utf-8")
    importDbContext.importMobileHistoryData(df)

    
    



