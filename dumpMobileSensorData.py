
import os
import pandas as pd
from src.dbcontext import MobileDumper


if __name__ == "__main__":
    
    # initialize dbcontext
    myDBcontext = MobileDumper({"user":"postgres",
                                "password":"r2tadmiadc",
                                "host":"140.122.82.98",
                                "port":"5432"}, "motcdev")

    data = pd.read_csv("mobileSensorData/mobileSensorHistory_20211110.csv", encoding="utf-8")

    myDBcontext.dump2Db(data)