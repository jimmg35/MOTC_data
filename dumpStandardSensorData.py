## 一小時

# import essential modules
import json
import requests
import datetime
import schedule
from datetime import datetime, timedelta
from airtw import DATETIME
# import main functionality
from src.dbcontext import Dbcontext

### 沒有CO2


TOKEN: str = "24MfvKyQb2YxkV5KGgSvZ87EE7mKXfgF"
FORMAT: str = "json"


def requestData(token, responseFormat, datetime):
    request_flag = True
    url = "https://airtw.epa.gov.tw/airquality_apis/WS_AQData.aspx?token={}&Format={}&datadate={}".format(
        token, 
        responseFormat,
        datetime
    )

    while request_flag:
        try:
            response = requests.request("GET", url)
            data = json.loads(response.text)
            request_flag = False
            return data
        except:
            request_flag = True
            print("Response deviation!")



if __name__ == "__main__":

    # initialize dbcontext
    myDBcontext = Dbcontext({"user":"postgres",
                            "password":"r2tadmiadc",
                            "host":"140.122.82.98",
                            "port":"5432"}, "motcdev")      #

    start_date = datetime(2021, 9, 1, 0, 0, 0)
    end_date = datetime(2021, 11, 15, 8, 0, 0)
    delta = timedelta(hours=1)
    while start_date <= end_date:
        DATETIME: str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        data = requestData(TOKEN, FORMAT, DATETIME)

        print(data[0]["DataCreationDate"])
        myDBcontext.airTwInsertChunkHistory(data)
        start_date += delta

    

    # schedule.every(3600).seconds.do(lambda: requestData(TOKEN, FORMAT, myDBcontext))
    # while True:
    #     schedule.run_pending()

    