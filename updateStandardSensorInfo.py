## 一小時

# import essential modules
import json
import requests
import datetime
import schedule
# import main functionality
from src.dbcontext import Dbcontext, MetaContext

### 沒有CO2

TOKEN: str = "24MfvKyQb2YxkV5KGgSvZ87EE7mKXfgF"
FORMAT: str = "json"
DATETIME: str = "2021/04/10 10:00:00"

def requestData(token, responseFormat, _context):
    url = "https://airtw.epa.gov.tw/airquality_apis/WS_AQData.aspx?token={}&Format={}".format(token, responseFormat)
    response = requests.request("GET", url)
    data = json.loads(response.text)
    _context.updateStandardSensorInfo(data)



if __name__ == "__main__":

    # initialize dbcontext
    myDBcontext = MetaContext({"user":"postgres",
                            "password":"r2tadmiadc",
                            "host":"140.122.82.98",
                            "port":"5432"}, "motcdev")      #

    requestData(TOKEN, FORMAT, myDBcontext)

    