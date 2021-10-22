## 一小時

# import essential modules
import json
import requests
import datetime
import schedule
# import main functionality
from src.dbcontext import Dbcontext

### 沒有CO2

TOKEN: str = "24MfvKyQb2YxkV5KGgSvZ87EE7mKXfgF"
FORMAT: str = "json"
DATETIME: str = "2021/04/10 10:00:00"

def requestData(token, responseFormat, _context):
    url = "https://airtw.epa.gov.tw/airquality_apis/WS_AQData.aspx?token={}&Format={}".format(token, responseFormat)
    response = requests.request("GET", url)
    try:
        data = json.loads(response.text)
        _context.clearAirTwTable()
        _context.airTwInsertChunk(data)
        _context.airTwInsertChunkHistory(data)
    except:
        print("Response deviation!")
    #print("Complete | " + data[0]["DataCreationDate"])



if __name__ == "__main__":

    # initialize dbcontext
    myDBcontext = Dbcontext({"user":"postgres",
                            "password":"r2tadmiadc",
                            "host":"140.122.82.98",
                            "port":"5432"}, "motcdev")      #

    # requestData(TOKEN, FORMAT, myDBcontext)

    schedule.every(3600).seconds.do(lambda: requestData(TOKEN, FORMAT, myDBcontext))
    while True:
        schedule.run_pending()

    