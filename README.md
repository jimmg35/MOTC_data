# MOTC_data

**抓取即時資料**

固定點 main.py
國家站 airtw.py
移動點 mobile.py
-- 資料庫config請於DbContext接口進行修改

**更新固定點與國家測站點位資訊**

固定點 updateFixedSensorInfo.py
國家站 updateStandardSensorInfo.py

啟動後會向第三方伺服器請求最新的點位資料
-- 資料庫config請於DbContext接口進行修改

**到入歷史資料至既有資料庫**

移動點 dumpMobileSensorData.py
國家站 dumpStandardSensorData.py

欲倒入資料庫之移動點資料請存成csv格式，並按照指定schema存放
將其放入mobileSensorData後再執行腳本。

國家站資料請於腳本內指定所需區間，啟動後會向第三方伺服器請求
歷史資料。