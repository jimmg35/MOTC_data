import os 
from os import listdir
import numpy as np 
import pandas as pd
import json
import typing
from typing import List, Dict


# deviceid, voc, pm2_5, humidity, temperature, date, hour, minute
# 2021/3/1 00:01

# self.column_array = {
#     "528": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
#     "1034": ["DEVICE_ID", "VOC(ppb)", "PM2_5(μg/m^3)", "HUMIDITY_MAIN(%)", "TEMPERATURE(℃)"]
# }


class Parser():
    def __init__(self):
        self.column_array = {
            "528": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "671": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "672": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "673": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "674": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"], 
            "675": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "677": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "678": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "680": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "709": ["DEVICE_ID", "PM2_5(μg/m^3)", "TEMPERATURE(℃)", "HUMIDITY(%)"],
            "756": ["DEVICE_ID", "PM2_5(μg/m^3)", "TEMPERATURE(℃)", "HUMIDITY(%)"],
            "986": ["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1024":["DEVICE_ID", "VOC()", "PM2_5(ug/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(C)"],
            "1025":["DEVICE_ID", "VOC()", "PM2_5(ug/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(C)"],
            "1027":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1029":["DEVICE_ID", "VOC()", "PM2_5(ug/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(C)"],
            "1032":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1034":["DEVICE_ID", "VOC(ppb)", "PM2_5(μg/m^3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1035":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1036":["DEVICE_ID", "VOC(ppb)", "PM2_5(ug/m3)", "HUMIDITY(%)", "TEMPERATURE(C)"],
            "1048":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1058":["DEVICE_ID", "VOC()", "PM2_5(ug/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(C)"],
            "1071":["DEVICE_ID", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1072":["DEVICE_ID", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1075":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1079":["DEVICE_ID", "VOC(ppb)", "PM2_5(ug/m3)", "HUMIDITY(%)", "TEMPERATURE(C)"],
            "1084":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1085":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1098":["DEVICE_ID", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1099":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1102":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1105":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1110":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1117":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1120":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1145":["DEVICE_ID", "VOC(ppb)", "PM2_5(ug/m3)", "HUMIDITY(%)", "TEMPERATURE_MAIN(oC)"],
            "1147":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1162":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1167":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1184":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY_MAIN(%)", "TEMPERATURE_MAIN(℃)"],
            "1189":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1192":["DEVICE_ID", "VOC()", "PM2_5(mg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"],
            "1207":["DEVICE_ID", "VOC()", "PM2_5(µg/m3)", "HUMIDITY(%)", "TEMPERATURE(℃)"]
        }
        self.output_column_array = ["deviceid", "voc", "pm2_5", "humidity", "temperature", "date", "hour", "minute"]
        self.no_voc_projects = ["1071", "1072", "1098", "709", "756"]
        self.no_voc_out_column = ["deviceid", "pm2_5", "humidity", "temperature", "date", "hour", "minute"]

    def parseProjectMonthData(self, project, data_path, output_path, output_file_name):
        """
            put all csv files(a project, a month) into folder(data_path)
        """
        divide_interval = 10
        output_count = 1
        file_num = len(listdir(data_path))
        total_data_chunk = []

        for index, csvFile in enumerate(listdir(data_path)):


            file_path_string = self.formatFileString(csvFile, index)
            file_path = os.path.join(data_path, file_path_string)
            data = pd.read_csv(file_path)
            data_chunk = []
            for column in self.column_array[project]:
                data_chunk.append(list(data[column]))

            # parse timestamp columne.
            date = []
            hour = []
            minute = []
            for time in list(data["TIME"]):
                date.append(time.split(' ')[0])
                hour.append(time.split(' ')[1].split(':')[0])
                minute.append(time.split(' ')[1].split(':')[1])
            data_chunk.append(date)
            data_chunk.append(hour)
            data_chunk.append(minute)
            data_chunk = np.array(data_chunk)
            total_data_chunk.append(data_chunk.T)
            if (index+1)%divide_interval == 0 or (index+1)==file_num:
                total_data = self.mergeAllTable(total_data_chunk)
                if project in self.no_voc_projects:
                    output_dataframe = pd.DataFrame(data=total_data, columns=self.no_voc_out_column)
                    output_dataframe.to_csv(os.path.join(output_path, output_file_name + '_' + str(output_count) + ".csv"))
                else:
                    output_dataframe = pd.DataFrame(data=total_data, columns=self.output_column_array)
                    output_dataframe.to_csv(os.path.join(output_path, output_file_name + '_' + str(output_count) + ".csv"))
                total_data_chunk = []
                output_count += 1

    def mergeAllTable(self, total_data_chunk):
        return np.vstack(total_data_chunk)
    
    def formatFileString(self, csvFile, index):
        file_path_string = ""
        for i in csvFile.split('_')[0: len(csvFile.split('_'))-1]:
            file_path_string += i + '_'
        file_path_string += "{}.csv".format(index)
        return file_path_string
    
    @staticmethod
    def parseProjectMeta(projMeta: List[Dict]) -> Dict:
        """
            {"id":{"name": name, "keys": keys}}
        """

        queryset = ['528','671','672','673','674',
                    '675','677','678','680','709',
                    '756','1024','1025','1027','1029',
                    '1032','1034','1035','1036','1048',
                    '1058','1071','1072','1075','1079',
                    '1084','1085','1102','1120','1145',
                    '1147','1162','1167','1184','1189',
                    '1192','1207','1156','565','624','891', '1223']
        output = {}
        for i in projMeta:
            if str(i["id"]) in queryset:
                keys = [k["key"] for k in i["projectKeys"]]
                output[str(i["id"])] = {"name":i["name"], "keys":keys}
        return output
    


class HourParser():
    # @staticmethod
    # def parseTimeStamp(timestamp):
        
    #     return date, int(hour)

    @staticmethod
    def parseHourData(data_list, deviceId, hasVoc):
        if hasVoc:
            total_chunk = []
            voc_data, pm25_data, hum_data, temp_data = data_list[0]["etl"], data_list[1]["etl"], data_list[2]["etl"], data_list[3]["etl"]
            for i, j, k, z in zip(voc_data, pm25_data, hum_data, temp_data):
                date = i["start"].split(' ')[0]
                hour = int(i["start"].split(' ')[1].split(':')[0])
                total_chunk.append([deviceId, i["avg"], j["avg"], k["avg"], z["avg"], date, hour])
            total_chunk_np = np.array(total_chunk)
            return total_chunk_np
        else:
            total_chunk = []
            pm25_data, hum_data, temp_data = data_list[0]["etl"], data_list[1]["etl"], data_list[2]["etl"]
            for j, k, z in zip(pm25_data, hum_data, temp_data):
                date = j["start"].split(' ')[0]
                hour = int(j["start"].split(' ')[1].split(':')[0])
                total_chunk.append([deviceId, "None", j["avg"], k["avg"], z["avg"], date, hour])
            total_chunk_np = np.array(total_chunk)
            return total_chunk_np
    

class GeoJsonParser():
    @staticmethod
    def parseProjectRealTime2GeoJson(data, projectId, folderName):
        
        #print("=====================================")
        #print(data)
        
        geoJsonMainShell = {
            "type": "FeatureCollection",
            "features": []
        }
        

        for i in data:
            try:
                featureShell = {
                    "type": "Feature",
                    "properties": {
                        "deviceId": i["deviceId"],
                        "time": i["time"],
                        "id": i["id"],
                        "value": i["value"][0]
                    },
                    "geometry": {
                        "type": "Point", 
                        "coordinates": [i["lon"], i["lat"]]
                    }
                },
                geoJsonMainShell["features"].append(featureShell)
            except:
                continue
        

        with open(folderName + '/result_{}.json'.format(projectId), 'w') as fp:
            json.dump(geoJsonMainShell, fp)
            print(projectId + " Done!")
