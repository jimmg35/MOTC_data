
# Author : @jimmg35


import requests
import json
from typing import List, Dict
from .utils import UrlBundler, Key



class Requester():
    """
        Requester for handling common task
    """
    def __init__(self, UB: UrlBundler, key: Key) -> None:
        self.UB: UrlBundler = UB
        self.Key: Key = key
    
    def getAllProjectsMeta(self) -> List[Dict]:
        ''' 
            Request for all project key 
        '''

        response = requests.request("GET", 
                                    self.UB.getProjects, 
                                    headers={"CK":self.Key.key})
        print("All Projects Meta downloaded!")
        return json.loads(response.text)


    def getDevicesOfProject(self, projectMetaData, chooseOneKey=True):
        ''' 
            Request for every device of all project 
        '''

        output = []
        for i in list(projectMetaData.keys()): #[0:10]
            
            keys = projectMetaData[i]["keys"]
            # Choose only one Project Key to request.
            if chooseOneKey:
                response = requests.request("GET",
                                            self.UB.getDevicesOfProj,
                                            headers={"CK":keys[0]})
                data = json.loads(response.text)
                for device in data:
                    try:
                        output.append({
                            "id": device["id"],
                            "name": device["name"],
                            "lat": device["lat"],
                            "lon": device["lon"],
                            "key": keys[0]
                        })
                    except:
                        # print(projectMetaData[i]["name"])
                        continue

                print("Project[{}] devices data downloaded!".format(i))
        return output


    def getSensorsOfDevice(self, MyStorage):
        '''
            Request for sensor meta of all device
        '''
        output = {}
        for device in MyStorage.storage["DeviceMeta"]:
            #device["ProjectID"]
            headers = {'CK': device["ProjectKey"]}
            response = requests.request("GET", 
                                        self.UB.getSensorOfDev.format(device["id"]), 
                                        headers=headers)
            data = [json.loads(response.text), device["ProjectKey"]]
            output[str(device["id"])] = data
            print("{} downloaded!!".format(device["id"]))
        return output


    def getIntervalDataOfSensor(self, DeviceSensorMeta, sensor_item, projectid, interval, start, end):
        """
            get interval sensor data for one project
             [[{device1},{device2},...],   pm                           
              [{device1},{device2},...],   hum
              [{device1},{device2},...]]   temp
        """
        project_data = DeviceSensorMeta[DeviceSensorMeta["projectid"] == projectid]
        each_sensor_device_data = []
        with open('log.txt', 'a') as f:
            for sensorid in sensor_item: # voc, pm2.5, humidity, temperature
                each_device_data = []
                #count = 0  # control how many devices per project
                for row in project_data.iterrows():
                    try:
                        response = requests.request("GET", 
                                                    self.UB.getIntervalData.format(row[1]["deviceid"], sensorid, 
                                                            start, end, interval), 
                                                    headers={'CK': row[1]["projectkey"]})
                        #print(response.text)
                        each_device_data.append(json.loads(response.text))
                        print("interval: {} project_id:{} device_id: {}, sensor_id: {}".format(str(interval),
                                                                                            projectid, 
                                                                                            row[1]["deviceid"], 
                                                                                            sensorid))
                    except:
                        f.write("{}, {}, {}\n".format(projectid, row[1]["deviceid"], sensorid))
                    # count += 1
                    # if count == 2:
                    #     break
                each_sensor_device_data.append(each_device_data)
        return each_sensor_device_data

    def getHourData(self, options):
        response = requests.request("GET", 
                                    self.UB.getIntervalData.format(options["deviceId"],
                                                                   options["item"], 
                                                                   options["start"], 
                                                                   options["end"], 60), 
                                    headers={'CK': options["projectKey"]})
        
        return json.loads(response.text)


    def getMinuteDataOfProject_interval_device(self, deviceid, CK, start, end, compare_stamp, ts):
        
        """
            撈取一個專案的一個裝置的資料(時間區間內)
        """
        
        with open('log.txt', 'a') as f:
            try:
                response = requests.request(
                                        "GET", 
                                        self.UB.getMinuteData.format(deviceid, start, end), 
                                        headers={'CK': CK}
                                    )
                data = json.loads(response.text)
                last_time = json.loads(response.text)[-1]["createTime"]
                last_time_compare = ts.parse_minute(last_time)
                print("{} successed at {}".format(deviceid, last_time))

                while compare_stamp != last_time_compare:
                    try:
                        response = requests.request(
                                            "GET", 
                                            self.UB.getMinuteData.format(deviceid, last_time, end), 
                                            headers={'CK': CK}
                                        )
                        last_time = json.loads(response.text)[-1]["createTime"]
                        last_time_ = ts.parse_minute(last_time)
                        last_time_compare = ts.build_minute(last_time_)
                        data += json.loads(response.text)
                        print("{} successed at {}".format(deviceid, last_time))
                    except:
                        f.write("{}, {}\n".format(deviceid, last_time))
                        print("{} failed at {}".format(deviceid, last_time))
                        break

            except:
                f.write("{}, {}\n".format(deviceid, last_time))
                print("{} failed at {}".format(deviceid, start))
                return None

            return data
    
    def getRealTimeProjectData(self, CK, sensor_ids):
        dataChunk = []
        for sensor_id in sensor_ids:
            response = requests.request(
                "GET", 
                self.UB.getRealTime.format(sensor_id), 
                headers={'CK': CK}
            )
            data = json.loads(response.text)
            dataChunk.append(data)
        return dataChunk


    def getMobileData_1156(self, CK, sensor_id):
        output = []
        for sid in sensor_id:
            response = requests.request(
                "GET", 
                self.UB.getRealTime.format(sid), 
                headers={'CK': CK}
            )
            # print(response.text)
            # print("==============")
            data = json.loads(response.text)
            output.append(data)
            print(sid)
        return output
    

    def getMobileHistoryData(self, deviceId, sensor_id, start, end):
        response = requests.request(
            "GET", 
            self.UB.getMobileHistory.format(deviceId, sensor_id, start, end), 
            headers={'CK': "PKRZ1XHS4SS4YEEYHF"}
        )
        data = json.loads(response.text)
        return data
        

        