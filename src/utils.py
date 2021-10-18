
# Author : @jimmg35

class UrlBundler():
    """
        Url library for the project.
    """
    base_url = r"https://iot.epa.gov.tw"
    getProjects: str = base_url + r"/iot/v1/project"
    getDevicesOfProj: str = base_url + r"/iot/v1/device"
    getSensorOfDev: str = base_url + r"/iot/v1/device/{}/sensor"
    
    getIntervalData: str = base_url + r"/iot/v1/device/{}/sensor/{}/rawdata/statistic?start={}&end={}&interval={}&raw=false&option=strict"

    getMinuteData: str = base_url + r"/iot/v1/device/{}/sensor/pm2_5,voc,temperature,humidity/rawdata?start={}&end={}"

    getRealTime: str = base_url + r"/iot/v1/sensor/{}/rawdata"

    getMobileHistory: str = r"https://iot.epa.gov.tw/iot/v1/device/{}/sensor/{}/rawdata?start={}&end={}"
    # 11954491464 sfm_flow 2021-08-01 08:00:00   2021-09-01 08:30:00


class Key():
    """
        key class for api authentication.
    """
    key: str = 'AK39R4UXH52FXA9CPA'

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()