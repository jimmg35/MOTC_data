

class MobileSensorHistory():
    
    def __init__(self, row):
        self.Device_Name = row["id"]
        self.Datetime = row["time"]
        self.longitude = row["lon"]
        self.latitude = row["lat"]
        self.Voc = row["voc"]
        self.Flow = row["SFM_flow"]
        self.Pm2_5_UART = row["pm2_5_uart"]
        self.Pm2_5_I2C = row["pm2_5_i2c"]
        self.Temperature = row["temperature"]
        self.Humidity = row["humidity"]
        self.Speed = row["speed"]
        
    