import datetime
import requests
import pandas as pd
from grt import grtmodule

class GrtBus:
    area = 1000
    type = 1000
    server_url = 'https://oj5lch0x53.execute-api.us-east-2.amazonaws.com/v1/sensor_data'
    start_at = 9 # 運行開始時刻
    finish_at = 18 # 運行終了時刻

    def __init__(self, bus_id, installed_on):
        self.bus_id = bus_id
        self.installed_on = installed_on

    def operating_hours(self):
        return GrtBus.finish_at - GrtBus.start_at

    def get_onboard_dates(self, gtfile):
        gtdir = "onboard-data/" # grand truth directory
        encoding = grtmodule.get_file_encoding(gtdir + gtfile)
        df = pd.read_csv(gtdir + gtfile, usecols=["バス番号", "日付"], encoding=encoding)
        df["日付"] = pd.to_datetime(df["日付"]) # 型変換
        target_days = df[(df["バス番号"] == self.bus_id) & (df["日付"] >= self.installed_on)]["日付"].unique()
        return pd.to_datetime(target_days)


class AzumaSensor:
    csv_dirs = ["ble-csv/", "meta-csv/"]

    def __init__(self, sensor_id):
        self.sensor_id = sensor_id

    def get_sensing_data(self, dt, operating_hours):
        td_start_at = datetime.timedelta(hours=GrtBus.start_at)
        td_service_on = datetime.timedelta(hours=operating_hours)
        start_at = dt + td_start_at
        end_at = start_at + td_service_on

        i_page = 1
        metas = []
        bles = []
        while True:
            params = {'start_at': start_at.timestamp(), 'end_at': end_at.timestamp(), 'area': GrtBus.area, 'type': GrtBus.type, 'page': i_page}
            response = requests.get(GrtBus.server_url, params=params)
            json_data = response.json()
            if not json_data['body']:
                break
            for item in json_data['body']:
                if item['meta']['sensor_id'] == self.sensor_id:          
                    for body in item['body']:
                        meta = [body['t'], body['route_id'], body['busstop_id']]
                        ble = dict(scan_time=body['t'], addresses=body['ble'])
                        metas.append(meta)
                        bles.append(ble)
                else:
                    pass
            i_page += 1

        return metas, bles

    def make_csv_data(self, dt, operating_hours):
        metas, bles = self.get_sensing_data(dt, operating_hours)

        # meta csv
        meta_columns = ["scan_time", "route_id", "busstop_id"]
        df = pd.DataFrame(metas, columns=meta_columns)
        df.to_csv(AzumaSensor.csv_dirs[1] + self.sensor_id + "/" + dt.date().strftime('%Y-%m-%d') + ".csv")

        # ble csv
        for ble in bles:
            df = pd.DataFrame(ble["addresses"])
            df.to_csv(AzumaSensor.csv_dirs[0] + self.sensor_id + "/" + str(ble["scan_time"]) + ".csv")