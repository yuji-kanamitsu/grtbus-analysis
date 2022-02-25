import datetime
db_bus = {
    "bus01": { # 大宮ルート
        "bus_id": "奈良200-か-824",
        "sensor_id": "nk_0883",
        "installed_on": datetime.datetime(2021, 11, 26)
    },
    "bus02": { # 奈良公園ルート（休日運行）
        "bus_id": "奈良200-か-820",
        "sensor_id": "nk_0879",
        "installed_on": datetime.datetime(2021, 11, 26)
    },
}

gtfiles = ["stop_202111.csv", "stop_202112.csv"] # 分析対象のgrand truthファイル

def grt_csv_maker(bus_data):
    # generate instanse
    from grt import grtbus
    bus = grtbus.GrtBus(bus_data["bus_id"], bus_data["installed_on"])
    sensor = grtbus.AzumaSensor(bus_data["sensor_id"])

    # make directory
    import os
    for dir in sensor.csv_dirs:
        os.makedirs(dir + sensor.sensor_id, exist_ok=True)

    # make csv
    for file in gtfiles:
        for date in bus.get_onboard_dates(file):
            dt = datetime.datetime(date.year, date.month, date.day)
            sensor.make_csv_data(dt, bus.operating_hours())
            print(f"date: {dt} DONE!")
        print(f"--------file: {file} DONE!--------")

grt_csv_maker(db_bus["bus01"])