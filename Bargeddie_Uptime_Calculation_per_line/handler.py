def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime

    import pandas as pd

    from cognite.client.data_classes import TimeSeriesWrite
    from cognite.client.utils import ZoneInfo

    dps1 = client.time_series.data.retrieve_dataframe_in_tz(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=datetime(2025, 6, 17, tzinfo=ZoneInfo("Europe/London")),
        end=datetime.now(tz=ZoneInfo("Europe/London")),
    )

    dps2 = client.time_series.data.retrieve_dataframe_in_tz(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=datetime(2025, 6, 17, tzinfo=ZoneInfo("Europe/London")),
        end=datetime.now(tz=ZoneInfo("Europe/London")),
    )

    def total_running_time(df):
        running_intervals = []
        running_time = pd.Timedelta(0)
        start_time = None
        column_name = df.columns[0]
        for index, row in df.iterrows():
            if row[column_name] > 0.3:
                if start_time is None:
                    start_time = index
            elif start_time is not None:
                running_intervals.append((start_time, index))
                start_time = None

        if start_time is not None:
            running_intervals.append((start_time, df.index[-1]))

        for interval in running_intervals:
            running_time += interval[1] - interval[0]

        return running_time

    line1_running_time = total_running_time(dps1)
    line2_running_time = total_running_time(dps2)

    ts_xid1 = "brgd_line1_runningtime"
    ts_xid2 = "brgd_line2_runningtime"

    try:
        ts = client.time_series.create(
            TimeSeriesWrite(name="Bargeddie Line 1 accumulated running time", external_id=ts_xid1)
        )
        client.time_series.data.insert([(datetime.now(), 0)], external_id=ts_xid1)
        print("Time series 1 created successfully:", ts)
    except Exception as e:
        print("Time series already exists")
        print(type(e))

    try:
        ts = client.time_series.create(
            TimeSeriesWrite(name="Bargeddie Line 2 accumulated running time", external_id=ts_xid2)
        )
        client.time_series.data.insert([(datetime.now(), 0)], external_id=ts_xid2)
        print("Time series 2 created successfully:", ts)
    except Exception as e:
        print("Time series already exists")
        print(type(e))

    client.time_series.data.insert([(datetime.now(), line1_running_time.total_seconds())], external_id=ts_xid1)
    client.time_series.data.insert([(datetime.now(), line2_running_time.total_seconds())], external_id=ts_xid2)
