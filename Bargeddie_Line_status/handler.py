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
        external_id='BRGD:s="1-03-M1_IW3_Strom"',
        start=datetime(2025, 6, 18, tzinfo=ZoneInfo("Europe/Oslo")),
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
    )

    dps2 = client.time_series.data.retrieve_dataframe_in_tz(
        external_id='BRGD:s="2-03-M1_IW3_Strom"',
        start=datetime(2025, 6, 18, tzinfo=ZoneInfo("Europe/Oslo")),
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
    )

    def status(value):
        if value < 100:
            return 0
        else:
            return 1

    dps1["status"] = dps1['BRGD:s="1-03-M1_IW3_Strom"'].apply(lambda x: status(x))
    dps2["status"] = dps2['BRGD:s="2-03-M1_IW3_Strom"'].apply(lambda x: status(x))

    ts_xid1 = "brgd_line1_status"
    ts_xid2 = "brgd_line2_status"

    df1 = pd.DataFrame({ts_xid1: dps1["status"]}, index=dps1.index)
    df2 = pd.DataFrame({ts_xid2: dps2["status"]}, index=dps2.index)

    try:
        ts = client.time_series.create(TimeSeriesWrite(name="Bargeddie Line 1 status bit", external_id=ts_xid1))
        print("Time series 1 created successfully:", ts)
    except Exception as e:
        print("Time series already exists")
        print(type(e))

    try:
        ts = client.time_series.create(TimeSeriesWrite(name="Bargeddie Line 2 status bit", external_id=ts_xid2))
        print("Time series 2 created successfully:", ts)
    except Exception as e:
        print("Time series already exists")
        print(type(e))

    client.time_series.data.insert_dataframe(df1)
    client.time_series.data.insert_dataframe(df2)
