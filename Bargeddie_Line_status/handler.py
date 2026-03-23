def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    import pandas as pd

    now = datetime.now(tz=ZoneInfo("Europe/Oslo"))

    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=now - timedelta(hours=2),
        end=datetime.now(),
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=now - timedelta(hours=2),
        end=datetime.now(),
    )

    def status(value):
        if value < 0.3:
            return 0
        else:
            return 1

    dps1["status"] = dps1['BRGD:s="DB_1-03-M1"."Motorleistung"'].apply(lambda x: status(x))
    dps2["status"] = dps2['BRGD:s="DB_2-03-M1"."Motorleistung"'].apply(lambda x: status(x))

    ts_xid1 = "brgd_line1_status"
    ts_xid2 = "brgd_line2_status"

    df1 = pd.DataFrame({ts_xid1: dps1["status"]}, index=dps1.index)
    df2 = pd.DataFrame({ts_xid2: dps2["status"]}, index=dps2.index)

    client.time_series.data.insert_dataframe(df1)
    client.time_series.data.insert_dataframe(df2)
