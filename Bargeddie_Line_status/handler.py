def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime

    import pandas as pd

    from cognite.client.utils import ZoneInfo

    dps1 = client.time_series.data.retrieve_dataframe_in_tz(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=datetime(2025, 6, 18, tzinfo=ZoneInfo("Europe/Oslo")),
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
    )

    dps2 = client.time_series.data.retrieve_dataframe_in_tz(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=datetime(2025, 6, 18, tzinfo=ZoneInfo("Europe/Oslo")),
        end=datetime.now(tz=ZoneInfo("Europe/Oslo")),
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
