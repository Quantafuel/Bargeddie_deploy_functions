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

    # Retrieve signals
    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=now - timedelta(hours=2),
        end=now,
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=now - timedelta(hours=2),
        end=now,
    )

    # Column names
    col1 = 'BRGD:s="DB_1-03-M1"."Motorleistung"'
    col2 = 'BRGD:s="DB_2-03-M1"."Motorleistung"'

    # --- Vectorized thresholding (FAST, SAFE, CLEAN) ---
    dps1["status"] = (dps1[col1] >= 0.3).astype(int)
    dps2["status"] = (dps2[col2] >= 0.3).astype(int)

    # Prepare for write-back
    df1 = pd.DataFrame({"brgd_line1_status": dps1["status"]}, index=dps1.index)
    df2 = pd.DataFrame({"brgd_line2_status": dps2["status"]}, index=dps2.index)

    client.time_series.data.insert_dataframe(df1)
    client.time_series.data.insert_dataframe(df2)
