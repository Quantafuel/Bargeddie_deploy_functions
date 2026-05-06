def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime

    ts_xid1 = "brgd_line1_runningtime"
    ts_xid2 = "brgd_line2_runningtime"

    now = datetime.now()

    def total_running_time_fast(df, threshold=0.3):
        if df.empty or len(df) < 2:
            return 0.0

        col = df.columns[0]
        running = df[col] > threshold

        dt = df.index.to_series().diff().dt.total_seconds().fillna(0)

        return dt[running].sum()

    last_dp1 = client.time_series.data.retrieve_latest(external_id=ts_xid1)
    last_time1 = last_dp1.timestamp
    last_value1 = last_dp1.value

    last_dp2 = client.time_series.data.retrieve_latest(external_id=ts_xid2)
    last_time2 = last_dp2.timestamp
    last_value2 = last_dp2.value

    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=last_time1,
        end=now,
        timezone="Europe/London",
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=last_time2,
        end=now,
        timezone="Europe/London",
    )

    increment_line1 = total_running_time_fast(dps1)
    new_total_line1 = last_value1 + increment_line1

    increment_line2 = total_running_time_fast(dps2)
    new_total_line2 = last_value2 + increment_line2

    client.time_series.data.insert(external_id=ts_xid1, datapoints=[(now, new_total_line1)])

    client.time_series.data.insert(external_id=ts_xid2, datapoints=[(now, new_total_line2)])
