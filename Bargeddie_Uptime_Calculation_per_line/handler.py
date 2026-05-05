def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime, timedelta

    import pandas as pd

    ts_xid1 = "brgd_line1_runningtime"
    ts_xid2 = "brgd_line2_runningtime"

    now = datetime.now()

    def total_running_time_fast(df, threshold=0.3):
        col = df.columns[0]

        # Boolean mask: running or not
        running = df[col] > threshold

        # Find changes (start/stop transitions)
        changes = running.astype(int).diff()

        # Start times = 0 → 1 transitions
        starts = df.index[changes == 1]

        # Stop times = 1 → 0 transitions
        stops = df.index[changes == -1]

        # Edge cases
        if running.iloc[0]:
            starts = starts.insert(0, df.index[0])
        if running.iloc[-1]:
            stops = stops.append(pd.Index([df.index[-1]]))

        # Compute total duration
        total = (stops.values - starts.values).sum()

        return pd.Timedelta(total)

    last_dp1 = client.time_series.data.retrieve_latest(external_id=ts_xid1)
    last_time1 = last_dp1.timestamp
    last_value1 = last_dp1.value

    last_dp2 = client.time_series.data.retrieve_latest(external_id=ts_xid2)
    last_time2 = last_dp2.timestamp
    last_value2 = last_dp2.value

    dps1 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_1-03-M1"."Motorleistung"',
        start=last_time1 - timedelta(minutes=2),
        end=now,
        timezone="Europe/London",
    )

    dps2 = client.time_series.data.retrieve_dataframe(
        external_id='BRGD:s="DB_2-03-M1"."Motorleistung"',
        start=last_time2 - timedelta(minutes=2),
        end=now,
        timezone="Europe/London",
    )

    increment_line1 = total_running_time_fast(dps1)
    new_total_line1 = last_value1 + increment_line1.total_seconds()

    increment_line2 = total_running_time_fast(dps2)
    new_total_line2 = last_value2 + increment_line2.total_seconds()

    client.time_series.data.insert(external_id=ts_xid1, datapoints=[(now, new_total_line1)])

    client.time_series.data.insert(external_id=ts_xid2, datapoints=[(now, new_total_line2)])
