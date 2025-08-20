# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 12:36:46 2025

@author: Henrik.Rost.Breivik
"""


def handle(client):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    import pandas as pd
    import scipy.signal as scs

    ext_ids = [
        'BRGD:s="DB_1-03-1-GA201"."Momentanwert_t_h"',
        'BRGD:s="DB_2-03-1-GA201"."Momentanwert_t_h"',
    ]

    now = datetime.now(ZoneInfo("Europe/Oslo"))
    tw_stop = now.replace(second=0, microsecond=0) - timedelta(seconds=30)
    tw_start = tw_stop - timedelta(seconds=60)

    for i in ext_ids:
        ts_dp_df = client.time_series.data.retrieve_dataframe(
            external_id=i, start="5m-ago", end="now", timezone="Europe/Oslo"
        )

        data = ts_dp_df[i]
        result_sg = pd.Series(scs.savgol_filter(data, window_length=20, polyorder=1), index=data.index)

        result_sg_df = result_sg.to_frame()
        result_sg_df.rename(columns={0: "Values"}, inplace=True)

        filtered_result = result_sg_df.loc[tw_start:tw_stop]

        client.time_series.data.insert(filtered_result, external_id=i + "_filtered")

        print(f"Addde filtered data from {tw_start} to {tw_stop} for timeseries {i + '_filtered'}")
