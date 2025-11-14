# -*- coding: utf-8 -*-
"""
Created on Thu Nov 13 09:56:12 2025

@author: EspenNordsveen
"""


def handle(client):
    from datetime import datetime

    import pandas as pd

    # Function for calculating cumsum of latest data
    def continue_cumsum(df, movement_type, last_value, new_col_name):
        d = df[df["MovementType"] == movement_type].copy()
        if d.empty:
            return pd.DataFrame(columns=[new_col_name])  # no new values

        d["Sum"] = d["WeightTare"].cumsum() + last_value
        d = d[["Date", "Sum"]].rename(columns={"Sum": new_col_name})
        return d.set_index("Date")

    old_waste_in_lastest = client.time_series.data.retrieve_latest(external_id="brgd_accumulated_waste_in")
    waste_in_time = old_waste_in_lastest.timestamp[0]
    waste_in_dp = old_waste_in_lastest.value[0]

    old_waste_out_lastest = client.time_series.data.retrieve_latest(external_id="brgd_accumulated_waste_out")
    waste_out_time = old_waste_out_lastest.timestamp[0]
    waste_out_dp = old_waste_out_lastest.value[0]

    old_metal_out_lastest = client.time_series.data.retrieve_latest(external_id="brgd_accumulated_metal_out")
    metal_out_time = old_metal_out_lastest.timestamp[0]
    metal_out_dp = old_metal_out_lastest.value[0]

    first_timestamp = min(waste_in_time, waste_out_time, metal_out_time)

    amcs_rows = client.raw.rows.retrieve_dataframe(
        "amcs_db",
        "brgd_tb",
        columns=["Date", "DateTimeGross", "MovementType", "WeightTare"],
        limit=None,
        min_last_updated_time=first_timestamp,
    )
    amcs_rows["DateTimeGross"] = pd.to_datetime(amcs_rows["DateTimeGross"])
    first_timestamp = datetime.fromtimestamp(first_timestamp / 1000)
    amcs_rows = amcs_rows[amcs_rows["DateTimeGross"] > first_timestamp]

    # %% New
    df_new = amcs_rows[["DateTimeGross", "MovementType", "WeightTare"]].copy()
    df_new = df_new.rename(columns={"DateTimeGross": "Date"})
    df_new["Date"] = pd.to_datetime(df_new["Date"])
    df_new = df_new.sort_values("Date")

    df_in = continue_cumsum(
        df_new[df_new["Date"] > datetime.fromtimestamp(waste_in_time / 1000)],
        "I",
        waste_in_dp,
        "brgd_accumulated_waste_in",
    )
    df_out = continue_cumsum(
        df_new[df_new["Date"] > datetime.fromtimestamp(waste_out_time / 1000)],
        "Transfer Out",
        waste_out_dp,
        "brgd_accumulated_waste_out",
    )
    df_metal = continue_cumsum(
        df_new[df_new["Date"] > datetime.fromtimestamp(metal_out_time / 1000)],
        "O",
        metal_out_dp,
        "brgd_accumulated_metal_out",
    )

    # %%

    dfs = [df_in, df_out, df_metal]

    for df in dfs:
        client.time_series.data.insert_dataframe(df)
