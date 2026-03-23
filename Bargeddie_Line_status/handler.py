def handle(client):
    """
    [requirements]
    pandas
    [/requirements]
    """
    from datetime import datetime

    col1 = 'BRGD:s="DB_1-03-M1"."Motorleistung"'
    col2 = 'BRGD:s="DB_2-03-M1"."Motorleistung"'
    out1 = "brgd_line1_status"
    out2 = "brgd_line2_status"

    # --- Retrieve timeseries as DataFrames ---
    dps1 = client.time_series.data.retrieve_latest(external_id=col1, before=datetime.now())

    dps2 = client.time_series.data.retrieve_latest(external_id=col2, before=datetime.now())

    if dps1.value <= 1:
        status_line1 = 0
    else:
        print("dps1 false")
        status_line1 = 1

    if dps2.value < 1:
        status_line2 = 0
    else:
        status_line2 = 1

    client.time_series.data.insert([datetime.now(), status_line1], external_id=out1)
    client.time_series.data.insert([datetime.now(), status_line2], external_id=out2)
