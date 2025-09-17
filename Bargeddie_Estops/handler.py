# This function adds datapoints for the number of active estops, and writes an event with the relevant data


def handle(client):
    import uuid

    from datetime import datetime
    from zoneinfo import ZoneInfo

    from cognite.client.data_classes import EventWrite

    e_stop_assets = client.time_series.list(asset_external_ids=["Viridor_Bargeddie_[BAR]Estops"], limit=None)

    e_stops_list = []

    for ts in e_stop_assets:
        if ts.name[-3:] == "CH1":
            e_stops_list.append(ts)

    estop_ids = [ts.external_id for ts in e_stops_list]
    active_estops_times = []
    current_active_estops = []

    active_estop_time = 0
    active_estop_id = ""

    last_dps = client.time_series.data.retrieve_latest(external_id=estop_ids)

    for dp in last_dps:

        if active_estop_time < dp.timestamp[0] and dp.value[0] == 0:
            active_estop_time = dp.timestamp[0]
            active_estop_id = ts.external_id

        if dp.value[0] == 0:
            current_active_estops.append(dp.external_id)

    active_estops_times.sort()

    now = datetime.now(ZoneInfo("Europe/Oslo"))
    timestamp_event = now.replace(second=0, microsecond=0)

    # Insert datapoints in ts
    datapoints = [(timestamp_event, len(current_active_estops))]
    client.time_series.data.insert(datapoints, external_id="Viridor_Bargeddie_active_estops")

    # Create Event with current active estops as metadata
    separator = "\n"
    current_active_estops_str = separator.join(str(item) for item in current_active_estops)

    estop_events = [
        EventWrite(
            external_id="active-estops-" + str(uuid.uuid4()),
            data_set_id=2481947663902676,
            start_time=int(timestamp_event.timestamp() * 1000),
            end_time=int(timestamp_event.replace(second=1).timestamp() * 1000),
            description="List of Current active E-stops",
            type="Estops",
            subtype="Active",
            asset_ids=[5340585700884579],
            metadata=({"Active estops": current_active_estops_str}),
        )
    ]
    res = client.events.create(estop_events)

    # Print to function log
    print("Time for last active E-stop:", datetime.fromtimestamp(active_estop_time / 1000))
    print("Id for last active E-stop:", active_estop_id)

    print("Current active E-stops:", current_active_estops)
    print("Current active E-stop start times:", active_estops_times)

    print("Event creation:", res)
