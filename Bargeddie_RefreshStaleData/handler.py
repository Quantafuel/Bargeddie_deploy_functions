# This function will create new datapoints for the listed external IDs when the data is stale.
# This is done to be able to show continuous lines in grafana.
# To add tags to the function, add the external ID to the list
# The variable minutes_ago sets the max "age" of datapoints to be refreshed


def handle(client):
    # from cog_client import client
    from datetime import datetime, timedelta

    ext_ids = [
        'BRGD:s="DB_1-03-1-GA201"."Momentanwert_t_h"',
        'BRGD:s="DB_2-03-1-GA201"."Momentanwert_t_h"',
    ]

    now = datetime.now()
    last_min_stop = now.replace(second=0, microsecond=0)
    last_min_start = last_min_stop - timedelta(seconds=60)

    for i in ext_ids:
        latest_dp = client.time_series.data.retrieve_latest(external_id=i)
        latest_dp_datetime = datetime.fromtimestamp(latest_dp.timestamp[0] / 1000)

        dp_insert = []

        if last_min_start > latest_dp_datetime:
            for k in range(0, 60):
                dp_insert.append((last_min_start.replace(second=k), latest_dp.value[0]))
            client.time_series.data.insert(dp_insert, external_id=i)
