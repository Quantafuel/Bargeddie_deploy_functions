# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 11:55:09 2025

@author: Espen.Nordsveen
"""


def handle(client):
    from datetime import datetime

    ts = client.time_series.retrieve(id=4650215105734580)

    total_bale_counter = int(ts.metadata.get("Bale counter all grades non resetable"))
    daily_bale_counter = int(ts.metadata.get("Bale counter grade 6 resetable (Daily)"))
    weekly_bale_counter = int(ts.metadata.get("Bale counter all grades resetable (Weekly)"))

    client.time_series.data.insert([(datetime.now(), total_bale_counter)], external_id="brgd_bale_counter_total")
    client.time_series.data.insert([(datetime.now(), daily_bale_counter)], external_id="brgd_bale_counter_daily")
    client.time_series.data.insert([(datetime.now(), weekly_bale_counter)], external_id="brgd_bale_counter_weekly")
