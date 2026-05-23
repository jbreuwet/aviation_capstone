---
title: Airport Congestion Analysis
---

# ✈ Airport Congestion & Holding Patterns

Live aircraft congestion metrics derived from OpenSky Network position data.

```sql congestion
select * from aviation.airport_congestion
where airport_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
order by hour_window desc, aircraft_count desc
```

## Aircraft Count by Airport

<BarChart
    data={congestion}
    x=airport_iata
    y=aircraft_count
    title="Total Aircraft Tracked by Airport"
    sort=true
/>

## Airborne vs On Ground

<BarChart
    data={congestion}
    x=airport_iata
    y=aircraft_airborne
    title="Airborne Aircraft by Airport"
    sort=true
/>

## Possible Holding Patterns

```sql holding
select
    airport_iata,
    airport_name,
    hour_window,
    possible_holding,
    aircraft_airborne,
    round(possible_holding * 100.0 / nullif(aircraft_airborne, 0), 1) as holding_pct
from aviation.airport_congestion
where airport_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
    and possible_holding > 0
order by hour_window desc
```

<DataTable data={holding} rows=15/>

## Congestion Over Time

<LineChart
    data={congestion}
    x=hour_window
    y=aircraft_count
    series=airport_iata
    title="Aircraft Count Over Time by Airport"
/>