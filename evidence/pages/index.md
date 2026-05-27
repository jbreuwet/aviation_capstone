---
title: Aviation Delay & Cancellation Dashboard
---

# ✈ Airport Delay & Cancellation Analysis

Real-time flight delay and cancellation metrics across 7 major US airports.

```sql airport_delays
select * from aviation.airport_delays
where airport_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
order by flight_date desc, total_flights desc
```

## Airport Delay Rankings

<BarChart 
    data={airport_delays}
    x=airport_iata
    y=avg_delay_minutes
    title="Average Delay by Airport (minutes)"
    sort=true
/>

## Cancellation Rates

<BarChart 
    data={airport_delays}
    x=airport_iata
    y=cancellation_rate_pct
    title="Cancellation Rate by Airport (%)"
    sort=true
/>

## Summary Metrics

<BigValue 
    data={airport_delays}
    value=total_flights
    title="Total Flights"
/>

<BigValue 
    data={airport_delays}
    value=avg_delay_minutes
    title="Avg Delay (min)"
/>

<BigValue 
    data={airport_delays}
    value=cancellation_rate_pct
    title="Cancellation Rate %"
/>

## Flight Details

<DataTable data={airport_delays}/>