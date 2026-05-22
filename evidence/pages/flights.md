---
title: Flight Details
---

# 🛫 Flight Details

Row level flight data with weather conditions at arrival airport.

```sql flight_details
select * from aviation.flight_details
where arr_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
order by flight_date desc, arr_delay_minutes desc nulls last
```

## Most Delayed Flights

```sql most_delayed
select 
    flight_iata,
    airline_name,
    dep_iata,
    arr_iata,
    arr_delay_minutes,
    weather_category,
    flight_date
from aviation.flight_details
where arr_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
    and arr_delay_minutes > 0
order by arr_delay_minutes desc
limit 20
```

<DataTable data={most_delayed}/>

## Delay Distribution by Airline

```sql airline_delays
select
    airline_name,
    count(*) as total_flights,
    avg(arr_delay_minutes) as avg_delay_minutes,
    count(case when flight_status = 'cancelled' then 1 end) as cancellations
from aviation.flight_details
where arr_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
group by airline_name
order by avg_delay_minutes desc nulls last
```

<BarChart
    data={airline_delays}
    x=airline_name
    y=avg_delay_minutes
    title="Average Delay by Airline (minutes)"
    sort=true
/>

## All Flights

<DataTable data={flight_details} rows=20/>