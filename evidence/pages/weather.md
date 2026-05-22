---
title: Weather Impact Analysis
---

# 🌤 Weather Impact on Flight Delays

Analysis of how weather conditions correlate with delays across monitored airports.

```sql weather_impact
select * from aviation.weather_impact
where airport_iata in ('EWR', 'ORD', 'SFO', 'JFK', 'LAX', 'ATL', 'DFW')
order by flight_date desc, weather_severity_rank desc
```

## Delays by Flight Category

<BarChart
    data={weather_impact}
    x=flight_category
    y=delay_minutes
    title="Delay Minutes by Weather Category"
    sort=true
/>

## Weather Severity by Airport

<BarChart
    data={weather_impact}
    x=airport_iata
    y=weather_severity_rank
    title="Average Weather Severity by Airport"
    sort=true
/>

## Flight Category Distribution

<DataTable 
    data={weather_impact}
    rows=15
/>