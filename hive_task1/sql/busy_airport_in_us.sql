
-- For measure busyness we need to take all flights that come in and out from airport
-- Then calculate for each part count by airport_code and sum them.
-- Then join airports by code and filter by USA ,then sort then limit.

SELECT airp.airport,measured_flights.counter
    from(
          SELECT data_set.airport_code,sum (data_set.counter) as counter
                from (
                      SELECT flight.Origin as airport_code, COUNT(flight.Origin) as counter FROM flight_2007 flight
                             WHERE 6<=flight.Month AND flight.Month<=8 AND flight.Cancelled=0
                             GROUP BY flight.Origin
                      UNION ALL
                      SELECT flight.Dest as airport_code, COUNT(flight.Dest) as counter FROM flight_2007 flight
                             WHERE 6<=flight.Month AND flight.Month<=8 AND flight.Cancelled=0
                             GROUP BY flight.Dest
                )data_set GROUP BY data_set.airport_code
    ) measured_flights join airport airp on airp.iata=measured_flights.airport_code
WHERE airp.country='USA'
ORDER BY measured_flights.counter DESC
limit 5;

