-- All count of flights that come or leave any of New York airports.
-- Served flight means for me not canceled.

SELECT COUNT(flight.UniqueCarrier)
 FROM flight_2007 flight
        JOIN airport airp_in ON airp_in.iata=flight.Dest
        JOIN airport airp_out ON airp_out.iata=flight.Origin
WHERE flight.Month=6 AND flight.Cancelled=0 AND (airp_in.city ='New York' OR airp_out.city ='New York');