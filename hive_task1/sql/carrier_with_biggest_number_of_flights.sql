SELECT carr.DESCRIPTION, carrier_flight.counter
  from (
        SELECT
            UniqueCarrier,
            COUNT(UniqueCarrier) as counter
        from flight_2007
        group by UniqueCarrier
  )carrier_flight join carrier carr on carr.CODE=carrier_flight.UniqueCarrier
ORDER BY carrier_flight.counter DESC
limit 1;
