USE flight_dataWarehouse;

-- Average Delay per Route (Origin to Destination)
SELECT o.Airport AS Origin, 
       o.State AS OriginState, 
       d.Airport AS Destination, 
       d.State AS DestinationState, 
       AVG(f.ArrivalDelay) AS AvgArrivalDelay
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, o.State, d.Airport, d.State
HAVING COUNT(*) > 50
ORDER BY AvgArrivalDelay DESC;

-- Total Distance by Route
SELECT o.Airport AS Origin, d.Airport AS Destination, SUM(f.Distance) AS TotalDistance
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, d.Airport
ORDER BY TotalDistance DESC;

-- Cancellation Rate per Route
SELECT o.Airport AS Origin, d.Airport AS Destination, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, d.Airport
HAVING COUNT(*) > 50
ORDER BY CancellationRate DESC;

-- Average Delay by Airline by year and Month
SELECT a.Airline, d.Year, d.Month, 
       AVG(f.ArrivalDelay) AS AvgArrivalDelay, 
       AVG(f.DepartureDelay) AS AvgDepartureDelay
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
ORDER BY d.Year, d.Month, AvgArrivalDelay DESC;

-- Total Flights per Airline by Year and Month
SELECT a.Airline, d.Year, d.Month, COUNT(*) AS TotalFlights
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
ORDER BY d.Year, d.Month, TotalFlights DESC;

-- Cancellation Rate by Airline by year and Month
SELECT a.Airline, d.Year, d.Month, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
HAVING COUNT(*) > 100
ORDER BY d.Year, d.Month, CancellationRate DESC;

-- Cancellation Rate by Destination State
SELECT d.State AS DestState, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY d.State
HAVING COUNT(*) > 50
ORDER BY CancellationRate DESC;

-- Daily Cancellation Patterns
SELECT d.Day, SUM(CAST(f.CancelledFlag AS INT)) AS DailyCancellations
FROM FactFlight f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Day
ORDER BY DailyCancellations DESC;