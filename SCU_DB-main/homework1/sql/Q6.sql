SELECT
     Id, OrderDate, PrevOrderDate, ROUND(julianday(OrderDate) - julianday(PrevOrderDate), 2)
FROM (
     SELECT Id, OrderDate, LAG(OrderDate, 1, OrderDate) OVER (ORDER BY OrderDate ASC) AS PrevOrderDate
     FROM 'Order' 
     WHERE CustomerId = 'BLONP'
     ORDER BY OrderDate ASC
     LIMIT 10
);