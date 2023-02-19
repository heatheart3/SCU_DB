SELECT IFNULL(t2.CompanyName, "MISSING_NAME"), t1.CustomerId, ROUND(t1.s, 2)
FROM
(
    SELECT a.CustomerId, SUM(b.Quantity * b.UnitPrice) s,NTILE(4) OVER (ORDER BY SUM(b.Quantity * b.UnitPrice) ASC) bkt
    FROM 'Order' a
        JOIN OrderDetail b ON a.Id = b.OrderId
    GROUP BY a.CustomerId
) t1
LEFT JOIN Customer t2 ON t2.Id = t1.customerId
WHERE t1.bkt = 1;