SELECT DISTINCT ShipName, SUBSTR(ShipName, 1,INSTR(ShipName, '-') - 1) as first_hypen
FROM 'Order' 
WHERE ShipName LIKE "%-%" 
ORDER BY ShipName;