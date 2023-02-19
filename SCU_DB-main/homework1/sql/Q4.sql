select CategoryName, count(*) as cnt, ROUND(AVG(UnitPrice),2),MIN(UnitPrice),MAX(UnitPrice),SUM(UnitsOnOrder)
from Product, Category
where Product.CategoryId = Category.Id
group by CategoryName
having cnt > 10;
