drop table if exists es3;
create table es3 as (
select r1.product as p1, r2.product as p2, count(1)
from reviews r1 join reviews r2
on r1.usr = r2.usr and r1.product < r2.product
group by r1.product, r2.product
order by r1.product);
