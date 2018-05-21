create table es3_pre as (
  select distinct usr, product from reviews);
  
create table es3 as (
  select r1.product as p1, r2.product as p2, count(1)
  from es3_pre r1 join es3_pre r2 on r1.usr = r2.usr and r1.product < r2.product
  group by r1.product, r2.product
  order by r1.product, r2.product);
