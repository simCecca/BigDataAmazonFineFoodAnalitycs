drop table if exists es2;
drop table if exists es2_printable;

create table es2 as (
SELECT product, cast(year as string) as year, cast(AVG(score) as string) as average
FROM reviews
where year >= 2003
group by product, year
order by product);


create table es2_printable as (
		
select product, collect_list(concat_ws(':',year,average)) as avg_list
from es2
group by product
);

