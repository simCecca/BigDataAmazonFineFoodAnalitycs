drop table if exists es1_year2word;
create table es1_year2word as (
select year, exp.splitted as word
from reviews
lateral view explode(split(lower(trim(regexp_replace(summary, '[\'_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"]', " "))), " +")) exp as splitted
);

drop table if exists es1_ywc;
create table es1_ywc as (
select year, word, count(1) as freq
from es1_year2word
group by year, word
);

drop table if exists es1_rank;
create table es1_rank as (
select year, word, freq, row_number() over (partition by year order by freq desc) as rnk
from es1_ywc
);

drop table if exists es1_final;
create table es1_final as (
select year, word, freq from es1_rank where rnk <= 10 order by year, freq desc
);

