add jar csv-serde-1.1.3-1.2.1-all.jar;

DROP TABLE IF EXISTS reviews_tmp;
DROP TABLE IF EXISTS reviews;

CREATE TABLE reviews_tmp (id INT, product STRING, usr STRING, pn STRING, hn STRING, hd STRING, score INT, time BIGINT, summary STRING, t STRING)
row format serde 'com.bizo.hive.serde.csv.CSVSerde';
LOAD DATA LOCAL INPATH 'data/Reviews.csv' OVERWRITE INTO TABLE reviews_tmp;

create table reviews as (

select id, product, usr, cast(score as int), cast(date_format(from_unixtime(cast(time as bigint)),'yyy') as int) as year, summary from reviews_tmp)



