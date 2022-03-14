## Inserting large CSV data into a database efficiently using Golang 



https://klotzandrew.com/blog/postgres-passing-65535-parameter-limit


BENCHMARKS 
Inserted 165534 rows in 948.937125ms (Also Includes file processing time)

> CREATE TABLE IF NOT EXISTS nepse(
date character varying(70) NOT NULL DEFAULT '',
ticker character varying(70) NOT NULL DEFAULT '',
high character varying(70) NOT NULL DEFAULT '',
close character varying(70) NOT NULL DEFAULT '',
low character varying(70) NOT NULL DEFAULT '',
volume character varying(70) NOT NULL DEFAULT '',
open character varying(70) NOT NULL DEFAULT '');

> DELETE FROM nepse;