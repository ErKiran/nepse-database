## Inserting large CSV data into a database efficiently using Golang 

To run this project. 

1. `bash setup.sh` to download nepse-data repo 
2. Create .env with necessary credentials see .env.example for it
3. `go run main.go `

Inserting Large data into a database often leads to the common problem i.e. ` PostgreSQL only supports 65535 parameters`  
We have used unnest to bypass this limitation. More on this blog post 
https://klotzandrew.com/blog/postgres-passing-65535-parameter-limit


BENCHMARKS 
Inserted 165534 rows in 948.937125ms (Also Includes file processing time)
Without file processig time 
Inserted 165534 rows in 3.747084ms 😱

> CREATE TABLE IF NOT EXISTS nepse(
date character varying(70) NOT NULL DEFAULT '',
ticker character varying(70) NOT NULL DEFAULT '',
high character varying(70) NOT NULL DEFAULT '',
close character varying(70) NOT NULL DEFAULT '',
low character varying(70) NOT NULL DEFAULT '',
volume character varying(70) NOT NULL DEFAULT '',
open character varying(70) NOT NULL DEFAULT '');

> DELETE FROM nepse;