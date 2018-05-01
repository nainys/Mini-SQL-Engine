# Mini-SQL-Engine

A mini sql engine that supports a subset of SQL queries for numeric data types using command line interface.

## Supported Queries - 
* Select all records : select * from table_name
* Aggregate functions: Simple aggregate functions on a single column (sum, average, max and min)
* Project Columns (could be any number of columns) from one or more tables : select col1, col2 from table_name
* Select/project with distinct from one table : select distinct col from table_name
* Select with where from one or more tables : select table1.col1, table2.col2 from table1, table2 where table1.col1 = 10 AND table2.col2 = 20
* Projection of one or more (including all the columns) from two tables with one join condition : select table1.col1, table2.col2 from table1, table2 where table1.col1 = table2.col2

## To run
bash mini_sql.sh query
