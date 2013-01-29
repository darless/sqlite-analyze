sqlite-analyze
==============

Analyze a SQLite database. 

To Run
======

./sqlite-analyze basic test-db/test.db
Table           : # of entries
--------------------------------------
table1          : 5536
table2          : 5536
table3          : 5536
--------------------------------------

./sqlite-analyze detail test-db/test.db
Parsing table table1
Parsing table table2
Parsing table table3

DB Size: 441.34 KB

Table           : # entries        Size             Entry Size       Percent         
-------------------------------------------------------------------------------------
table2          : 5536             87.04 KB         15.0 B           19.72 %         
table3          : 5536             149.5 KB         27.0 B           33.87 %         
table1          : 5536             206.85 KB        37.0 B           46.87 %



Future
======
- Generate DB with random data by reading its schema


