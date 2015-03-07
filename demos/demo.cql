

DROP KEYSPACE IF EXISTS casstcl_tests;

CREATE KEYSPACE casstcl_tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

USE casstcl_tests;

DROP TABLE IF EXISTS numbers;

CREATE TABLE numbers (
	row int,
	mylist list<text>,
	myset set<text>,
	mymap map<int,text>,
	primary key(row)
);
