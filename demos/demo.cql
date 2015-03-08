

DROP KEYSPACE IF EXISTS casstcl_test;

CREATE KEYSPACE casstcl_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

USE casstcl_test;

DROP TABLE IF EXISTS numbers;

CREATE TABLE numbers (
	row int,
	mylist list<text>,
	myset set<text>,
	mymap map<int,text>,
	primary key(row)
);

CREATE TABLE rock_blobster (
	row int,
	blobbie blob,
	primary key(row)
);
