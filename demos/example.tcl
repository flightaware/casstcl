
# mac
lappend auto_path /usr/local/lib

package require casstcl

set cass [::casstcl::cass create #auto]

$cass set_contact_points 127.0.0.1
$cass set_port 9042
$cass connect

$cass exec "CREATE KEYSPACE examples WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };"

$cass exec "CREATE TABLE examples.basic (key text, bln boolean, flt float, dbl double, i32 int, i64 bigint, PRIMARY KEY (key));"

$cass  exec "insert into examples.basic (key, bln, flt, dbl, i32, i64) values ('1', true, 0.001, 0.0002, 1, 2);"

$cass select "select * from examples.basic;" row {parray row}


