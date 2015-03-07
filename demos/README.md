

assumes you have a cluster on 127.0.0.1 port 9042

if you don't, set up a .casstclrc file in your home directory that is something like

```tcl
set ::params(host) 172.16.32.1
```

To create the test keyspace and tables, feed to cqlshrc the file **demo.cql** or run

```sh
tclsh run.tcl -file demo.cql
```

Run the program to insert into casstcl_test.numbers:

```sh
tclsh numbers.tcl
```

Poke around in cqlsh and do a select on that

Run selnumbers.tcl to see the row in casstcl_test.numbers:

```sh
tclsh selnumbers.tcl
```


