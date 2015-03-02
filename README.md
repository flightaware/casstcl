CassTcl, a Tcl C extension providing a Tcl interface to the Cassandra database
===

CassTcl provides a Tcl interface to the Cassandra cpp-driver C/C++ API.

License
---

Open source under the permissive Berkeley copyright, see file LICENSE

Requirements
---
Requires the Datastaxx cpp-driver be installed.  (https://github.com/datastax/cpp-driver)

Building
---

```sh
autoconf
configure
make
sudo make install
```

For FreeBSD, something like

```sh
./configure --with-tcl=/usr/local/lib/tcl8.6  --mandir=/usr/local/man --enable-symbols
```

Accessing from Tcl
---

```tcl
package require casstcl
```


CassTcl objects
---

CassTcl provides object creation commands...

::casstcl::cass create handle, to create a cassandra handling object.

```tcl
set cass [::casstcl::cass create #auto]

$cass exec "insert into foo ..."
```

...or...

```tcl
::casstcl::cass create cass

cass exec "insert into foo ..."
```


Methods of cassandra cluster interface object
---

```tcl
    set ::cassdb [::cassandra::connect args]
```

* *$cassdb* **exec** *$statement**

Perform the requested CQL statement.  Waits for it to complete. Either it works or you get a Tcl error.

* *$cassdb* **async** *?-callback callbackRoutine?* *?-table tableName?* *?-array arrayName?* *?-batch batchObjectName?* *?$statement?* *?arg...?*

Perform the requested CQL statement.  Does not wait.  Creates a result object called a *future* object that you can use the methods of to find out the status of your statement and iterate over select results. Allows for considerable performance gains over exec at the cost of greater code complexity.

If the **-callback** argument is specified then the next argument is a callback routine that will be invoked when the Cassandra request has completed or errored or whatnot.  The callback routine will be invoked with a single argument, which is the name of the future object created (such as *::future17*) when the request was made.

If -batch is specified the argument is a batch object and that is used as the source.

If *-table* is specified it is the fully qualified name of a table and *-array* is also required, and vice versa.  These specify the affected table name and an array that the data elements will come from.  Args are zero or more arguments which are element names for the array and also legal column names for the table.  This technology will infer the data types and handle them behind your back as long as import_column_type_map has been run on the connection.

If neither *-table*, *-array* or *-batch* has been specified, the arguments to the right of the statement need to be alternating between data and data type, like *14 int 3.7 float*.  This is the simplest for casstcl but requires the code to be more intimate with the data types than it otherwise would be.  If you use this style and you change a data type in the schema you also have to change it in the code.  So we don't like it.

See also the future object.

* **$cassdb select** *?-pagesize n?* **$statement array code**

Iterate filling array with results of the select statement and executing code upon it.  break, continue and return from the code is supported.

Null values are unset from the array, so you have to use info exists to see if a value is not null.

The array is cleared each iteration for you automatically so you don't neeed to jack with unset.

If the **-pagesize** argument is present then it should be followed by an integer which is the number of query results that should be returned "per pass".  Changing this should transparent to the caller but smaller pagesize numbers should allow greater concurrency in many cases by allowing the application to process some results while the cluster is still producing them.  The default pagesize is 100 rows.

* *$cassdb* **connect** *?keyspace?*

Connect to the cassandra cluster.  Use the specified keyspace if the keyspace argument is present.  Alternatively after connecting you can specify the keyspace to use with something like

```tcl
$cassdb exec "use wx;"
```

* *$cassdb* **prepare** *$statement*

Prepare the specified statement.  Although this command will probably work, the casstcl infrastructure in support of prepared statements including binding values into statements does not (yet) exist.

* *$cassdb* **keyspaces**

Return a list of all of the keyspaces known to the cluster.

* *$cassdb* **tables** *$keyspace*

Return a list of all of the tables within the named keyspace.

* *$cassdb* **columns** *$keyspace* *$table*

Returns a list of the names of all of the columns within the specified table within the specified keyspace.

* *$cassdb* **columns_with_types** *$keyspace* *$table*

Returns a list of key-value pairs consisting of all of the columns and their Cassandra data types.

All of the schema-accessing functions, *list_keyspaces*, *list_tables*, *list_columns* and *list_column_types* locate this information using the metadata access capabilities provided by the cpp-driver.  As the cpp-driver will update its metadata on the fly as changes to the schema are made, the schema-accessing functions likewise will reflect any changes in the schema if called subsequently to schema changes occurring.

* *$cassdb* **reimport_type_map**

Casstcl maintains the data type of each column for each table for each schema in the cluster.  As the metadata can change on the fly, long-running programs that want to try to adapt to changes in the cluster schema can invoke this to regenerate casstcl's column-to-datatype mapping cache.

* *$cassdb* **set_contact_points** *$addressList*

Provide a list of one or more addresses to contact the cluster at.

The first call sets the contact points and any subsequent calls appends additional contact points. Passing an empty string will clear the contact points. White space is striped from the contact points.

* *$cassdb* **set_port** *$port*

Specify a port to connect to the cluster on.  Since the cpp-driver uses the native binary CQL protocol, this would normally be 9042.

* *$cassdb* **set_protocol_version** *$protocolVersion*

Sets the protocol version. This will theoretically automatically downgrade to protocol version 1 if necessary.  The default value is 2.

* *$cassdb* **set_num_threads_io** *$numThreads*

Set the number of IO threads. This is the number of threads that will handle query requests.  The default is 1.

* *$cassdb* **set_queue_size_io** *$queueSize*

Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* *$cassdb* **set_queue_size_event** *$queueSize*

Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* *$cassdb* **set_queue_size_log** *$logSize*

Sets the size of the the fixed size queue that stores log messages.  The default is 4096.

* *$cassdb* **set_core_connections_per_host** *$numConnections*

Sets the number of connections made to each server in each IO thread.  Defaults to 1.

* *$cassdb* **set_max_connections_per_host** *$numConnections*

Sets the maximum number of connections made to each server in each IO thread.  Defaults to 2.

* *$cassdb* **set_max_concurrent_creation** *$numConnections*

Sets the maximum number of connections that will be created concurrently.  Connections are created when the current connections are unable to keep up with request throughput.  The default is 1.

* *$cassdb* **set_max_concurrent_requests_threshold** *$numRequests*

Sets the threshold for the maximum number of concurrent requests in-flight on a connection before creating a new connection.  The number of new connections created will not exceed max_connections_per_host.  Default 100.

* *$cassdb* **set_connect_timeout** *$timeoutMS*

Sets the timeout for connecting to a node.  Timeout is in milliseconds.  The default is 5000 ms.

* *$cassdb* **set_request_timeout** *$timeoutMS*

Sets the timeout for waiting for a response from a node.  Timeout is in milliseconds.  The default is 12000 ms.

* *$cassdb* **set_reconnect_wait_time** *$timeoutMS*

Sets the amount of time to wait before attempting to reconnect.  Default 2000 ms.

* *$cassdb* **set_credentials** *$user $password*

Set credentials for plaintext authentication.

* *$cassdb* **set_tcp_nodelay** *$enabled*

Enable/Disable Nagel's algorithm on connections.  The default is disabled.

* *$cassdb* **set_token_aware_routing** *$enabled*

Configures the cluster to use Token-aware request routing, or not.

This routing policy composes the base routing policy, routing requests first to replicas on nodes considered 'local' by the base load balancing policy.

The default is enabled.

This routing policy composes the base routing policy, routing requests first to replicas on nodes considered local by the base load balancing policy.

* *$cassdb* **set_tcp_keepalive** *$enabled $delaySecs*

Enables/Disables TCP keep-alive.  Default is disabled.  delaySecs is the initial delay in seconds; it is ignored when disabled.

* *$cassdb* **delete**

Disconnect from the cluster and delete the cluster connection object.

* *$cassdb* **close**

Disconnect from the cluster.

Batches
---

Batching CQL can yield a huge performance increase.

While you can construct batches out of straight CQL with BEGIN BATCH and APPLY BATCH, casstcl provides batch objects to help you construct, manage and use batches.

Create a new batch using the batch method of the cassandra object:

```tcl
$cassdb create mybatch
set mybatch [$cassdb create #auto]
```

Both styles work.

* *$batch* **add** *$statement*

Adds the specified statement to the batch.

* *$batch* **consistency** *?$consistencyLevel?*

Sets the batch write consistency level to the specified level.  Consistency must be **any**, **one**, **two**, **three**, **quorum**, **all**, **local_quorum**, **each_quorum**, **serial**, **local_serial**, or **local_one**.

* *$batch* **upsert** *$table* *$list*

Generate in upsert into the batch.  List contains key value pairs where keys should be columns that exist in the fully qualified table.

A typical usage would be:

```tcl
$batch upsert wx.wx_metar [array get row]
```

* *$batch* **reset**

Reset the batch by deleting all of its data.

* *$batch* **delete**

Delete the batch object and all of its data.

Configuring SSL Connections
---

* *$cassdb* **set_ssl_cert** *$pemFormattedCertString*

This sets the client-side certificate chain.  This is used by the server to authenticate the client.  This should contain the entire certificate chain starting with the certificate itself.

* *$cassdb* **set_ssl_private_key** *$pemFormattedCertString $password*

This sets the client-side private key.  This is by the server to authenticate the client.

* *$cassdb* **add_trusted_cert** *$pemFormattedCertString*

This adds a trusted certificate.  This is used to verify the peer's certificate.

* *$cassdb* **set_ssl_verify_flag** *$flag*

This sets the verification that the client will perform on the peer's certificate.  **none** selects that no verification will be performed, while **verify_peer_certificate** will verify that a certificate is present and valid.

Finally, **verify_peer_identity** will match the IP address to the certificate's common name or one of its subject alternative names.  This implies that the certificate is also present.

Future Objects
---

Future objects are created when casstcl's async method is invoked.  It is up to the user to ensure that the objects returned by the async method have their own methods invoked to enquire as to the status and ultimate disposition of their requests.  After you're finished with a future object, you should delete it.

```tcl
set future [$cassObj async "select * from wx_metar where airport = 'KHOU' order by time desc limit 1"]

# see if the request was successful
if {[$future isready]} {
	if {[$future status] != "CASS_OK"} {
		set errorString [$future error_message]
	}
}
```

* *$future* **isready**

Returns 1 if the future (query result) is ready, else 0.

* *$future* **wait** *?us?*

Waits for the request to complete.  If the optional argument us is specified, times out after that number of microseconds.

* *$future* **foreach** *rowArray code*

Iterate through the query results, filling the named array with the columns of the row and their values and executing code thereupon.

* *$future* **status**

Return the cassandra status code converted back to a string, like CASS_OK and CASS_ERROR_SSL_NO_PEER_CERT and whatnot.  If it's anything other than CASS_OK then whatever you did didn't work.

* *$future* **error_message**

Return the cassandra error message for the future, empty string if none.

* *$future* **delete**

Delete the future.  Care should be taken to delete these when done with them to avoid leaking memory.

Casstcl logging callback
---

The Cassandra cpp-driver supports custom logging callbacks to allow an application to receive logging messages rather than having them go to stderr, which is what happens by default, and casstcl provides Tcl-level support for this capability.

The logging callback is defined like this:

```tcl
::casstcl::cass set_logging_callback callbackFunction
```

When a log message callback occurs from the Cassandra cpp-driver, casstcl will obtain that and invoke the specified callback function one argument containing a list of key value pairs representing the (currently six) things that are received in a logging object:

* "clock" and a floating point epoch clock with millisecond accuracy

* "severity" and the cassandra log level as a string.  value will be one of **disabled**, **critical**, **error**, **warn**, **info**, **debug**, **trace** or **unknown**.

* "file" and possibly the name of some file, or an empty string... not sure what it is, maybe the source file of the cpp-driver that is throwing the error.

* "line" and some line number, not sure what it is, probably the line number of the source file of the cpp-driver that is throwing the error

* "function" and the name of some function, probably the cpp-driver function that is throwing the error

* "message" and the error message itself

A sample implementation:

```tcl
proc logging_callback {pairList} {
    puts "logging_callback called..."
    array set log $pairList
    parray log
    puts ""
}

casstcl::cass set_logging_callback logging_callback
```

According to the cpp-driver documentation, logging configuration should be done before calling any other driver function, so if you're going to use this, invoke it after package requiring casstcl and before invoking ::casstcl::cass create.

Also note that logging is global.  That is, it's not tied to a specific connection if you had more than one connection for some reason.  Also currently it is not fully cleaned up if you unload the package.

Casstcl library functions
---

The library functions such as *download_schema*, *list_schema*, *list_tables*, etc, have been removed.  These capabilities are now provided by the metadata-traversing methods *list_keyspaces*, *list_tables*, *list_columns* and *list_column_types* documented above.


* **::casstcl::validator_to_type** *$type*

Given a validator type such as "org.apache.cassandra.db.marshal.AsciiType" and return the corresponding Cassandra type.  Can decode complex type references including reversed, list, set, and map.  This function is used by casstcl's *list_column_types* method.

Bugs
---

Some Cassandra data types are unimplemented or broken.  Specifically *unknown*, *custom*, *decimal*, *varint*, and *timeuuid* either don't or probably don't work.

Memory leaks are a distinct possibility.

The code is new and therefore hasn't gotten a lot of use.  Nonetheless, the workhorse functions **async**, **exec** and **select** seem to work pretty well.
