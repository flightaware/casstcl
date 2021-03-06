CassTcl, a Tcl language interface to the Cassandra database
===

CassTcl provides a Tcl interface to the Cassandra database using the cpp-driver C/C++ API.

Functionality
---

- Completely asynchronous
- Provides a natural Tcl interface
- Works out and manages all the data type conversions behind your back
- Thread safe
- Ad-hoc queries in CQL
- Supports prepared statements
- Supports batches
- Supports all types of Cassandra collections
- Compatible with binary protocol version 1 and 2
- Supports authentication via credentials using SASL PLAIN
- Supports SSL
- Production quality
- Free!

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

To connect to a cluster (example):

```tcl
set cass [::casstcl::cass create #auto]

$cass contact_points $host
$cass port $port ;# 9042
$cass credentials $user $password
$cass connect
```

Or a convenience one-liner:

```tcl
set cass [::casstcl::connect -host $host -port $port -user $user -password $password]
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
set cassdb [::casstcl::cass create cass]

cass exec "insert into foo ..."
```


Methods of cassandra cluster interface object
---

* *$cassdb* **connect** *?-callback callbackRoutine?* *?keyspaceName?*

 Attempts to connect to the specified database, optionally setting the keyspace. Since the keyspace can be changed in CQL, this is not tracked and not recommended ... always provide a fully qualified table name.

 If the **-callback** argument is specified then the next argument is a callback routine that will be invoked when successfully connected.

 If the connection fails, the callback may not be invoked (i.e. a script error may be generated instead).

 The callback routine will be invoked with a single argument, which is the name of the future object created (such as *::future17*) when the request was made.

* *$cassdb* **exec** *?-callback callbackRoutine?* *?-head?* *?-error_only?* *?-table tableName?* *?-array arrayName?* *?-prepared preparedObjectName?* *?-batch batchObjectName?* *?-consistency consistencyLevel?* *$request* *?arg...?*

* *$cassdb* **async** *?-callback callbackRoutine?* *?-head?* *?-table tableName?* *?-array arrayName?* *?-prepared preparedObjectName?* *?-batch batchObjectName?* *?-consistency consistencyLevel?* *?$request?* *?arg...?*

* *$cassdb* **exec** *?-callback callbackRoutine?* *?-head?* *?-error_only?* *-upsert* *?-mapunknown columnName?* *?-nocomplain?* *?-ifnotexists?* *tableName* *argList*

* *$cassdb* **async** *?-callback callbackRoutine?* *?-head?* *-upsert* *?-mapunknown columnName?* *?-nocomplain?* *?-ifnotexists?* *tableName* *argList*

 Perform a request.  The request is normally a CQL statement.  Waits for it to complete if **exec** is used without **-callback** (synchronous).   Does not wait if **async** is used or **exec** is used with **-callback** (asynchronous).

 If synchronous then tcl waits for the cassandra call to complete and gives you back an error if an error occurs.

 If used asynchronously then the request is issued to cassandra and a result object called a *future* object is created and returned.

 You can use the methods of the future object to find out the status of your statement, iterate over select results, etc. Asynchronous operation allows for considerable performance gains over synchronous at the cost of greater code complexity.

 If the **-callback** argument is specified then the next argument is a callback routine that will be invoked when the Cassandra request has completed or errored or whatnot.  The callback routine will be invoked with a single argument, which is the name of the future object created (such as *::future17*) when the request was made.

 If **-head** is specified then when the callback even occurs it is queued at the head of the notifier queue rather than at the tail, causing the event to be processed ahead of other events already in the queue.  This can be useful when you are getting a lot of events to make sure that your important cassandra completion events get priority above, say, the events that are causing your casstcl batches to get added to.

 **-error_only** instructs casstcl to only call the callback function on error.  If error-only is specified and the callback from the cassandra cpp-driver indicates the asynchronous request was successful, the future object is deleted and the callback is not taken.  Assuming most requests are succeeding this greatly reduces invocations of the Tcl interpreter, and can bring a major performance increase.

 If **-batch** is specified the request is a batch object and that is used as the source of the statement(s).

 If **-table** is specified it is the fully qualified name of a table and *-array* is also required, and vice versa.  These specify the affected table name and an array that the data elements will come from.  Args are zero or more arguments which are element names for the array and also legal column names for the table.  This technology will infer the data types and handle them behind your back as long as import_column_type_map has been run on the connection.

 If **-prepared** is specified it is the name of a prepared statement object and the final argument is a list of key value pairs where the key corresponds to the name of a value in the prepared statement and the value is to be correspondingly bound to the matching **?** argument in the statement.

 If **-consistency** is specified it is the consistency level to use for any created statement(s).  Cannot be used with **-batch**.

 If **-upsert** is specified then the final arguments are a table name and a list of key-value pairs where the key corresponds to the name of a column and the value corresponds to the new value for that column. The new values will be "upserted" into the table based on the primary key.

 If **-mapunknown columnName** is specified then unknown arguments in the upsert key-value list will be mapped to a *map* column.

 If **-nocomplain** is specified then unknown arguments in the upsert key-value list will be silently ignored.

 If **-ifnotexists** is specified then the row is only inserted if it doesn't already exist.

 If none of *-table*, *-array*, *-batch*, *-upsert*, or *-prepared* have been specified, the arguments to the right of the statement need to be alternating between data and data type, like *14 int 3.7 float*.  This is the simplest for casstcl but requires the code to be more intimate with the data types than it otherwise would be.  If you use this style and you change a data type in the schema you also have to change it in the code.  So we don't like it.

 See also the future object.

* *$cassdb* **select** *?-pagesize n?* *?-consistency consistencyLevel?* *?-withnulls?* **$statement array code**

 Iterate filling array with results of the select statement and executing code upon it.  break, continue and return from the code is supported.

 Unless you specify *-withnulls*, null values are unset from the array, so you have to use info exists to see if a value is not null.

 The array is cleared each iteration for you automatically so you don't neeed to jack with unset.

 If the **-pagesize** argument is present then it should be followed by an integer which is the number of query results that should be returned "per pass".  Changing this should transparent to the caller but smaller pagesize numbers should allow greater concurrency in many cases by allowing the application to process some results while the cluster is still producing them.  The default pagesize is 100 rows.

 If the **-consistency** argument is present then it should be followed by a consistency level, which will be used when creating any statement(s).

* *$cassdb* **prepare** *objName* *tableName* *$statement*

 Prepare the specified statement and creates a prepared object named *objName*.  Although the table name shouldn't technically need to be specified, since the cpp-driver API doesn't provide access to the data types of the elements of the statement (yet; they have a ticket open to do it), we need the table name so we can look up the data types of the values being substituted when a prepared statement is being bound.  (It's OK to specify the table name since Cassandra doesn't support joins and whatnot so there shouldn't be more than one table referenced.)

 The result is a prepared statement object that currently has two methods, **delete**, which does the needful, and **statement**, which returns the string that was prepared as a statement.  The important thing is that prepared objects can be passed as arguments to the **batch**, **async** and **exec** methods.

Here's an example of defining a prepared statement and a subsequent use of it to add to a batch.

```tcl
    set ::positionsPrepped [$::cass prepare #auto hummingbird.latest_positions_by_facility "INSERT INTO hummingbird.latest_positions_by_facility (facility, hexid, c, s, type, ident, t, alt, clock, gs, heading, lat, lon, reg, squawk, aircrafttype) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL 3600;"]

    $::batch add -prepared $::positionsPrepped [array get row]
```

* *$cassdb* **cluster_version**

 Return the Cassandra cluster version as a list of {major minor patchlevel}.

 Requires CPP driver >= v2.3.0

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

* *$cassdb* **contact_points** *$addressList*

 Provide a list of one or more addresses to contact the cluster at.

 The first call sets the contact points and any subsequent calls appends additional contact points. Passing an empty string will clear the contact points. White space is striped from the contact points.

* *$cassdb* **port** *$port*

 Specify a port to connect to the cluster on.  Since the cpp-driver uses the native binary CQL protocol, this would normally be 9042.

* *$cassdb* **protocol_version** *$protocolVersion*

 Sets the protocol version. This will theoretically automatically downgrade to protocol version 1 if necessary.  The default value is 2.

* *$cassdb* **heartbeat_interval** *heartbeatInterval*

 To prevent intermediate network devices from disconnecting pooled connections due to inactivity, the driver periodically sends a lightweight heartbeat request, by default every 30 seconds.  This can be changed with this method; use zero to disable connection heartbeats.

* *$cassdb* **whitelist_filtering** *?hostList?*

 Limit cassandra host connection attempts to the set of hosts provided in *hostList*.  It's not a good idea to use this to limit connections to hosts in a local data center; please see the cpp-driver documentation for details.

* *$cassdb* **num_threads_io** *$numThreads*

 Set the number of IO threads. This is the number of threads that will handle query requests.  The default is 1.

* *$cassdb* **queue_size_io** *$queueSize*

 Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* *$cassdb* **queue_size_event** *$queueSize*

 Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* *$cassdb* **queue_size_log** *$logSize*

 Sets the size of the the fixed size queue that stores log messages.  The default is 4096.

* *$cassdb* **core_connections_per_host** *$numConnections*

 Sets the number of connections made to each server in each IO thread.  Defaults to 1.

* *$cassdb* **max_connections_per_host** *$numConnections*

 Sets the maximum number of connections made to each server in each IO thread.  Defaults to 2.

* *$cassdb* **max_concurrent_creation** *$numConnections*

 Sets the maximum number of connections that will be created concurrently.  Connections are created when the current connections are unable to keep up with request throughput.  The default is 1.

* *$cassdb* **max_concurrent_requests_threshold** *$numRequests*

 Sets the threshold for the maximum number of concurrent requests in-flight on a connection before creating a new connection.  The number of new connections created will not exceed max_connections_per_host.  Default 100.

* *$cassdb* **max_requests_per_flush** *$numRequests*

 Set the maximum number of requests processed by an I/O worker per flush.  Default 128.

* *$cassdb* **write_bytes_high_water_mark** *$highWaterMarkBytes*

 Sets the high water mark for the number of bytes outstanding on a connection.  Disables writes to a connection if the number of bytes queued exceeds this value. cpp-driver documented default is 64 KB.

* *$cassdb* **write_bytes_low_water_mark** *$lowWaterMarkBytes*

 Sets the low water mark for the number of bytes outstanding on a connection.  After exceeding high water mark bytes, writes will only resume once the number of outstanding bytes falls below this value.  Default is 32 KB.

* *$cassdb* **pending_requests_high_water_mark** *$highWaterMark*

 Sets the high water mark for the number of requests queued waiting for a connection in a connection pool.  Disables writes to a host on an IO worker if the number of requests queued exceeds this value.  Default is 128 * max_connections_per_host.

* *$cassdb* **pending_requests_low_water_mark** *$lowWaterMark*

 Sets the low water mark for the number of requests queued waiting for a connection in a connection pool.  After exceeding high water mark requests, writes to a host will only resume once the number of requests falls below this value.  Default is 64 * max_connections_per_host.

* *$cassdb* **connect_timeout** *$timeoutMS*

 Sets the timeout for connecting to a node.  Timeout is in milliseconds.  The default is 5000 ms.

* *$cassdb* **request_timeout** *$timeoutMS*

 Sets the timeout for waiting for a response from a node.  Timeout is in milliseconds.  The default is 12000 ms.

* *$cassdb* **reconnect_wait_time** *$timeoutMS*

 Sets the amount of time to wait before attempting to reconnect.  Default 2000 ms.

* *$cassdb* **credentials** *$user $password*

 Set credentials for plaintext authentication.

* *$cassdb* **tcp_nodelay** *$enabled*

 Enable/Disable Nagel's algorithm on connections.  The default is disabled.

* *$cassdb* **load_balance_round_robin**

 Requests that the driver discover all nodes in a cluster and cycles through the per request.  All are considered local.

* *$cassdb* **load_balance_dc_aware** *localDC* *usedHostsPerRemoteDC* *allowRemoteDCs*

 Configures the cluster to use data center-aware load balancing.  For each query all the live inodes in the local data center are tried first, followed by any node from other data centers.

 This is the default and does not need to called unless switching an existing from another policy or changing settings.

 Without further configuration a default local data center is chosen from the first connected contact point and no remote hosts are considered in query plans.  If you use this mechanism be sure to use only contact points from the local data center.

 *localDC* is the name of the data center to try first.  *usedHostsPerRemoteDC* is the number of hosts used in each remote data center if no hosts are available in the local data center.  *allowRemoteDCs* is a boolean.  If true it allows remote hosts to be used to no local data center hosts are available and the consistency level is *one* or *quorum*.

* *$cassdb* **token_aware_routing** *$enabled*

 Configures the cluster to use Token-aware request routing, or not.

 This routing policy composes the base routing policy, routing requests first to replicas on nodes considered 'local' by the base load balancing policy.

 The default is enabled.

* *$cassdb* **latency_aware_routing** *$enabled*

 Configures the cluster to use latency-aware request routing, or not.

 This routing policy composes the base routing policy tracks the exponentially weighted moving average of query latencies to nodes in the cluster.  If a given node's latency exceeds an exclusion threshold, it is no longer queried.

* *$cassdb* **tcp_keepalive** *$enabled $delaySecs*

 Enables/Disables TCP keep-alive.  Default is disabled.  delaySecs is the initial delay in seconds; it is ignored when disabled.

* *$cassdb* **delete**

 Disconnect from the cluster and delete the cluster connection object.

* *$cassdb* **close**

 Disconnect from the cluster.

* *$cassdb* **metrics**

 Obtain metrics from the cassandra cluster and return them as a list of key-value pairs.  The returned values and their meanings are as follows:

|key|value
----|-----
requests.min|requests minimum in microseconds
requests.max|requests maximum in microseconds
requests.mean|requests mean in microseconds
requests.stddev|requests standard deviation in microseconds
requests.median|requests median in microseconds
requests.percentile_75th|75th percentile in microseconds
requests.percentile_95th|95th percentile in microseconds
requests.percentile_98th|98th percentile in microseconds
requests.percentile_99th|99th percentile in microseconds
requests.percentile_999th|999th percentile in microseconds
requests.mean_rate|mean rate in requests per second
requests.one_minute_rate|one-minute rate in requests per second
requests.five_minute_rate|five-minute rate in requests per second
requests.fifteen_minute_rate|fifteen-minute rate in requests per second
stats.total_connections|the total number of connections
stats.available_connections|the number of connections available to take requests
stats.exceeded_pending_requests_water_mark|occurrences when requests exceeded a pool's high water mark
stats.exceeded_write_bytes_water_mark|occurrences when number of bytes exceeded a pool's high water mark
errors.connection_timeouts|occurrences of a connection timeout
errors.pending_request_timeouts|Occurrences of requests that timed out waiting for a connection
errors.request_timeouts|Occurrences of requests that timed out waiting for a request to finish

Batches
---

Batches are a way to provide atomicity, so if anything in a batch fails, none of the actions occur.

While you can construct batches out of straight CQL with BEGIN BATCH and APPLY BATCH, casstcl provides batch objects to help you construct, manage and use batches.

Create a new batch using the batch method of the cassandra object:

```tcl
$cassdb batch mybatch
set mybatch [$cassdb batch #auto]
```

Both styles work.

The default batch type is *logged*.  An optional argument to the batch creation command is the batch type, which can be *logged*, *unlogged* or *counter*.

```tcl
set mybatch [$cassdb batch #auto unlogged]
```

* *$batch* **add** *?-table tableName?* *?-array arrayName?* *?-prepared preparedObjectName? ?args..?

 Adds the specified statement to the batch. Processes arguments similarly to the **exec** and **async** methods.

* *$batch* **consistency** *?$consistencyLevel?*

 Sets the batch write consistency level to the specified level.  Consistency must be **any**, **one**, **two**, **three**, **quorum**, **all**, **local_quorum**, **each_quorum**, **serial**, **local_serial**, or **local_one**.

* *$batch* **upsert** ?-mapunknown mapcolumn? ?-nocomplain? ?-ifnotexists? *$table* *$list*

 Generate in upsert into the batch.  List contains key value pairs where keys should be columns that exist in the fully qualified table.

 A typical usage would be:

```tcl
$batch upsert wx.wx_metar [array get row]
```

 If *-nocomplain* is specified then any column names not found in the column map for the specified table are silently dropped along with their values.

 If *-mapunknown* is specified then an additional argument containing a column name should be specified.  With this usage any column names not found in the column map are written to a map collection of the specified column name.  The column name must exist as a column in the table and be of the *map* type and the key and values types of the map must be *text*.

* *$batch* **count**

 Return the number of rows added to the batch using *add* or *upsert*.

* *$batch* **reset**

 Reset the batch by deleting all of its data.

* *$batch* **delete**

 Delete the batch object and all of its data.

Note on batch size
----

As of Cassandra 3.1 there is a limit on the maximum size of a batch in the vicinity of 16 megabytes.  Generating batches that are too large will result in "no hosts available" when trying to send the batch and illegal argument exceptions on the backend from java about a mutation being too large.

Note on batch performance
----

Batching isn't for performance.  Google for details.  However since batching reduces the number of callbacks, casstcl can run faster with batches if it is heavily loaded.  A recent addition to the async and exec methods, **-error_only** specifies that completion callbacks are only done for errors and this improves casstcl throughput considerably when under heavy load.

Note on logged versus unlogged batches
----

casstcl batches are logged by default.  From the Cassandra documentation:

 Cassandra uses a batch log to ensure all operations in a batch are applied atomically. (Note that the operations are still only isolated within a single partition.)

 There is a performance penalty for batch atomicity when a batch spans multiple partitions. If you do not want to incur this penalty, you can tell Cassandra to skip the batchlog with the UNLOGGED option. If the UNLOGGED option is used, operations are only atomic within a single partition.

Configuring SSL Connections
---

* *$cassdb* **ssl_cert** *$pemFormattedCertString*

 This sets the client-side certificate chain.  This is used by the server to authenticate the client.  This should contain the entire certificate chain starting with the certificate itself.

* *$cassdb* **ssl_private_key** *$pemFormattedCertString $password*

 This sets the client-side private key.  This is used by the server to authenticate the client.

* *$cassdb* **add_trusted_cert** *$pemFormattedCertString*

 This adds a trusted certificate.  This is used to verify the peer's certificate.

* *$cassdb* **ssl_verify_flag** *$flag*

 This sets the verification that the client will perform on the peer's certificate.  **none** selects that no verification will be performed, while **verify_peer_certificate** will verify that a certificate is present and valid.

 Finally, **verify_peer_identity** will match the IP address to the certificate's common name or one of its subject alternative names.  This implies that the certificate is also present.

* *$cassdb* **ssl_enable**

 This attaches the SSL context configured using the aforementioned SSL methods to our cassandra cluster structure and enables SSL.

Future Objects
---

Future objects are created when casstcl's async or exec methods are invoked with a callback specified.  It is up to the user to ensure that the future objects returned have their methods invoked to enquire as to the status and ultimate disposition of their requests.  The results of selects, for instance, can be iterated using the object's **foreach** method.  After you're finished with a future object, you should delete it.

```tcl
set future [$cassObj async "select * from wx_metar where airport = 'KHOU' order by time desc limit 1"]

# see if the request was successful
if {[$future isready]} {
	if {[$future status] != "CASS_OK"} {
		set errorString [$future error_message]
	}
}

$future foreach row {
	parray row
	puts ""
}

$future delete
```

* *$future* **isready**

 Returns 1 if the future (query result) is ready, else 0.

* *$future* **wait** *?us?*

 Waits for the request to complete.  If the optional argument *us* is specified, times out and returns after that number of microseconds have elapsed without the request having completed.

* *$future* **foreach** *rowArray code*

 Iterate through the query results, filling the named array with the columns of the row and their values and executing code thereupon.

* *$future* **status**

 Return the cassandra status code converted back to a string, like CASS_OK and CASS_ERROR_SSL_NO_PEER_CERT and whatnot.  If it's anything other than CASS_OK then whatever you did didn't work.

* *$future* **error_message**

 Return the cassandra error message for the future, or an empty string if none.

* *$future* **delete**

 Delete the future.  Delete futures when you are done with them or you will leak memory.

Exec and select can kind of hide the future object to make things simpler.  Sometimes you may like to go synchronously because given the nature of your application you're willing to wait for the result and you don't want to jack around with callbacks, yadda.  Great.  Exec and select are normally pretty good for that but future objects have some capabilities you can't get with them so you can do an async without the callback then wait immediately on the future object.  Remember to delete your future objects when you're done with them.

```tcl
set future [$cassObj async "select * from wx_metar where airport = 'KHOU' order by time desc limit 1"]
$future wait
if {[$future status] != "CASS_OK"} {
	set errorString [$future error_message]
}
...
$future delete
```

Casstcl logging callback
---

The Cassandra cpp-driver supports custom logging callbacks to allow an application to receive logging messages rather than having them go to stderr, which is what happens by default, and casstcl provides Tcl-level support for this capability.

The logging callback is defined like this:

```tcl
::casstcl::cass logging_callback callbackFunction
```

When a log message callback occurs from the Cassandra cpp-driver, casstcl will obtain that and invoke the specified callback function one argument containing a list of key value pairs representing the (currently six) things that are received in a logging object:

* "clock" and a floating point epoch clock with millisecond accuracy

* "severity" and the cassandra log level as a string.  value will be one of **disabled**, **critical**, **error**, **warn**, **info**, **debug**, **trace** or **unknown**.

* "file" and possibly the name of some file, or an empty string... not sure what it is, maybe the source file of the cpp-driver that is throwing the error.

* "line" and some line number, not sure what it is, probably the line number of the source file of the cpp-driver that is throwing the error

* "function" and the name of some function, probably the cpp-driver function that is throwing the error

* "message" and the error message itself

The level of detail queued to the logging callback may be adjusted via:

```tcl
::casstcl::cass log_level level
```

This sets the log level for the cpp-driver.  The legal values, in order from least output (none) to most output (all) are "disabled", "critical", "error", "warn", "info", "debug", and "trace".

The default is "warn".

A sample implementation:

```tcl
proc logging_callback {pairList} {
    puts "logging_callback called..."
    array set log $pairList
    parray log
    puts ""
}

casstcl::cass logging_callback logging_callback
casstcl::cass log_level info
```

According to the cpp-driver documentation, logging configuration should be done before calling any other driver function, so if you're going to use this, invoke it after package requiring casstcl and before invoking ::casstcl::cass create.

Also note that logging is global.  That is, it's not tied to a specific connection if you had more than one connection for some reason.  Also currently it is not fully cleaned up if you unload the package.

It is important to at least capture this information as useful debuggin information sometimes comes back through this interface.  For example you might just get a "Query error" back from the library but then you get a logging callback that provides much more information, such as

```
:371:int32_t cass::decode_buffer_size(const cass::Buffer &)): Routing key cannot contain an empty value or a collection
```

The above messages makes the problem much more clear.  The caller probably didn't specify a value for the primary key.

Casstcl library functions
---

The library functions such as *download_schema*, *list_schema*, *list_tables*, etc, have been removed.  These capabilities are now provided by the metadata-traversing methods *list_keyspaces*, *list_tables*, *list_columns* and *list_column_types* documented above.


* **::casstcl::validator_to_type** *$type*

 Given a validator type such as "org.apache.cassandra.db.marshal.AsciiType" and return the corresponding Cassandra type.  Can decode complex type references including reversed, list, set, and map.  This function is used by casstcl's *list_column_types* method.

* **::casstcl::quote** *$value* *$type*

 Return a quote of the specified value according to the specified cassandra type.

* **casstcl::typeof** *table.columnName*

* **casstcl::quote_typeof** *table.columnName* *value* *?subType?*

 Quote the value according to the field *columnName* in table *table*.  *subType* can be **key** for collection sets and lists and can be **key** or **value** for collection maps.  For these usages it returns the corresponding data type.  If the subType is specified and the column is a collection you get a list back like *list text* and *map int text*.

* **casstcl::assemble_statement** *statementVar* *line*

 Given the name of a variable (initially set to an empty string) to contain a CQL statement, and a line containing possibly a statement or part of a statement, append the line to the statement and return 1 if a complete statement is present, else 0.

 Comments and blank lines are skipped.

 (The check for a complete statement is the mere presence of a semicolon anywhere on the line, which is kind of meatball.)

* **run_fp** *cassdb* *fp*

 Read from a file pointer and execute complete commands through the specified cassandra object.

* **run_file** *cassdb* *fileName*

 Run CQL commands from a file.

* **interact** *cassdb*

 Enter a primitive cqlsh-like shell.  Provides a prompt of *tcqlsh>* when no partial command has been entered or *......>* when a partial command is present.  Once a semicolon is seen via one input line or multiple, the command is executed.  "help" for meta-commands.  "exit" to exit the interact proc.

errorCode
---

When propagating an error from the cpp-driver back to Tcl, the errorCode will be a list containing three our four elements.  The first will be the literal string **CASSANDRA**, the second will be the cassandra error code in text form matching the error codes in the cassandra.h include file, for example **CASS_ERROR_LIB_NO_HOSTS_AVAILBLE** or **CASS_ERROR_SSL_INVALID_CERT**.

The third value will be the output of the cpp-drivers *cass_error_desc()* call, returning a result such as "Invalid query".

The fourth value, if present, will be the output of *cass_future_error_message()*.  When present it is usually pretty specific as to the nature of the problem and quite useful to aid in debugging.

Both error messages are used to generate the error string itself, to the degree that they are available when the tcl error return is being generated.

Bugs
---

It seems to be working pretty well and we haven't found memory leaks in quite a while.  Still, the code is pretty new and hasn't gotten a ton of use.  Nonetheless, the workhorse functions **async**, **exec** and **select** seem to work pretty well.

FlightAware
---
FlightAware has released over a dozen applications  (under the free and liberal BSD license) into the open source community. FlightAware's repositories are available on GitHub for public use, discussion, bug reports, and contribution. Read more at https://flightaware.com/about/code/

