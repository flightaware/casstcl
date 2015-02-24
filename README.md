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

    autoconf
    configure
    make
    sudo make install

For FreeBSD, something like

    ./configure --with-tcl=/usr/local/lib/tcl8.6  --mandir=/usr/local/man --enable-symbols
 
Accessing from Tcl
---

    package require casstcl


CassTcl objects
---

CassTcl provides object creation commands...

* ::casstcl::cass create handle, to create a cassandra handling object.

	set cass [::casstcl::cass create #auto]

	$cass exec "insert into foo ..."

...or...

	::casstcl::cass create cass

	cass exec "insert into foo ..."


Methods of cassandra cluster interface object
---

```tcl
    set ::cassdb [::cassandra::connect args]
```

* $cassdb exec $statement

Perform the requested CQL statement.  Either it works or you get a Tcl error.

* $cassdb select ?-pagesize n? $statement array code

Iterate filling array with results of the select statement and executing code upon it.  break, continue and return from the code is supported.

Null values are unset from the array, so you have to use info exists to see if a value is not null.

The array is cleared each iteration for you automatically so you don't neeed to jack with unset.

* $cassdb connect ?keyspace?

Connect to the cassandra cluster.  Use the specified keyspace if present.

* $cassdb prepare $statement

Prepare the specified statement.

* $cassdb set_contact_points $addressList

Provide a list of one or more addresses to contact the cluster at.

The first call sets the contact points and any subsequent calls appends additional contact points. Passing an empty string will clear the contact points. White space is striped from the contact points.

* $cassdb set_port $port

Specify a port to connect to the cluster on.  Since the cpp-driver uses the native binary CQL protocol, this would normally be 9042.

* $cassdb set_protocol_version $protocolVersion

Sets the protocol version. This will theoretically automatically downgrade to protocol version 1 if necessary.  The default value is 2.

* $cassdb set_num_threads_io $numThreads

Set the number of IO threads. This is the number of threads that will handle query requests.  The default is 1.

* $cassdb set_queue_size_io

Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* $cassdb set_queue_size_event

Sets the size of the the fixed size queue that stores pending requests.  The default is 4096.

* $cassdb set_queue_size_log

Sets the size of the the fixed size queue that stores log messages.  The default is 4096.

* $cassdb set_core_connections_per_host $numConnections

Sets the number of connections made to each server in each IO thread.  Defaults to 1.

* $cassdb set_max_connections_per_host $numConnections

Sets the maximum number of connections made to each server in each IO thread.  Defaults to 2.

* $cassdb set_max_concurrent_creation $numConnections

Sets the maximum number of connections that will be created concurrently.  Connections are created when the current connections are unable to keep up with request throughput.  The default is 1.

* $cassdb set_max_concurrent_requests_threshold $numRequests

Sets the threshold for the maximum number of concurrent requests in-flight on a connection before creating a new connection.  The number of new connections created will not exceed max_connections_per_host.  Default 100.

* $cassdb set_connect_timeout $timeoutMS

Sets the timeout for connecting to a node.  Timeout is in milliseconds.  The default is 5000 ms.

* $cassdb set_request_timeout $timeoutMS

Sets the timeout for waiting for a response from a node.  Timeout is in milliseconds.  The default is 12000 ms.

* $cassdb set_reconnect_wait_time $timeoutMS

Sets the amount of time to wait before attempting to reconnect.  Default 2000 ms.

* $cassdb set_credentials $user $password

Set credentials for plaintext authentication.

* $cassdb set_tcp_nodelay $enabled

Enable/Disable Nagel's algorithm on connections.  The default is disabled.

* $cassdb set_token_aware_routing $enabled

Configures the cluster to use Token-aware request routing, or not.

This routing policy composes the base routing policy, routing requests first to replicas on nodes considered 'local' by the base load balancing policy.

The default is enabled.

This routing policy composes the base routing policy, routing requests first to replicas on nodes considered local by the base load balancing policy.

* $cassdb set_tcp_keepalive $enabled $delaySecs

Enables/Disables TCP keep-alive.  Default is disabled.  delaySecs is the initial delay in seconds; it is ignored when disabled.

* $cassdb delete

Disconnect from the cluster and delete the cluster connection object.

* $cassdb close

Disconnect from the cluster.

* $cassdb add_trusted_cert $pemFormattedCertString

This adds a trusted certificate.  This is used to verify the peer's certificate.

* $cassdb set_ssl_cert $pemFormattedCertString

This sets the client-side certificate chain.  This is used by the server to authenticate the client.  This should contain the entire certificate chain starting with the certificate itself.

* $cassdb set_ssl_private_key $pemFormattedCertString $password

This sets the client-side private key.  This is by the server to authenticate the client.

* $cassdb set_ssl_verify_flag $flag

This sets the verification that the client will perform on the peer's certificate.  "none" selects that no verification will be performed, while "verify_peer_certificate" will verify that a certificate is present and valid.

Finally, "verify_peer_identity" will match the IP address to the certificate's common name or one of its subject alternative names.  This implies that the certificate is also present.

Casstcl library functions
---

* ::casstcl::download_schema

Get information from the current cluster about keyspaces, tables, columns and column data types.

* ::casstcl::download_schema_if_not_loaded

Download schema information from the cassandra cluster only if it hasn't already been downloded.

* ::casstcl::list_schema

Return a list of all the schema defined on the cluster.

* ::casstcl::list_table $schema

Return a list of all of the tables defined by the specified schema.

* ::casstcl::list_columns $schema $table

Return a list of all the columns defined in the specified table in the specified schema.

* ::casstcl::column_type $schema $table $column

Return the cassandra data type of the specified schema, table and column


