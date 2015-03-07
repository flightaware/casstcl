#
# connect to cassandra
#
# change contact points and possibly port below if needed to connect to
# your desired cassandra cluster
#

if {[lsearch /usr/local/lib $auto_path] < 0} {
	lappend auto_path /usr/local/lib
}

package require casstcl
package require cmdline

if {![info exists ::params(host)]} {
	set ::params(host) 127.0.0.1
}

if {![info exists ::params(port)]} {
	set ::params(port) 9042
}

proc import_casstclrc {} {
	if {[file readable ~/.casstclrc]} {
		source ~/.casstclrc
	}
}

proc cass_connect {} {
	set ::cass [::casstcl::cass create #auto]

	$::cass contact_points $::params(host)
	$::cass port $::params(port)
	$::cass connect

	return $::cass
}

