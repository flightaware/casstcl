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

proc cass_connect {} {
	set ::cass [::casstcl::cass create #auto]

	$::cass set_contact_points $::params(host)
	$::cass set_port $::params(port)
	$::cass connect

	return $::cass
}

