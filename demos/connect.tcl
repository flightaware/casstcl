#
# connect to cassandra
#
# change contact points and possibly port below if needed to connect to
# your desired cassandra cluster
#

package require casstcl

proc cass_connect {} {
	set cass [::casstcl::cass create #auto]

	$cass set_contact_points 127.0.0.1
	$cass set_port 9042
	$cass connect

	return $cass
}

