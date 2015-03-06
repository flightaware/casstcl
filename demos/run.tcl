

source connect.tcl

proc usage {} {
	puts stderr "usage: $::argv0 cqlFile"
}

proc main {{argv ""}} {
	if {[llength $argv] != 1} {
		usage
	}
	cass_connect

	set fp [open $argv]
	set sql [read $fp]
	close $fp

	$cass exec $sql
}

if {!$tcl_interactive} {
	main $argv
}
