#
# run cql files
#
#

source connect.tcl

proc read_and_run {fp} {
	set query ""
	while {[gets $fp line] >= 0} {
		#puts "line: $line"
		if {$line == ""} {
			continue
		}

		append query " [string trim $line]"

		if {[string first ";" $line] < 0} {
			continue
		}

		puts "query: $query"
		$::cass exec $query
		set query ""
	}
}

proc run_file {file} {
	set fp [open $file]
	read_and_run $fp
	close $fp
}

proc usage {} {
	puts stderr "usage: $::argv0 cqlFile"
}

proc main {{argv ""}} {
	set options {
		{host.arg "127.0.0.1" "address of cluster host"}
		{port.arg "9042" "port of cluster host"}
		{file.arg "" "specify file to read cql from"}
	}

	set usage ":$::argv0 ?-host host? ?-port port? ?-file file?"

	if {[catch {array set ::params [::cmdline::getKnownOptions argv $options $usage]} catchResult] == 1} {
		puts stderr $catchResult
		exit 1
	}

	import_casstclrc

	cass_connect

	if {$::params(file) == ""} {
		read_and_run stdin
	} else {
		run_file $::params(file)
	}
}

if {!$tcl_interactive} {
	main $argv
}
