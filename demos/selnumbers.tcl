

proc select_numbers {n} {
	set query "select * from casstcl_test.numbers"
	set prepared [$::cass prepare #auto casstcl_test.numbers $query]

	$::cass select $query row {
		parray row
		puts ""
	}
}

source connect.tcl

cass_connect

select_numbers 0


