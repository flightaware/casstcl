


proc insert_blobbie {n} {
	set row(row) $n
	set row(blobbie) "will it blend"

	set query "insert into casstcl_test.rock_blobster (row, blobbie) values (?, ?);"
	set prepared [$::cass prepare #auto casstcl_test.rock_blobster $query]

	$::cass exec -prepared $prepared [array get row]
}

source connect.tcl

cass_connect

insert_blobbie 0


