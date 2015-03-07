

array set numbers [list 0 zero 1 one 2 two 3 three 4 four 5 five 6 six 7 seven 8 eight 9 nine 10 ten 11 eleven 12 twelve 13 thirteen 14 fourteen 15 fifteen 16 sixteen 17 seventeen 18 eighteen 19 nineteen 20 twenty 30 thirty 40 forty 50 fifty 60 sixty 70 seventy 80 eighty 90 ninety 100 hundred 1000 thousand 1000000 million 1000000000 billion]

proc number_name {number} {
	if {[info exists ::numbers($number)]} {
		return $::numbers($number)
	}
}

proc insert_numbers {n} {
	set row(row) $n
	set row(mylist) [array names ::numbers]
	set row(myset) [array names ::numbers]
	set row(mymap) [array get ::numbers]

	set query "insert into casstcl_test.numbers (row, mylist, myset, mymap) values (?, ?, ?, ?);"
	set prepared [$::cass prepare #auto casstcl_test.numbers $query]

	$::cass exec -prepared $prepared [array get row]
}

source connect.tcl

cass_connect

insert_numbers 0


