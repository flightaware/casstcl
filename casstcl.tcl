#
# casstcl support functions
#
#
#

#package provide casstcl

namespace eval ::casstcl {
	variable schemaDict
	variable schemaDownloaded 0
	variable simpleValidatorToType
	variable validatorTypeLookupCache
	variable columnTypeMap

	array set simpleValidatorToType [list \
		"org.apache.cassandra.db.marshal.AsciiType"         ascii     \
		"org.apache.cassandra.db.marshal.LongType"          bigint    \
		"org.apache.cassandra.db.marshal.BytesType"         blob      \
		"org.apache.cassandra.db.marshal.BooleanType"       boolean   \
		"org.apache.cassandra.db.marshal.CounterColumnType" counter   \
		"org.apache.cassandra.db.marshal.SimpleDateType"    date      \
		"org.apache.cassandra.db.marshal.DecimalType"       decimal   \
		"org.apache.cassandra.db.marshal.DoubleType"        double    \
		"org.apache.cassandra.db.marshal.FloatType"         float     \
		"org.apache.cassandra.db.marshal.InetAddressType"   inet      \
		"org.apache.cassandra.db.marshal.Int32Type"         int       \
		"org.apache.cassandra.db.marshal.UTF8Type"          text      \
		"org.apache.cassandra.db.marshal.TimeType"          time      \
		"org.apache.cassandra.db.marshal.DateType"          timestamp \
		"org.apache.cassandra.db.marshal.TimestampType"     timestamp \
		"org.apache.cassandra.db.marshal.TimeUUIDType"      timeuuid  \
		"org.apache.cassandra.db.marshal.UUIDType"          uuid      \
		"org.apache.cassandra.db.marshal.IntegerType"       varint    \
		"org.apache.cassandra.db.marshal.ListType"          list      \
		"org.apache.cassandra.db.marshal.MapType"           map       \
		"org.apache.cassandra.db.marshal.SetType"           set       \
		"org.apache.cassandra.db.marshal.TupleType"         tuple     \
		"org.apache.cassandra.db.marshal.CompositeType"     composite ]

	# load the cache with all the simple types
	array set validatorTypeLookupCache [array get simpleValidatorToType]

#
# validator_to_type - perform translations from simple and complex cassandra
#   java validator types to cassandra datatypes, for example the ones
#   done by translate_simple_validator_type above plus things like
#
#   org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type) to {map text text}
#
#   org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type) to {set text}
#
#   org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType) to timestamp
#
proc validator_to_type {validator} {
	variable simpleValidatorToType
	variable validatorTypeLookupCache

	# see if it's cached either by preloading or by having already been
	# figured out
	if {[info exists validatorTypeLookupCache($validator)]} {
		return $validatorTypeLookupCache($validator)
	}

	if {[regexp {([A-Za-z]*)Type\(([^)]*)\)$} $validator dummy complexType subType]} {
		set complexType [string tolower $complexType]

		switch $complexType {
			"reversed" {
				set result $simpleValidatorToType($subType)
			}

			"set" -
			"list" {
				set result [list $complexType $simpleValidatorToType($subType)]
			}

			"map" {
				lassign [split $subType ","] keyType valueType
				set result [list map $simpleValidatorToType($keyType) $simpleValidatorToType($valueType)]
			}

			"tuple" {
				set result [list tuple]

				foreach subType [split $subType ","] {
					lappend result $simpleValidatorToType($subType)
				}
			}

			default {
				error "unrecognized complex type '$complexType'"
			}
		}

		# cache this for next time
		set validatorTypeLookupCache($validator) $result
		return $result
	}
}

#
# import_column_type_map - given a casstcl object, traverse the metadata
#   identifying the keyspaces and, for each keyspace, the table and, for
#   each table, the columns.  For eacch column write the keyspace, table,
#   and column as the key of the columnTypeMap array with the textual
#   type being the value.
#
proc import_column_type_map {obj} {
	variable columnTypeMap

	unset -nocomplain columnTypeMap

	foreach keyspace [$obj keyspaces] {
		foreach table [$obj tables $keyspace] {
			foreach "column type" [$obj columns_with_types $keyspace $table] {
				set columnTypeMap($keyspace.$table.$column) $type
			}
		}
	}
}

proc typeof {name {subType ""}} {
	variable columnTypeMap

	if {$subType == ""} {
		return $columnTypeMap($name)
	}

	set list $columnTypeMap($name)

	switch -exact -- [lindex $list 0] {
		"list" -
		"set" {
			if {$subType != "value"} {
				error "[lindex $list 0] subtype of '$subType' must be value if specified"
			}
			return [lindex $List 1]
		}

		"map" {
			switch $subType {
				"key" {
					return [lindex $list 1]
				}

				"value" {
					return [lindex $list 2]
				}

				default {
					error "map subtype must be 'key' or 'value', you said '$subType'"
				}
			}
		}

		"tuple" {
			if {![string is integer -strict $subType]} {
				error "tuple subtype must be an integer, you said '$subType'"
			}

			set upper [expr {[llength $list] - 1}]

			if {$subType < 0 || $subType > $upper} {
				error "tuple subtype must be 0 to $upper, you said '$subType'"
			}

			return [lindex $list $subType]
		}

		default {
			error "invalid column type '$list'"
		}
	}

	error "bug"
}

#
# quote - quote a cassandra element based on the data type of the element
#
proc quote {value {type text}} {
    switch $type {
		"ascii" -
		"varchar" -
        "text" {
            return "'[string map {' ''} $value]'"
        }

        "boolean" {
            return [expr {$value ? "true" : "false"}]
        }

        default {
            return $value
        }
    }
}

#
# quote_typeof - quote_typeof wx.wx_metar $vlaue
#
proc quote_typeof {tableColumn value {subType ""}} {
	set type [typeof $tableColumn $subType]
	return [quote $value $type]
}

#
# ::casstcl::connect - convenience function to create a cassandra object
#   with optional specification of host and port
#
#   set cassHandle [::casstcl::connect -host $host -port $port]
#
proc connect {args} {
	package require cmdline

	set options {
		{host.arg "127.0.0.1" "address of cluster host"}
		{port.arg "9042" "port of cluster host"}
	}

	set usage "connect ?-host host? ?-port port?"

	if {[catch {array set params [::cmdline::getKnownOptions argv $options $usage]} catchResult] == 1} {
		error $catchResult
	}

    set cass [::casstcl::cass create #auto]

    $cass contact_points $::params(host)
    $cass port $::params(port)
    $cass connect

    return $cass
}

#
# assemble_statement - given the name of a variable to contain a CQL
#   statement and a line containing possibly a statement or part of
#   a statement, append the line to the statement and return 1 if
#   a complete statement is present, else 0
#
#   comments and blank lines are skipped
#
#   the check for a complete statement is the mere presence of any semicolon,
#   kind of meatball, that means a semicolon can't appear in a quoted part
#   or anything
#
proc assemble_statement {statementVar line} {
	upvar $statementVar statement

	#puts "line: $line"

	set line [string trim $line]

	# drop blank lines and comments
	if {$line == "" || [string range $line 0 1] == "--"} {
		return 0
	}

	if {$statement == ""} {
		set statement $line
	} else {
		append statement " $line"
	}

	return [expr {[string first ";" $line] >= 0}]
}

#
# run_fp - read from a file pointer and execute commands in cassandra
#
proc run_fp {cassHandle fp} {
	set query ""
	while {[gets $fp line] >= 0} {
		if {[assemble_statement query $line]} {
			puts "query: $query"
			$cassHandle exec $query
			set query ""
		}
	}
}

#
# run_file - run CQL commands from a file
#
proc run_file {cassHandle file} {
	set fp [open $file]
	run_fp $cassHandle $fp
	close $fp
}

#
# interact - provide a primitive cqlsh-like shell
#
proc interact {cassHandle} {
	set query ""

	while true {
		# emit prompt
		if {$query == ""} {
			puts -nonewline "tcqlsh> "
		} else {
			puts -nonewline "........> "
		}
		flush stdout

		gets stdin line

		if {[eof stdin]} {
			return
		}

		if {$line == "exit"} {
			return
		}

		if {[assemble_statement query $line]} {
			$cassHandle exec $query
			set query ""
		}
	}
}

} ;# namespace ::casstcl

# vim: set ts=4 sw=4 sts=4 noet :

