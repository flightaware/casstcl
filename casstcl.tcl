#
# casstcl support functions
#
#
#

package provide casstcl

namespace eval ::casstcl {
	variable schemaDict
	variable schemaDownloaded 0

#
# translate_simple_validator_type - perform translations from simple cassandra
#   java validator types to cassandra datatypes, for example
#
#   org.apache.cassandra.db.marshal.DoubleType to double
#   org.apache.cassandra.db.marshal.UTF8Type to text
#   org.apache.cassandra.db.marshal.Int32Type to int
#
proc translate_simple_validator_type {validator} {
	if {![regexp {\.([^.]*)Type$} $validator dummy simpleType]} {
		return ""
	}

	set type [string tolower $simpleType]
	switch $type {
		int32 {
			return "int"
		}

		utf8 {
			return "text"
		}

		inetaddress {
			return "inet"
		}

		default {
			return $type
		}
	}
}

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
	set simpleType [translate_simple_validator_type $validator]
	if {$simpleType != ""} {
		return $simpleType
	}

	if {[regexp {([A-Za-z]*)Type\(([^)]*)\)$} $validator dummy complexType subType]} {
		set complexType [string tolower $complexType]

		switch $complexType {
			"reversed" {
				return [translate_simple_validator_type $subType]
			}

			"set" -
			"list" {
				return [list $complexType [translate_simple_validator_type $subType]]
			}

			"map" {
				lassign [split $subType ","] keyType valueType
				return [list map [translate_simple_validator_type $keyType] [translate_simple_validator_type $valueType]]
			}
		}

		error "could not translate validator '$validator' to a type"
	}

}

#
# download_schema - select keyspace, table, column and column data types
#   from the cassandra cluster into a Tcl dictionary
#
proc download_schema {} {
	variable schemaDict
	variable schemaDownloaded

	set schemaDict [list]
	set cql "select keyspace_name, columnfamily_name, column_name, validator from system.schema_columns;"
	$::cass select $cql row {
		dict set schemaDict $row(keyspace_name) $row(columnfamily_name) $row(column_name) [validator_to_type $row(validator)]
	}
	set schemaDownloaded 1
	return 
}

#
# download_schema_if_not_downloaded - download the schema if we haven't already
#   downloaded it
#
proc download_schema_if_not_downloaded {} {
	variable schemaDownloaded

	if {$schemaDownloaded} {
		return
	}

	download_schema
}

#
# list_schema - return a list of all the schema known by the cluster we are
#   connected to
#
proc list_schema {} {
	variable schemaDict

	download_schema_if_not_downloaded
	return [dict keys $schemaDict]
}

#
# list_tables - return a list of all the tables defined by the specified schema
#
proc list_tables {schema} {
	variable schemaDict

	download_schema_if_not_downloaded
	return [dict keys [dict get $schemaDict $schema]]
}

#
# list_columns - return a list of all the columns in the specified table in
#   the specified schema
#
proc list_columns {schema table} {
	variable schemaDict

	download_schema_if_not_downloaded
	return [dict keys [dict get [dict get $schemaDict $schema] $table]]
}

#
# column_type - return the cassandra data type of the specified schema, table
#   and column
#
proc column_type {schema table column} {
	variable schemaDict

	download_schema_if_not_downloaded
	return [dict get $schemaDict $schema $table $column]
}



} ;# namespace ::casstcl

# vim: set ts=4 sw=4 sts=4 noet :

