#
# casstcl support functions
#
#
#

package provide casstcl

namespace eval ::casstcl {
	variable schemaDict
	variable schemaDownloaded 0
	variable simpleValidatorToType

	array set simpleValidatorToType {"org.apache.cassandra.db.marshal.AsciiType" ascii "org.apache.cassandra.db.marshal.LongType" int "org.apache.cassandra.db.marshal.BytesType" blob "org.apache.cassandra.db.marshal.BooleanType" boolean "org.apache.cassandra.db.marshal.CounterColumnType" counter "org.apache.cassandra.db.marshal.DecimalType" decimal "org.apache.cassandra.db.marshal.DoubleType" double "org.apache.cassandra.db.marshal.FloatType" float "org.apache.cassandra.db.marshal.InetAddressType" inet "org.apache.cassandra.db.marshal.Int32Type" int32 "org.apache.cassandra.db.marshal.UTF8Type" text "org.apache.cassandra.db.marshal.TimestampType" timestamp "org.apache.cassandra.db.marshal.DateType" timestamp "org.apache.cassandra.db.marshal.UUIDType" uuid "org.apache.cassandra.db.marshal.IntegerType" int "org.apache.cassandra.db.marshal.TimeUUIDType" timeuuid "org.apache.cassandra.db.marshal.ListType" list "org.apache.cassandra.db.marshal.MapType" map "org.apache.cassandra.db.marshal.SetType" set "org.apache.cassandra.db.marshal.CompositeType" composite}

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

	if {[info exists simpleValidatorToType($validator)]} {
		return $simpleValidatorToType($validator)
	}

	if {[regexp {([A-Za-z]*)Type\(([^)]*)\)$} $validator dummy complexType subType]} {
		set complexType [string tolower $complexType]

		switch $complexType {
			"reversed" {
				return $simpleValidatorToType($subType)
			}

			"set" -
			"list" {
				return [list $complexType $simpleValidatorToType($subType)]
			}

			"map" {
				lassign [split $subType ","] keyType valueType
				return [list map $simpleValidatorToType($keyType) $simpleValidatorToType($valieType)]
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

#
# table_typemap_to_array - given a schema and table and the name of an array,
#   fill the array with key-value pairs where the keys are the names of each
#   column in the table and the values are the cassandra data types of those
#   columns
#
proc table_typemap_to_array {schema table _array} {
	upvar $_array array

	array set array [dict get $schema $schema $table]
}



} ;# namespace ::casstcl

# vim: set ts=4 sw=4 sts=4 noet :

