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
	variable validatorTypeLookupCache

	array set simpleValidatorToType {"org.apache.cassandra.db.marshal.AsciiType" ascii "org.apache.cassandra.db.marshal.LongType" bigint "org.apache.cassandra.db.marshal.BytesType" blob "org.apache.cassandra.db.marshal.BooleanType" boolean "org.apache.cassandra.db.marshal.CounterColumnType" counter "org.apache.cassandra.db.marshal.DecimalType" decimal "org.apache.cassandra.db.marshal.DoubleType" double "org.apache.cassandra.db.marshal.FloatType" float "org.apache.cassandra.db.marshal.InetAddressType" inet "org.apache.cassandra.db.marshal.Int32Type" int "org.apache.cassandra.db.marshal.UTF8Type" text "org.apache.cassandra.db.marshal.TimestampType" timestamp "org.apache.cassandra.db.marshal.DateType" timestamp "org.apache.cassandra.db.marshal.UUIDType" uuid "org.apache.cassandra.db.marshal.IntegerType" int "org.apache.cassandra.db.marshal.TimeUUIDType" timeuuid "org.apache.cassandra.db.marshal.ListType" list "org.apache.cassandra.db.marshal.MapType" map "org.apache.cassandra.db.marshal.SetType" set "org.apache.cassandra.db.marshal.CompositeType" composite}

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
				break
			}

			"set" -
			"list" {
				set result [list $complexType $simpleValidatorToType($subType)]
				break
			}

			"map" {
				lassign [split $subType ","] keyType valueType
				set result [list map $simpleValidatorToType($keyType) $simpleValidatorToType($valieType)]
				break
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

proc import {obj} {
	namespace eval ::casstcl::keyspaces

	foreach keyspace [$obj list_keyspaces] {
		namespace eval ::casstcl::keyspaces::$keyspace
		foreach table [$obj list_tables $keyspace] {
			array set ::casstcl::keyspaces::${keyspace}::${table} [$obj list_column_types]
		}
	}
}

} ;# namespace ::casstcl

# vim: set ts=4 sw=4 sts=4 noet :

