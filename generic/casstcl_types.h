/*
 *
 * Include file for casstcl_types
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */


/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_value_type_to_string -- given a CassValueType,
 *   return a const char * to a character string of equivalent
 *   meaning
 *
 * Results:
 *      a string gets returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *
casstcl_cass_value_type_to_string (CassValueType valueType); 


/*
 *--------------------------------------------------------------
 *
 * casstcl_string_to_cass_value_type -- lookup a string
 *   to be one of the cass value type strings for CassValueType and set
 *   a pointer to a passed-in CassValueType value to the corresponding
 *   type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Results:
 *      ...cass value type gets returned
 *      CASS_VALUE_TYPE_UNKNOWN is returned if nothing matches
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
CassValueType
casstcl_string_to_cass_value_type (char *string); 

/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_compound_cass_value_types
 *
 * Lookup a string from a Tcl object and identify it as one of the cass
 * value type strings for CassValueType (int, text uuid, etc.) and set
 * a pointer to a passed-in CassValueType value to the corresponding
 * type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Also if it is a list of "map type type" or "set type" or "list type"
 * then set the valueSubType1 to type defined by the set or list and
 * in the case of a map set valueSubType1 for the key datatype and
 * valueSubType2 for the value datatype
 *
 * Results:
 *      ...cass value types gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_compound_cass_value_types (Tcl_Interp *interp, Tcl_Obj *tclObj, casstcl_cassTypeInfo *typeInfo); 

/*
 *----------------------------------------------------------------------
 *
 * casstcl_InitCassBytesFromBignum --
 *
 *	Allocate and initialize a CassBytes from a 'bignum'.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_InitCassBytesFromBignum(
    Tcl_Interp *interp,		/* Used for error reporting if not NULL. */
    CassBytes *v,		/* CassBytes to initialize */
    mp_int *a);			/* Initial value */

/*
 *----------------------------------------------------------------------
 *
 * casstcl_GetTimestampFromObj --
 *
 *	Accepts a Tcl object value that specifies a whole number of
 *	seconds and optionally a fractional number of seconds, and
 *	converts the value to the whole number of milliseconds.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_GetTimestampFromObj(
    Tcl_Interp *interp,		/* Used for error reporting if not NULL. */
    Tcl_Obj *objPtr,		/* Object from which to get milliseconds. */
    cass_int64_t *milliseconds);	/* Place to store whole milliseconds. */


/*
 *----------------------------------------------------------------------
 *
 * casstcl_NewTimestampObj --
 *
 *	Accepts a Cassandra 'timestamp' value, in milliseconds, and
 *	creates a Tcl object based on it.  If the milliseconds is
 *	evenly divisible by 1000, a Tcl wide integer object will be
 *	returned, containing the exact number of seconds.  Otherwise,
 *	a Tcl double object will be returned with an approximate value,
 *	where the fractional portion of the double will represent the
 *	milliseconds and the whole portion will represent the number
 *	of seconds.
 *
 * Results:
 *	The newly created Tcl object, having a reference count of zero.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

Tcl_Obj *casstcl_NewTimestampObj(cass_int64_t milliseconds);

/*
 *----------------------------------------------------------------------
 *
 * mp_read_unsigned_bin --
 *
 *	Read a binary encoded 'bignum' from the specified buffer.  It
 *	must have been initialized first.  This routine was borrowed
 *	directly from the Tcl 8.6 source code (i.e. because we needed
 *	it and it was not available as an export).
 *
 * Results:
 *	A standard LibTomMath result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int mp_read_unsigned_bin (mp_int * a, const unsigned char *b, int c);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_InitBignumFromCassBytes --
 *
 *	Allocate and initialize a 'bignum' from a CassBytes.
 *
 * Results:
*	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_InitBignumFromCassBytes(
    Tcl_Interp *interp,		/* Used for error reporting if not NULL. */
    mp_int *a,			/* Bignum to initialize */
    CassBytes *v);		/* Initial value */

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_array --
 *
 *   Now this little ditty takes an array name and a query and an objv
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list of column names
 *
 *   It fetches the data type of the column from the column-datatype cache
 *
 *   It fetches the value from the array and converts it and binds it
 *   to the statement
 *
 *   This requires that the table name and keyspace be known
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_bind_names_from_array (
  casstcl_sessionClientData *ct, 
  char *table, 
  char *query, 
  char *tclArray, 
  int objc, 
  Tcl_Obj *CONST objv[], 
  CassConsistency *consistencyPtr, 
  CassStatement **statementPtr);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_list --
 *
 *   fully qualified table name
 *   name of the table
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   Similar to casstcl_bind_names_from_array
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_list (
  casstcl_sessionClientData *ct, 
  char *table, 
  char *query, 
  int objc, 
  Tcl_Obj *CONST objv[], 
  CassConsistency *consistencyPtr, 
  CassStatement **statementPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_typename_obj_to_cass_value_types --
 *
 *   Look up the validator in the column type map and decode it into
 *   three CassValueType entries, with extremely fast caching
 *
 * Results:
 *      A standard Tcl result.
 *
 *      TCL_CONTINUE is returned if the type index name isn't found.
 *      Also in that case, the value types are set to CASS_VALUE_TYPE_UNKNOWN.
 *
 *----------------------------------------------------------------------
 */
int casstcl_typename_obj_to_cass_value_types (
  Tcl_Interp *interp, 
  char *table, 
  Tcl_Obj *typenameObj, 
  casstcl_cassTypeInfo *typeInfoPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_cass_value_to_tcl_obj --
 *
 *      Given a cassandra CassValue, generate a Tcl_Obj of a corresponding
 *      type
 *
 *      This is a vital routine to the entire edifice.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_cass_value_to_tcl_obj (
  casstcl_sessionClientData *ct, 
  const CassValue *cassValue, 
  Tcl_Obj **tclObj);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_append_tcl_obj_to_collection
 *
 * Convert a Tcl object to a cassandra value of the specified type and
 * append it to the specified collection
 *
 * This is used for constructing cassandra maps, sets and lists.
 *
 * You create a set or a list by appending elements to it.
 *
 * You create a map by appending successions of key elements and value
 * elements to it.
 *
 * They have a specified datatype for sets and lists; for keys there is
 * one for the key and one for the value so for instance the keys can
 * be integers and the values can be strings or whatever.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */
int casstcl_append_tcl_obj_to_collection (
  casstcl_sessionClientData *ct, 
  CassCollection *collection, 
  CassValueType valueType, 
  Tcl_Obj *obj); 
/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_tcl_obj --
 *
 * This routine binds Tcl objects to ?-substitution parameters in nascent
 * cassandra statements.
 *
 * It takes a statement, an index (which parameter to substitute left to
 * right from 0 to n-1), the cassandra data type (and subtype(s) if it is
 * a list, set or map), and it will convert the Tcl object to the specified
 * data type and bind it to the statement.
 *
 * This is a really important routine.
 *
 * If type conversion fails, like Cassandra wants floating point and the
 * Tcl object won't convert to floating point then it's a Tcl error.
 *
 * If everything works then TCL_OK is returned.
 *
 * valueSubType1 is only used for maps, sets and lists
 * valueSubType2 is only used for maps
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_bind_tcl_obj (
  casstcl_sessionClientData *ct, 
  CassStatement *statement, 
  char *name, 
  int name_length, 
  cass_size_t index, 
  casstcl_cassTypeInfo *typeInfo, 
  Tcl_Obj *obj);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_values_and_types --
 *
 *   Now this little ditty takes a query and an objv and a pointer to
 *   a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list pairs where the first element
 *   is a value and the second element is a cassandra data type
 *
 *   For each pair it interprets the data type, converts the element to
 *   that type, and binds it to that position in the statement.
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_values_and_types (
  casstcl_sessionClientData *ct, 
  char *query, 
  int objc, 
  Tcl_Obj *CONST objv[], 
  CassConsistency *consistencyPtr, 
  CassStatement **statementPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_GetInetFromObj --
 *
 *  Attempt to return an Inet from the Tcl object "objPtr".
 *
 * Results:
 *  A standard Tcl result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_GetInetFromObj(
    Tcl_Interp *interp, /* Used for error reporting if not NULL. */
    Tcl_Obj *objPtr,  /* The object from which to get an Inet. */
    CassInet *inetPtr);  /* Place to store resulting Inet. */

/* vim: set ts=4 sw=4 sts=4 noet : */
