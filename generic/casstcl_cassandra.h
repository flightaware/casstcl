/*
 *
 * Include file for casstcl_cassandra
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

/*
 *--------------------------------------------------------------
 *
 * casstcl_cassObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the cass connection handle.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void casstcl_cassObjectDelete (ClientData clientData);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_make_upsert_statement_from_objv --
 *
 *   This takes an objv and objc containing possible arguments such
 *   as -mapunknown, -nocomplain and -ifnotexists and in the objv
 *   always a fully qualified table name and a list of key-value pairs
 *
 *   It creates a cass statement and if successful sets the caller's
 *   pointer to a pointer to a cass statement to that statement
 *
 *   It returns a standard Tcl result, TCL_ERROR if something went
 *   wrong and you don't get a statement
 *
 *   It returns TCL_OK if all went well
 *
 *   This uses casstcl_make_upsert_statement to make the statement after
 *   it figures the arguments thereto
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_make_upsert_statement_from_objv (
	casstcl_sessionClientData *ct, 
	int objc, 
	Tcl_Obj *CONST objv[], 
	CassConsistency *consistencyPtr, 
	CassStatement **statementPtr);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_select --
 *
 *      Given a cassandra query, array name and Tcl_Obj pointing to some
 *      Tcl code, perform the select, filling the named array with elements
 *      from each row in turn and executing code against it.
 *
 *      break, continue and return are supported (probably)
 *
 *      Issuing commands with async and processing the results with
 *      async foreach allows for greater concurrency.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_select (
	casstcl_sessionClientData *ct, 
	char *query, 
	char *arrayName, 
	Tcl_Obj *codeObj, 
	int pagingSize, 
	CassConsistency *consistencyPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_keyspaces --
 *
 *      Return a list of the extant keyspaces in the cluster by
 *      examining the metadata managed by the driver.
 *
 *      The cpp-driver docs indicate that the driver stays abreast with
 *      changes to the schema so we prefer to ask it rather than
 *      caching our own copy, or something.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_list_keyspaces (casstcl_sessionClientData *ct, Tcl_Obj **objPtr);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_tables --
 *
 *      Set the Tcl result to a list of the extant tables in a keyspace by
 *      examining the metadata managed by the driver.
 *
 *      This is cool because the driver will update the metadata if the
 *      schema changes during the session and further examinations of the
 *      metadata by the casstcl metadata-accessing functions will see the
 *      changes
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_list_tables (casstcl_sessionClientData *ct, char *keyspace, Tcl_Obj **objPtr);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_columns --
 *
 *      Set a Tcl object pointer to a list of the extant columns in the
 *      specified table in the specified keyspace by examining the
 *      metadata managed by the driver.
 *
 *      If includeTypes is 1 then instead of listing just the columns it
 *      also lists their data types, as a list of key-value pairs.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_list_columns (
	casstcl_sessionClientData *ct, 
	char *keyspace, 
	char *table, 
	int includeTypes, 
	Tcl_Obj **objPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_cassObjCmd --
 *
 *      Create a cass object...
 *
 *      cass create my_cass
 *      cass create #auto
 *
 * The created object is invoked to do things with a CassDB
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */
int casstcl_cassObjCmd(
	ClientData clientData, 
	Tcl_Interp *interp, 
	int objc, 
	Tcl_Obj *CONST objv[]);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_make_upsert_statement --
 *
 *   given a session client data, fully qualified table name, Tcl list
 *   obj and a pointer to a statement pointer,
 *
 *   this baby...
 *
 *     ...generates an upsert statement (in the form of insert but that's
 *     how cassandra rolls)
 *
 *     ...uses casstcl_bind_names_from_list to bind the data elements
 *     to the statement using the right data types
 *
 *   It creates a cassandra statement and sets your pointer to it
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_make_upsert_statement (
	casstcl_sessionClientData *ct, 
	char *tableName, Tcl_Obj *listObj, 
	CassConsistency *consistencyPtr, 
	CassStatement **statementPtr, 
	char *mapUnknown, 
	int dropUnknown, 
	int ifNotExists);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_make_statement_from_objv --
 *
 *   This is a beautiful thing because it will work from the like four
 *   places where we generate statements: exec, select, async, and
 *   batch.
 *
 *   This is like what would be in the option-handling case statements.
 *
 *   We will look at the objc and objv we are given with what's in front
 *   of the command that got invoked stripped off, that is for example
 *   if the command was
 *
 *       $batch add -array row $query column column column
 *
 *       $batch add $query $value $type $value $type
 *
 *   ...we expect to get it from "-array" on, that is, they'll pass us
 *   the address of the objv starting from there and the objc properly
 *   discounting whatever preceded the stuff we handle
 *
 *   We then figure it out and invoke the underlying stuff.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_make_statement_from_objv (
	casstcl_sessionClientData *ct, 
	int objc, Tcl_Obj *CONST objv[], 
	int argOffset, 
	CassStatement **statementPtr);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_reimport_column_type_map --
 *    Call out to the Tcl interpreter to invoke
 *    ::casstcl::import_column_type_map from the casstcl library;
 *    the proc resides in source file is casstcl.tcl.
 *
 *    This convenience function gets called from a method of the
 *    casstcl cass object and is invoked upon connection as well
 *
 * Results:
 *    The program compiles.
 *
 *----------------------------------------------------------------------
 */
int casstcl_reimport_column_type_map (casstcl_sessionClientData *ct);

/* vim: set ts=4 sw=4 sts=4 noet : */
