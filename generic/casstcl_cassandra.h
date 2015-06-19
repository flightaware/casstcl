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
 *--------------------------------------------------------------
 *
 * casstcl_invoke_callback_with_argument --
 *
 *     The twist here is that a callback object might be a list, not
 *     just a command name, like the argument to -callback might be
 *     more than just a function name, like it could be an object name
 *     and a method name and an argument or whatever.
 *
 *     This code splits out that list and generates up an eval thingie
 *     and invokes it with the additional argument tacked onto the end,
 *     a future object or the like.
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int casstcl_invoke_callback_with_argument (Tcl_Interp *interp, Tcl_Obj *callbackObj, Tcl_Obj *argumentObj);


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

/* vim: set ts=4 sw=4 sts=4 noet : */
