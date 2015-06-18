/*
 *
 * Include file for casstcl_prepared
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

/*
 *--------------------------------------------------------------
 *
 * casstcl_prepared_command_to_preparedClientData -- given a "prepared"
 * command name like prepared0, find it
 *   in the interpreter and return a pointer to its prepared client
 *   data or NULL
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
casstcl_preparedClientData * casstcl_prepared_command_to_preparedClientData (Tcl_Interp *interp, char *preparedCommandName);

/*
 *--------------------------------------------------------------
 *
 * casstcl_preparedObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the prepared object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void casstcl_preparedObjectDelete (ClientData clientData);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_prepared --
 *
 *   binds names into a prepared statement
 *
 *   takes a prepared statement client data, a Tcl list of key-value
 *   pairs and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   Similar to casstcl_bind_names_from_array and casstcl_bind_names_from_list
 *
 *   Works a little differently because it binds the parameters by name
 *   as specified in the key-value pairs.  This capability is only available
 *   to prepared statements, and makes things a little easier on the
 *   developer.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int casstcl_bind_names_from_prepared (
	casstcl_preparedClientData *pcd, 
	int objc, 
	Tcl_Obj *CONST objv[], 
	CassConsistency *consistencyPtr, 
	CassStatement **statementPtr);
/*
 *----------------------------------------------------------------------
 *
 * casstcl_preparedObjectObjCmd --
 *
 *    dispatches the subcommands of a casstcl prepared statement-handling
 *    command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int casstcl_preparedObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

/* vim: set ts=4 sw=4 sts=4 noet : */
