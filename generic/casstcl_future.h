/*
 *
 * Include file for casstcl_future
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

/*
 *----------------------------------------------------------------------
 *
 * casstcl_future_eventProc --
 *
 *    this routine is called by the Tcl event handler to process callbacks
 *    we have set up from future (result objects) we've gotten from Cassandra
 *
 * Results:
 *    The callback routine set when the async method was invoked is
 *    invoked in the Tcl interpreter with one argument, that being the
 *    future object that was also created when the async method was
 *    invoked
 *
 *    If an uncaught error occurs when evaluating the command, a Tcl
 *    background exception is invoked
 *
 *----------------------------------------------------------------------
 */
int casstcl_future_eventProc (Tcl_Event *tevPtr, int flags);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_future_callback --
 *
 *    this routine is called by the cassandra cpp-driver as a callback
 *    when a callback was set up by us using cass_future_set_callback
 *
 *    this occurs when the request has completed or errored
 *
 *    this generates a Tcl event and queues it to the thread that issued
 *    the command to do an asynchronous cassandra command with callback
 *    in the first place.
 *
 *    when Tcl processes the event, casstcl_future_eventProc will be invoked.
 *    that guy will do a Tcl eval to invoke the callback
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
void casstcl_future_callback (CassFuture* future, void* data);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_createFutureObjectCommand --
 *
 *    given a casstcl_sessionClientData pointer, a pointer to a
 *    CassFuture structure and a Tcl callback object (containing a
 *    function name), this routine creates a Tcl command like
 *    "future17" that can be invoked with method arguments to access,
 *    manipulate and destroy cassandra future objects.
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int casstcl_createFutureObjectCommand (
	casstcl_sessionClientData *ct, 
	CassFuture *future, 
	Tcl_Obj *callbackObj, 
	int flags);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_futureObjectObjCmd --
 *
 *    dispatches the subcommands of a casstcl future-handling command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int casstcl_futureObjectObjCmd(
	ClientData cData, 
	Tcl_Interp *interp, 
	int objc, 
	Tcl_Obj *CONST objv[]);


/*
 *----------------------------------------------------------------------
 *
 * casstcl_iterate_over_future --
 *
 *      Given a casstcl client data structure, a cassandra cpp-driver
 *      CassFuture object, the name of an array and a Tcl object
 *      containing a code body, populate the array with each row in
 *      the result in turn and execute the code body
 *
 * Results:
 *      A standard Tcl result.
 *
 * Note that it is up to the caller to free the future.
 *
 * Also note that this code bears way too much in common with casstcl_select
 * but it doesn't quite go to call this from that because that uses paging
 * through the results
 *
 *----------------------------------------------------------------------
 */
int casstcl_iterate_over_future (
	casstcl_sessionClientData *ct, 
	CassFuture *future, char *arrayName, 
	Tcl_Obj *codeObj);

/*
 *--------------------------------------------------------------
 *
 * casstcl_futureObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the future object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void casstcl_futureObjectDelete (ClientData clientData);

/*
 *--------------------------------------------------------------
 *
 * casstcl_future_command_to_futureClientData -- given a "future"
 * command name like future0, find it
 *   in the interpreter and return a pointer to its future client
 *   data or NULL
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
casstcl_futureClientData * casstcl_future_command_to_futureClientData (Tcl_Interp *interp, char *futureCommandName);

/* vim: set ts=4 sw=4 sts=4 noet : */
