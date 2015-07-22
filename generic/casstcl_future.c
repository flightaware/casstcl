/*
 * casstcl_futures - Functions used to create, delete, and handle futures
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_cassandra.h"
#include "casstcl_future.h"
#include "casstcl_error.h"
#include "casstcl_event.h"

#include <assert.h>

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
int
casstcl_future_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our casstcl_futureEvent structure that has the Tcl_Event plus a pointer
	// to casstcl_futureClientData, which is our key to the kindgdom.
	// Go get that.

	casstcl_futureEvent *evPtr = (casstcl_futureEvent *)tevPtr;
	casstcl_futureClientData *fcd = evPtr->fcd;
	Tcl_Interp *interp = fcd->ct->interp;

	// eval the command.  it should be the callback we were told as the
	// first argument and the future object we created, like future0, as
	// the second.

	CassError rc = cass_future_error_code(fcd->future);
	
	// Callback if we have an error OR if CASSTCL_FUTURE_CALLBACK_ON_ERROR_ONLY not set
	if ( ((fcd->flags & CASSTCL_FUTURE_CALLBACK_ON_ERROR_ONLY) != CASSTCL_FUTURE_CALLBACK_ON_ERROR_ONLY ) || 
		(casstcl_future_error_to_tcl(fcd->ct, rc, fcd->future) == TCL_ERROR ) ) { 
		// get the name of the future object this callback is related to
		// into an object so that we can pass it as an argument

		Tcl_Obj *futureObj = Tcl_NewObj();
		Tcl_GetCommandFullName(interp, fcd->cmdToken, futureObj);
	
		casstcl_invoke_callback_with_argument (interp, fcd->callbackObj, futureObj);
	
	} else {
	
		Tcl_DeleteCommandFromToken (interp, fcd->cmdToken);
	}

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

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
void casstcl_future_callback (CassFuture* future, void* data) {
	casstcl_futureEvent *evPtr;

	casstcl_futureClientData *fcd = data;
	evPtr = (casstcl_futureEvent *) ckalloc (sizeof (casstcl_futureEvent));
	evPtr->event.proc = casstcl_future_eventProc;
	evPtr->fcd = fcd;
	int queueEnd = ((fcd->flags & CASSTCL_FUTURE_QUEUE_HEAD_FLAG) == CASSTCL_FUTURE_QUEUE_HEAD_FLAG) ? 
					TCL_QUEUE_HEAD : TCL_QUEUE_TAIL;
	Tcl_ThreadQueueEvent(fcd->ct->threadId, (Tcl_Event *)evPtr, queueEnd);
}

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
int
casstcl_createFutureObjectCommand (casstcl_sessionClientData *ct, CassFuture *future, Tcl_Obj *callbackObj, int flags)
{
    // allocate one of our cass future objects for Tcl and configure it
	casstcl_futureClientData *fcd;

	CassError rc = cass_future_error_code (future);
	if (rc != CASS_OK) {
		casstcl_future_error_to_tcl (ct, rc, future);
		cass_future_free (future);
		return TCL_ERROR;
	}

    fcd = (casstcl_futureClientData *)ckalloc (sizeof (casstcl_futureClientData));
    fcd->cass_future_magic = CASS_FUTURE_MAGIC;
	fcd->ct = ct;
	fcd->future = future;
	fcd->flags = flags;
	Tcl_Interp *interp = ct->interp;

	if (callbackObj != NULL) {
		Tcl_IncrRefCount(callbackObj);
	}
	fcd->callbackObj = callbackObj;

	if (callbackObj != NULL) {
		cass_future_set_callback (future, casstcl_future_callback, fcd);
	}

	static unsigned long nextAutoCounter = 0;
	char *commandName;
	int    baseNameLength;

#define FUTURESTRING "future"
	baseNameLength = sizeof(FUTURESTRING) + snprintf (NULL, 0, "%lu", nextAutoCounter) + 1;
	commandName = ckalloc (baseNameLength);
	snprintf (commandName, baseNameLength, "%s%lu", FUTURESTRING, nextAutoCounter++);

    // create a Tcl command to interface to cass
    fcd->cmdToken = Tcl_CreateObjCommand (interp, commandName, casstcl_futureObjectObjCmd, fcd, casstcl_futureObjectDelete);
    Tcl_SetObjResult (interp, Tcl_NewStringObj (commandName, -1));
	ckfree(commandName);
    return TCL_OK;
}


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
int
casstcl_futureObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	casstcl_futureClientData *fcd = (casstcl_futureClientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "isready",
        "wait",
        "foreach",
		"status",
		"error_message",
		"delete",
        NULL
    };

    enum options {
        OPT_ISREADY,
        OPT_WAIT,
        OPT_FOREACH,
		OPT_STATUS,
		OPT_ERRORMESSAGE,
		OPT_DELETE
    };

    /* basic validation of command line arguments */
    if (objc < 2) {
        Tcl_WrongNumArgs (interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option", TCL_EXACT, &optIndex) != TCL_OK) {
		return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_ISREADY: {
			Tcl_SetBooleanObj (Tcl_GetObjResult(interp), cass_future_ready (fcd->future));
			break;
		}

		case OPT_WAIT: {
			int microSeconds = 0;

			if (objc > 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "?us?");
				return TCL_ERROR;
			}

			if (objc == 3) {
				if (Tcl_GetIntFromObj (interp, objv[2], &microSeconds) == TCL_ERROR) {
					Tcl_AppendResult (interp, " while converting microseconds element", NULL);
					return TCL_ERROR;
				}
				Tcl_SetBooleanObj (Tcl_GetObjResult(interp), cass_future_wait_timed (fcd->future, microSeconds));
			} else {
				cass_future_wait (fcd->future);
			}
			break;
		}

		case OPT_FOREACH: {
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "rowArray codeBody");
				return TCL_ERROR;
			}

			char *arrayName = Tcl_GetString (objv[2]);
			Tcl_Obj *codeObj = objv[3];

			resultCode = casstcl_iterate_over_future (fcd->ct, fcd->future, arrayName, codeObj);
			break;
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (interp, fcd->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}

		case OPT_STATUS: {
			const char *cassErrorCodeString = casstcl_cass_error_to_errorcode_string (cass_future_error_code (fcd->future));

			Tcl_SetObjResult (interp, Tcl_NewStringObj (cassErrorCodeString, -1));
			break;
		}

		case OPT_ERRORMESSAGE: {
			CassString cassErrorDesc;
			cass_future_error_message (fcd->future, &cassErrorDesc.data, &cassErrorDesc.length);
			Tcl_SetStringObj (Tcl_GetObjResult(interp), cassErrorDesc.data, cassErrorDesc.length);
			break;
		}
    }
    return resultCode;
}


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
int
casstcl_iterate_over_future (casstcl_sessionClientData *ct, CassFuture *future, char *arrayName, Tcl_Obj *codeObj)
{
	int tclReturn = TCL_OK;
	const CassResult* result = NULL;
	CassIterator* iterator;
	int rc = cass_future_error_code(future);

	if (rc != CASS_OK) {
		casstcl_future_error_to_tcl (ct, rc, future);
		cass_future_free (future);
		return TCL_ERROR;
	}

	/*
	 * NOTE: Apparently, an asynchronous "connect" has no result
	 *       even when it succeeds.
	 */
	result = cass_future_get_result(future);
	Tcl_Interp *interp = ct->interp;

	if (result == NULL) {
		Tcl_ResetResult(interp);
		return TCL_OK;
	}
	iterator = cass_iterator_from_result(result);

	int columnCount = cass_result_column_count (result);

	while (cass_iterator_next(iterator)) {
		CassString cassNameString;
		int i;

		const CassRow* row = cass_iterator_get_row(iterator);

		// process all the columns into the tcl array
		for (i = 0; i < columnCount; i++) {
			Tcl_Obj *newObj = NULL;
			const char *columnName;
			const CassValue *columnValue;

			cass_result_column_name (result, i, &cassNameString.data, &cassNameString.length);
			columnName = cassNameString.data;

			columnValue = cass_row_get_column (row, i);

			if (cass_value_is_null (columnValue)) {
				Tcl_UnsetVar2 (interp, arrayName, columnName, 0);
				continue;
			}

			if (casstcl_cass_value_to_tcl_obj (ct, columnValue, &newObj) == TCL_ERROR) {
				tclReturn = TCL_ERROR;
				break;
			}

			if (newObj == NULL) {
				Tcl_UnsetVar2 (interp, arrayName, columnName, 0);
			} else {
				if (Tcl_SetVar2Ex (interp, arrayName, columnName, newObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
					tclReturn = TCL_ERROR;
					break;
				}
			}
		}

		// now execute the code body (this version of eval does not
		// require any reference count management of the object)
		int evalReturnCode = Tcl_EvalObjEx(interp, codeObj, 0);
		if ((evalReturnCode != TCL_OK) && (evalReturnCode != TCL_CONTINUE)) {
			if (evalReturnCode == TCL_BREAK) {
				tclReturn = TCL_BREAK;
			}

			if (evalReturnCode == TCL_ERROR) {
				char        msg[60];
				tclReturn = TCL_ERROR;

				sprintf(msg, "\n    (\"select\" body line %d)",
						Tcl_GetErrorLine(interp));
				Tcl_AddErrorInfo(interp, msg);
			}

			break;
		}
	}
	cass_iterator_free(iterator);
	return tclReturn;
}

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
void
casstcl_futureObjectDelete (ClientData clientData)
{
    casstcl_futureClientData *fcd = (casstcl_futureClientData *)clientData;

    assert (fcd->cass_future_magic == CASS_FUTURE_MAGIC);

	cass_future_free (fcd->future);

	if (fcd->callbackObj != NULL) {
		Tcl_DecrRefCount(fcd->callbackObj);
	}

    ckfree((char *)clientData);
}

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
casstcl_futureClientData *
casstcl_future_command_to_futureClientData (Tcl_Interp *interp, char *futureCommandName)
{
	Tcl_CmdInfo futureCmdInfo;

	if (!Tcl_GetCommandInfo (interp, futureCommandName, &futureCmdInfo)) {
		return NULL;
	}

	casstcl_futureClientData *fcd = (casstcl_futureClientData *)futureCmdInfo.objClientData;
    if (fcd->cass_future_magic != CASS_FUTURE_MAGIC) {
		return NULL;
	}

	return fcd;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
