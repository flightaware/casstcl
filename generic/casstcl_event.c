/*
 *----------------------------------------------------------------------
 *
 * casstcl_logging_eventProc --
 *
 *    this routine is called by the Tcl event handler to process callbacks
 *    we have set up from logging callbacks we've gotten from Cassandra
 *    loop is
 *
 * Results:
 *    returns 1 to say we handled the event and the dispatcher can delete it
 *
 *----------------------------------------------------------------------
 */
int
casstcl_logging_eventProc (Tcl_Event *tevPtr, int flags) {

	// we got called with a Tcl_Event pointer but really it's a pointer to
	// our casstcl_loggingEvent structure that has the Tcl_Event plus a pointer
	// to casstcl_sessionClientData, which is our key to the kindgdom.
	// Go get that.

	casstcl_loggingEvent *evPtr = (casstcl_loggingEvent *)tevPtr;
	Tcl_Interp *interp = evPtr->interp;

#define CASSTCL_LOG_CALLBACK_LISTCOUNT 12

	Tcl_Obj *listObjv[CASSTCL_LOG_CALLBACK_LISTCOUNT];

	// probably won't happen but if we get a logging callback and have
	// no callback object, return 1 saying we handled it and let the
	// dispatcher delete the message NB this isn't exactly cool
	if (casstcl_loggingCallbackObj == NULL) {
		return 1;
	}

	// construct a list of key-value pairs representing the log message

	listObjv[0] = Tcl_NewStringObj ("clock", -1);
	listObjv[1] = Tcl_NewDoubleObj (evPtr->message.time_ms / 1000.0);

	listObjv[2] = Tcl_NewStringObj ("severity", -1);
	listObjv[3] = Tcl_NewStringObj (casstcl_cass_log_level_to_string (evPtr->message.severity), -1);

	listObjv[4] = Tcl_NewStringObj ("file", -1);
	listObjv[5] = Tcl_NewStringObj (evPtr->message.file, -1);

	listObjv[6] = Tcl_NewStringObj ("line", -1);
	listObjv[7] = Tcl_NewIntObj (evPtr->message.line);

	listObjv[8] = Tcl_NewStringObj ("function", -1);
	listObjv[9] = Tcl_NewStringObj (evPtr->message.function, -1);

	listObjv[10] = Tcl_NewStringObj ("message", -1);
	int messageLength = strnlen (evPtr->message.message, CASS_LOG_MAX_MESSAGE_SIZE);
	listObjv[11] = Tcl_NewStringObj (evPtr->message.message, messageLength);

	Tcl_Obj *listObj = Tcl_NewListObj (CASSTCL_LOG_CALLBACK_LISTCOUNT, listObjv);

	// even if this fails we still want the event taken off the queue
	// this function will do the background error thing if there is a tcl
	// error running the callback
	casstcl_invoke_callback_with_argument (interp, casstcl_loggingCallbackObj, listObj);

	// tell the dispatcher we handled it.  0 would mean we didn't deal with
	// it and don't want it removed from the queue
	return 1;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_logging_callback --
 *
 *    this routine is called by the cassandra cpp-driver as a callback
 *    when a log message has been received and cass_log_set_callback
 *    has been done to register this callback
 *
 * Results:
 *    an event is queued to the thread that started our conversation with
 *    cassandra
 *
 *----------------------------------------------------------------------
 */
void casstcl_logging_callback (const CassLogMessage *message, void *data) {
	casstcl_loggingEvent *evPtr;

	Tcl_Interp *interp = data;
	evPtr = ckalloc (sizeof (casstcl_loggingEvent));
	evPtr->event.proc = casstcl_logging_eventProc;
	evPtr->interp = interp;
	evPtr->message = *message; /* structure copy */
	Tcl_ThreadQueueEvent(casstcl_loggingCallbackThreadId, (Tcl_Event *)evPtr, TCL_QUEUE_TAIL);
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_EventSetupProc --
 *    This routine is a required argument to Tcl_CreateEventSource
 *
 *    Normally here an extension that generates events does something
 *    to make sure the application wakes up when events of the desired
 *    type occur.
 *
 *    We don't need to do anything here because we generate Tcl events
 *    onto the originating thread via the callbacks invoked from the
 *    Cassandra cpp-driver library and that's (apparently) all Tcl
 *    needs to do its thing.
 *
 * Results:
 *    The program compiles.
 *
 *----------------------------------------------------------------------
 */
void
casstcl_EventSetupProc (ClientData data, int flags)
{
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_EventCheckProc --
 *
 *    Normally here an extension that generates events would look at its
 *    tables or whatnot to see what needs to be generated as an event.
 *
 *    We don't need to do that because we generate Tcl events
 *    onto the originating thread via the callbacks invoked from the
 *    Cassandra cpp-driver library, so we handle it that way.
 *
 * Results:
 *    The program compiles.
 *
 *----------------------------------------------------------------------
 */
void
casstcl_EventCheckProc (ClientData data, int flags)
{
}



