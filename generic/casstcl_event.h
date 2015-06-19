/*
 *
 * Include file for casstcl_event
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

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
casstcl_logging_eventProc (Tcl_Event *tevPtr, int flags);

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
void casstcl_logging_callback (const CassLogMessage *message, void *data);


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
void casstcl_EventSetupProc (ClientData data, int flags);

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
void casstcl_EventCheckProc (ClientData data, int flags);

/* vim: set ts=4 sw=4 sts=4 noet : */
