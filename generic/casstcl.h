/*
 *
 * Include file for casstcl package
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

#include <tcl.h>
#include <string.h>
#include <cassandra.h>

#define CASS_MAGIC 7138570
#define CASS_FUTURE_MAGIC 71077345

extern int
casstcl_cassObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objvp[]);

typedef struct casstcl_clientData
{
    int cass_magic;
    Tcl_Interp *interp;
    CassCluster *cluster;
    CassSession* session;
	CassSsl *ssl;
    Tcl_Command cmdToken;
	Tcl_ThreadId threadId;
	Tcl_Obj *loggingCallbackObj;
} casstcl_clientData;

typedef struct casstcl_futureClientData
{
    int cass_magic;
	casstcl_clientData *ct;
	CassFuture *future;
	Tcl_Command cmdToken;
	Tcl_Obj *callbackObj;
} casstcl_futureClientData;

typedef struct casstcl_loggingEvent
{
	Tcl_Event event;
	Tcl_Interp *interp;
	CassLogMessage message;
} casstcl_loggingEvent;

typedef struct casstcl_futureEvent
{
	Tcl_Event event;
	casstcl_futureClientData *fcd;
} casstcl_futureEvent;

/* vim: set ts=4 sw=4 sts=4 noet : */
