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
#include <tclTomMath.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cassandra.h>

#define CASS_SESSION_MAGIC 7138570
#define CASS_FUTURE_MAGIC 71077345
#define CASS_BATCH_MAGIC 14215469
#define CASS_PREPARED_MAGIC 713832281

extern Tcl_ObjType casstcl_cassTypeTclType;

extern int
casstcl_cassObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objvp[]);

typedef struct casstcl_cassTypeInfo {
	CassValueType cassValueType;
	CassValueType valueSubType1;
	CassValueType valueSubType2;
} casstcl_cassTypeInfo;

typedef struct casstcl_sessionClientData
{
    int cass_session_magic;
    Tcl_Interp *interp;
    CassCluster *cluster;
    CassSession* session;
	CassSsl *ssl;
    Tcl_Command cmdToken;
	Tcl_ThreadId threadId;
	Tcl_Obj *loggingCallbackObj;
} casstcl_sessionClientData;

typedef struct casstcl_futureClientData
{
    int cass_future_magic;
	casstcl_sessionClientData *ct;
	CassFuture *future;
	Tcl_Command cmdToken;
	Tcl_Obj *callbackObj;
} casstcl_futureClientData;

typedef struct casstcl_batchClientData
{
    int cass_batch_magic;
	casstcl_sessionClientData *ct;
	CassBatch *batch;
	CassBatchType batchType;
	Tcl_Command cmdToken;
	CassConsistency consistency;
	int count;
} casstcl_batchClientData;

typedef struct casstcl_preparedClientData
{
    int cass_prepared_magic;
	casstcl_sessionClientData *ct;
	const CassPrepared *prepared;
	Tcl_Obj *tableNameObj;
	Tcl_Command cmdToken;
} casstcl_preparedClientData;

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
