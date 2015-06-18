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

#define CASSTCL_FUTURE_QUEUE_HEAD_FLAG 1

/*
 * This is the absolute limit on the whole number of seconds that we can
 * support for the Cassandra 'timestamp' data type normalization routines.
 * When multiplied by 1000 and having 1000 added to that result, it MUST
 * fit into a 64-bit signed integer, for both positive and negative signs.
 * Also, it must be capable of being round-tripped to double without any
 * loss of precision.
 */
#define CASS_TIMESTAMP_UPPER_LIMIT (4294967295LL)
#define CASS_TIMESTAMP_LOWER_LIMIT (-CASS_TIMESTAMP_UPPER_LIMIT)

extern Tcl_ObjType casstcl_cassTypeTclType;
extern Tcl_Obj *casstcl_loggingCallbackObj;
extern Tcl_ThreadId casstcl_loggingCallbackThreadId;
/*
** NOTE: The types in this section were "borrowed" from version 1.0 of the
**       cpp-driver.
*/
#if CASS_VERSION_MAJOR > 1
typedef size_t cass_size_t;

typedef struct CassBytes_ {
  const cass_byte_t* data;
  cass_size_t size;
} CassBytes;

typedef struct CassString_ {
    const char* data;
    cass_size_t length;
} CassString;

typedef struct CassDecimal_ {
  cass_int32_t scale;
  CassBytes varint;
} CassDecimal;
#endif

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
	int flags;
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
	char *string;
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

#ifdef __cplusplus
extern "C" {
#endif

extern int
casstcl_cassObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objvp[]);

extern casstcl_futureClientData *
casstcl_future_command_to_futureClientData (Tcl_Interp *interp, char *futureCommandName);

extern int
casstcl_future_error_to_tcl (casstcl_sessionClientData *ct, CassError cassError, CassFuture *future);

extern int
casstcl_cass_value_to_tcl_obj (casstcl_sessionClientData *ct, const CassValue *cassValue, Tcl_Obj **tclObj);

#ifdef __cplusplus
}
#endif


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



#if 0
/*
 *----------------------------------------------------------------------
 *
 * casstcl_DumpAddrInfo --
 *
 *	Attempts to dump the contents of the struct addrinfo.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

void casstcl_DumpAddrInfo(
  FILE *pFile,
  struct addrinfo *pAddrInfo,
  int indent
);
#endif



/*
 *----------------------------------------------------------------------
 *
 * casstcl_cassObjectObjCmd --
 *
 *    dispatches the subcommands of a cass object command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int casstcl_cassObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

/* vim: set ts=4 sw=4 sts=4 noet : */
