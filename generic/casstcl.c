/*
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_batch.h"
#include "casstcl_prepared.h"
#include "casstcl_cassandra.h"
#include "casstcl_future.h"
#include "casstcl_error.h"
#include "casstcl_consistency.h"

// possibly unfortunately, the cassandra cpp-driver logging stuff is global
Tcl_Obj *casstcl_loggingCallbackObj = NULL;
Tcl_ThreadId casstcl_loggingCallbackThreadId = NULL;


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
int
casstcl_invoke_callback_with_argument (Tcl_Interp *interp, Tcl_Obj *callbackObj, Tcl_Obj *argumentObj) {
	int callbackListObjc;
	Tcl_Obj **callbackListObjv;
	int tclReturnCode;

	int evalObjc;
	Tcl_Obj **evalObjv;

	int i;

	if (Tcl_ListObjGetElements (interp, callbackObj, &callbackListObjc, &callbackListObjv) == TCL_ERROR) {
		Tcl_AppendResult (interp, " while converting callback argument", NULL);
		return TCL_ERROR;
	}

	evalObjc = callbackListObjc + 1;
	evalObjv = (Tcl_Obj **)ckalloc (sizeof (Tcl_Obj *) * evalObjc);

	for (i = 0; i < callbackListObjc; i++) {
		evalObjv[i] = callbackListObjv[i];
		Tcl_IncrRefCount (evalObjv[i]);
	}

	evalObjv[evalObjc - 1] = argumentObj;
	Tcl_IncrRefCount (evalObjv[evalObjc - 1]);

	tclReturnCode = Tcl_EvalObjv (interp, evalObjc, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	// if we got a Tcl error, since we initiated the event, it doesn't
	// have anything to traceback further from here to, we must initiate
	// a background error, which will generally cause the bgerror proc
	// to get invoked
	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
	}

	for (i = 0; i < evalObjc; i++) {
		Tcl_DecrRefCount (evalObjv[i]);
	}

	ckfree ((char *)evalObjv);
	return tclReturnCode;
}



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
){
  int index;
  uint8_t *pAddress = NULL  if( !pAddrInfo ) return;
  if( indent>0 ) fprintf(pFile, "\n");
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "pAddrInfo = [%p\n", pAddrInfo);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->flags = %d\n", pAddrInfo->ai_flags);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->family = %d\n", pAddrInfo->ai_family);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->socktype = %d\n", pAddrInfo->ai_socktype);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->protocol = %d\n", pAddrInfo->ai_protocol);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->addrlen = %d\n", (int)pAddrInfo->ai_addrlen);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->addr = [");
  if(pAddrInfo->ai_family == AF_INET){
    struct sockaddr_in *pSockAddr = (struct sockaddr_in *)pAddrInfo->ai_addr;
    pAddress = (uint8_t *)&pSockAddr->sin_addr.s_addr;
    fprintf(pFile, "IPv4 [");
  }else if(pAddrInfo->ai_family == AF_INET6){
    struct sockaddr_in6 *pSockAddr = (struct sockaddr_in6 *)pAddrInfo->ai_addr;
    pAddress = (uint8_t *)&pSockAddr->sin6_addr.s6_addr;
    fprintf(pFile, "IPv6 [");
  }else{
    fprintf(pFile, "<not_ipv4_or_ipv6>");
  }
  if( pAddress ){
    for (index=0; index<pAddrInfo->ai_addrlen; index++) {
      if( index>0 ){
        fprintf(pFile, ", ");
      }
      fprintf(pFile, "%d", (int)(pAddress[index]));
    }
  }
  fprintf(pFile, "]]\n");
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->canonname = \"%s\"\n", pAddrInfo->ai_canonname);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "  ->next = [%p]\n", pAddrInfo->ai_next);
  casstcl_DumpAddrInfo(pFile, pAddrInfo->ai_next, indent + 1);
  fprintf(pFile, "%*s", indent * 2, "");
  fprintf(pFile, "]\n");
}
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
int
casstcl_cassObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	casstcl_sessionClientData *ct = (casstcl_sessionClientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "async",
        "select",
        "exec",
        "connect",
		"prepare",
		"batch",
		"keyspaces",
		"tables",
		"columns",
		"columns_with_types",
		"reimport_column_type_map",
        "contact_points",
        "port",
        "protocol_version",
		"num_threads_io",
		"queue_size_io",
		"queue_size_event",
		"queue_size_log",
		"core_connections_per_host",
		"max_connections_per_host",
		"max_concurrent_creation",
		"max_concurrent_requests_threshold",
		"max_requests_per_flush",
		"write_bytes_high_water_mark",
		"write_bytes_low_water_mark",
		"pending_requests_high_water_mark",
		"pending_requests_low_water_mark",
		"connect_timeout",
		"request_timeout",
		"reconnect_wait_time",
		"credentials",
		"tcp_nodelay",
		"load_balance_round_robin",
		"load_balance_dc_aware",
		"token_aware_routing",
		"latency_aware_routing",
		"tcp_keepalive",
		"add_trusted_cert",
		"ssl_cert",
		"ssl_private_key",
		"ssl_verify_flag",
		"ssl_enable",
		"delete",
		"close",
        NULL
    };

    enum options {
        OPT_ASYNC,
        OPT_SELECT,
        OPT_EXEC,
        OPT_CONNECT,
		OPT_PREPARE,
		OPT_BATCH,
		OPT_LIST_KEYSPACES,
		OPT_LIST_TABLES,
		OPT_LIST_COLUMNS,
		OPT_LIST_COLUMN_TYPES,
		OPT_REIMPORT_COLUMN_TYPE_MAP,
        OPT_CONTACT_POINTS,
        OPT_PORT,
        OPT_PROTOCOL_VERSION,
		OPT_NUM_THREADS_IO,
		OPT_QUEUE_SIZE_IO,
		OPT_QUEUE_SIZE_EVENT,
		OPT_QUEUE_SIZE_LOG,
		OPT_CORE_CONNECTIONS_PER_HOST,
		OPT_MAX_CONNECTIONS_PER_HOST,
		OPT_MAX_CONCURRENT_CREATION,
		OPT_MAX_CONCURRENT_REQUESTS_THRESHOLD,
		OPT_MAX_REQUESTS_PER_FLUSH,
		OPT_WRITE_BYTES_HIGH_WATER_MARK,
		OPT_WRITE_BYTES_LOW_WATER_MARK,
		OPT_PENDING_REQUESTS_HIGH_WATER_MARK,
		OPT_PENDING_REQUESTS_LOW_WATER_MARK,
		OPT_CONNECT_TIMEOUT,
		OPT_REQUEST_TIMEOUT,
		OPT_RECONNECT_WAIT_TIME,
		OPT_CREDENTIALS,
		OPT_TCP_NODELAY,
		OPT_LOAD_BALANCE_ROUND_ROBIN,
		OPT_LOAD_BALANCE_DC_AWARE,
		OPT_TOKEN_AWARE_ROUTING,
		OPT_LATENCY_AWARE_ROUTING,
		OPT_TCP_KEEPALIVE,
		OPT_ADD_TRUSTED_CERT,
		OPT_SSL_CERT,
		OPT_SSL_PRIVATE_KEY,
		OPT_SSL_VERIFY_FLAG,
		OPT_SSL_ENABLE,
		OPT_DELETE,
		OPT_CLOSE
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
		case OPT_SELECT: {
			char *query;
			char *arrayName;
			char *consistencyName = NULL;
			Tcl_Obj *consistencyObj = NULL;
			CassConsistency consistency;
			Tcl_Obj *code;
			int pagingSize = 100;
			int arg = 2;
			int      subOptIndex;

			static CONST char *subOptions[] = {
				"-pagesize",
				"-consistency",
				NULL
			};

			enum subOptions {
				SUBOPT_PAGESIZE,
				SUBOPT_CONSISTENCY
			};

			// if we don't have at least five arguments and an odd number
			// of arguments at that, it's an error
			if ((objc < 5) || ((objc & 1) == 0)) {
				Tcl_WrongNumArgs (interp, 2, objv, "?-pagesize n? query arrayName code");
				return TCL_ERROR;
			}

			while (arg + 3 < objc) {
				if (Tcl_GetIndexFromObj (interp, objv[arg++], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
					return TCL_ERROR;
				}

				switch ((enum subOptions) subOptIndex) {
					case SUBOPT_PAGESIZE: {
						if (Tcl_GetIntFromObj (interp, objv[arg++], &pagingSize) == TCL_ERROR) {
							Tcl_AppendResult (interp, " while converting paging size", NULL);
							return TCL_ERROR;
						}
						break;
					}
					case SUBOPT_CONSISTENCY: {
						consistencyObj = objv[arg++];
						consistencyName = Tcl_GetString(consistencyObj);
						if (strlen(consistencyName) > 0 && casstcl_obj_to_cass_consistency(ct, consistencyObj, &consistency) != TCL_OK) {
							return TCL_ERROR;
						}
						break;
					}
				}
			}

			query = Tcl_GetString (objv[arg++]);
			arrayName = Tcl_GetString (objv[arg++]);
			code = objv[arg++];

			return casstcl_select (ct, query, arrayName, code, pagingSize, (consistencyObj != NULL) ? &consistency : NULL);
		}

		case OPT_EXEC:
		case OPT_ASYNC: {
			CassStatement* statement = NULL;
			CassFuture *future = NULL;
			int arg = 2;
			int      subOptIndex;
			Tcl_Obj *callbackObj = NULL;
			char *batchObjName = NULL;
			int futureFlags = 0;

			static CONST char *subOptions[] = {
				"-callback",
				"-batch",
				"-head",
				NULL
			};

			enum subOptions {
				SUBOPT_CALLBACK,
				SUBOPT_BATCH,
				SUBOPT_HEAD
			};

			// if we don't have at least three arguments, it's an error
			if (objc < 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "?-callback n? ?-batch batchObject? ?-head? ?-array arrayName? ?-table tableName? ?-prepared preparedName? ?-consistency level? statement ?args?");
				return TCL_ERROR;
			}

			while (arg + 1 < objc) {
				// stop as soon as you don't match something, leaving arg
				// at the not-matched thing (i.e. don't use arg++ in
				// this statement)
				if (Tcl_GetIndexFromObj (NULL, objv[arg], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
					break;
				}
				arg++;

				switch ((enum subOptions) subOptIndex) {
					case SUBOPT_CALLBACK: {
						callbackObj = objv[arg++];
						break;
					}

					case SUBOPT_BATCH: {
						batchObjName = Tcl_GetString (objv[arg++]);
						break;
					}

					case SUBOPT_HEAD: {
						futureFlags |= (CASSTCL_FUTURE_QUEUE_HEAD_FLAG);
						break;
					}
				}
			}

			if (batchObjName != NULL) {
				if (arg != objc) {
					Tcl_ResetResult (interp);
					Tcl_AppendResult (interp, "batch usage: obj ?-callback callback? ?-head? -batch batchName", NULL);
					return TCL_ERROR;
				}

				// get the batch object from the command name we extracted
				casstcl_batchClientData *bcd = casstcl_batch_command_to_batchClientData (interp, batchObjName);
				if (bcd == NULL) {
					Tcl_ResetResult (interp);
					Tcl_AppendResult (interp, "batch object '", batchObjName, "' doesn't exist or isn't a batch object", NULL);
					return TCL_ERROR;
				}
				const CassBatch *batch = bcd->batch;

				future = cass_session_execute_batch (ct->session, batch);

			} else {
				// it's a statement, possibly with arguments

				if (casstcl_make_statement_from_objv (ct, objc, objv, arg, &statement) == TCL_ERROR) {
					return TCL_ERROR;
				}

				future = cass_session_execute (ct->session, statement);
				cass_statement_free (statement);
			}

			// even with exec if you use -callback it's asynchronous
			if (((enum options) optIndex == OPT_EXEC) && (callbackObj == NULL)) {
				// synchronous
				cass_future_wait (future);

				CassError rc = cass_future_error_code (future);
				if (rc != CASS_OK) {
					resultCode = casstcl_future_error_to_tcl (ct, rc, future);
				}

				cass_future_free (future);
			} else {
				// asynchronous
				if (casstcl_createFutureObjectCommand (ct, future, callbackObj, futureFlags) == TCL_ERROR) {
					resultCode = TCL_ERROR;
				}
			}

			break;
		}

		case OPT_CONNECT: {
			CassError rc = CASS_OK;
			CassFuture *future;
			char *keyspace = NULL;
			int arg = 2;
			int      subOptIndex;
			Tcl_Obj *callbackObj = NULL;

			static CONST char *subOptions[] = {
				"-callback",
				NULL
			};

			enum subOptions {
				SUBOPT_CALLBACK
			};

			if (objc < 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "?keyspace?");
				return TCL_ERROR;
			}

			while (arg + 1 < objc) {
				// stop as soon as you don't match something, leaving arg
				// at the not-matched thing (i.e. don't use arg++ in
				// this statement)
				if (Tcl_GetIndexFromObj (NULL, objv[arg], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
					break;
				}
				arg++;

				switch ((enum subOptions) subOptIndex) {
					case SUBOPT_CALLBACK: {
						callbackObj = objv[arg++];
						break;
					}
				}
			}

			if (arg >= objc) {
				future = cass_session_connect (ct->session, ct->cluster);
			} else {
				keyspace = Tcl_GetString (objv[arg]);
				future = cass_session_connect_keyspace (ct->session, ct->cluster, keyspace);
			}

			if (callbackObj != NULL) {
				// asynchronous
				if (casstcl_createFutureObjectCommand (ct, future, callbackObj, 0) == TCL_ERROR) {
					resultCode = TCL_ERROR;
				}
			} else {
				cass_future_wait (future);

				rc = cass_future_error_code (future);
				if (rc == CASS_OK) {
					// import the schema keyspaces, tables, columns and types
					casstcl_reimport_column_type_map (ct);
				} else {
					resultCode = casstcl_future_error_to_tcl (ct, rc, future);
				}

				cass_future_free (future);
			}
			break;
		}

		case OPT_PREPARE: {
			char *query = NULL;
			CassError rc = CASS_OK;
			CassFuture *future;
			char *statementString;
			int statementStringLength;

			if (objc != 5) {
				Tcl_WrongNumArgs (interp, 2, objv, "name table statement");
				return TCL_ERROR;
			}

			statementString = Tcl_GetStringFromObj (objv[4], &statementStringLength);

			future = cass_session_prepare (ct->session, statementString);

			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				resultCode = casstcl_future_error_to_tcl (ct, rc, future);
				cass_future_free (future);
				Tcl_AppendResult (interp, " while attempting to prepare statement '", query, "'", NULL);
				break;
			}

			const CassPrepared *cassPrepared = cass_future_get_prepared (future);
			cass_future_free (future);

			// allocate one of our cass prepared data objects for Tcl
			// and configure it
			casstcl_preparedClientData *pcd = (casstcl_preparedClientData *)ckalloc (sizeof (casstcl_preparedClientData));

			pcd->cass_prepared_magic = CASS_PREPARED_MAGIC;
			pcd->ct = ct;
			pcd->prepared = cassPrepared;

			pcd->string = ckalloc (statementStringLength + 1);
			strncpy (pcd->string, statementString, statementStringLength);

			pcd->tableNameObj = objv[3];
			Tcl_IncrRefCount (pcd->tableNameObj);


			char *commandName = Tcl_GetString (objv[2]);

#define PREPARED_STRING_FORMAT "prepared%lu"
			// if commandName is #auto, generate a unique name for the object
			int autoGeneratedName = 0;
			if (strcmp (commandName, "#auto") == 0) {
				static unsigned long nextAutoCounter = 0;
				int baseNameLength = snprintf (NULL, 0, PREPARED_STRING_FORMAT, nextAutoCounter) + 1;
				commandName = ckalloc (baseNameLength);
				snprintf (commandName, baseNameLength, PREPARED_STRING_FORMAT, nextAutoCounter++);
				autoGeneratedName = 1;
			}

			// create a Tcl command to interface to cass
			pcd->cmdToken = Tcl_CreateObjCommand (interp, commandName, casstcl_preparedObjectObjCmd, pcd, casstcl_preparedObjectDelete);
			Tcl_SetObjResult (interp, Tcl_NewStringObj (commandName, -1));
			if (autoGeneratedName == 1) {
				ckfree(commandName);
			}
			break;
		}

		case OPT_BATCH: {
			CassBatchType cassBatchType = CASS_BATCH_TYPE_LOGGED;

			if (objc < 3 || objc > 4) {
				Tcl_WrongNumArgs (interp, 1, objv, "name ?type?");
				return TCL_ERROR;
			}

			if (objc == 4) {
				if (casstcl_obj_to_cass_batch_type (interp, objv[3], &cassBatchType) == TCL_ERROR) {
					Tcl_AppendResult (interp, " while determining batch type", NULL);
					return TCL_ERROR;
				}
			}

			return casstcl_createBatchObjectCommand (ct, Tcl_GetString (objv[2]), cassBatchType);
		}

		case OPT_LIST_KEYSPACES: {
			Tcl_Obj *obj = NULL;
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			resultCode = casstcl_list_keyspaces (ct, &obj);
			if (obj != NULL) {
				Tcl_SetObjResult (ct->interp, obj);
			}
			break;
		}

		case OPT_LIST_TABLES: {
			Tcl_Obj *obj = NULL;
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "keyspace");
				return TCL_ERROR;
			}

			char *keyspace = Tcl_GetString (objv[2]);

			resultCode = casstcl_list_tables (ct, keyspace, &obj);
			if (obj != NULL) {
				Tcl_SetObjResult (ct->interp, obj);
			}
			break;
		}

		case OPT_LIST_COLUMNS:
		case OPT_LIST_COLUMN_TYPES: {
			Tcl_Obj *obj = NULL;
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "keyspace tableName");
				return TCL_ERROR;
			}

			char *keyspace = Tcl_GetString (objv[2]);
			char *table = Tcl_GetString (objv[3]);

			resultCode = casstcl_list_columns (ct, keyspace, table, (optIndex == OPT_LIST_COLUMN_TYPES), &obj);
			if (obj != NULL) {
				Tcl_SetObjResult (ct->interp, obj);
			}
			break;
		}

		case OPT_REIMPORT_COLUMN_TYPE_MAP: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			resultCode = casstcl_reimport_column_type_map (ct);
			break;
		}

		case OPT_CONTACT_POINTS: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "address_list");
				return TCL_ERROR;
			}

			cass_cluster_set_contact_points(ct->cluster, Tcl_GetString(objv[2]));
			break;
		}

		case OPT_PORT: {
			int port = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "port");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &port) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting port element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_port(ct->cluster, port);
			break;
		}

		case OPT_PROTOCOL_VERSION: {
			int protocolVersion = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "protocolVersion");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &protocolVersion) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting protocolVersion element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_protocol_version(ct->cluster, protocolVersion);
			break;
		}

		case OPT_NUM_THREADS_IO: {
			int numThreadsIO = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "numThreadsIO");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &numThreadsIO) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting numThreadsIO element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_num_threads_io(ct->cluster, numThreadsIO);
			break;
		}

		case OPT_QUEUE_SIZE_IO: {
			int queueSizeIO = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "queueSizeIO");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &queueSizeIO) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting queueSizeIO element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_queue_size_io(ct->cluster, queueSizeIO);
			break;
		}

		case OPT_QUEUE_SIZE_EVENT: {
			int queueSizeEvent = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "queueSizeEvent");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &queueSizeEvent) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting queueSizeEvent element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_queue_size_event(ct->cluster, queueSizeEvent);
			break;
		}

		case OPT_QUEUE_SIZE_LOG: {
			int queueSizeLog = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "queueSizeLog");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &queueSizeLog) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting queueSizeLog element", NULL);
				return TCL_ERROR;
			}

#if 0
			cass_cluster_set_queue_size_log (ct->cluster, queueSizeLog);
#endif
			break;
		}

		case OPT_CORE_CONNECTIONS_PER_HOST: {
			int coreConnectionsPerHost = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "coreConnectionsPerHost");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &coreConnectionsPerHost) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting coreConnectionsPerHost element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_core_connections_per_host (ct->cluster, coreConnectionsPerHost);
			break;
		}

		case OPT_MAX_CONNECTIONS_PER_HOST: {
			int maxConnectionsPerHost = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "maxConnectionsPerHost");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &maxConnectionsPerHost) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting maxConnectionsPerHost element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_max_connections_per_host (ct->cluster, maxConnectionsPerHost);
			break;
		}

		case OPT_MAX_CONCURRENT_CREATION: {
			int maxConcurrentCreation = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "maxConcurrentCreation");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &maxConcurrentCreation) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting maxConcurrentCreation element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_max_concurrent_creation (ct->cluster, maxConcurrentCreation);
			break;
		}

		case OPT_MAX_CONCURRENT_REQUESTS_THRESHOLD: {
			int maxConcurrentRequestsThreshold = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "maxConcurrentRequestsThreshold");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &maxConcurrentRequestsThreshold) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting maxConcurrentRequestsThreshold element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_max_concurrent_requests_threshold (ct->cluster, maxConcurrentRequestsThreshold);
			break;
		}

		case OPT_MAX_REQUESTS_PER_FLUSH: {
			int maxRequestsPerFlush = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "maxRequestsPerFlush");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &maxRequestsPerFlush) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting maxRequestsPerFlush element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_max_requests_per_flush (ct->cluster, maxRequestsPerFlush);
			break;
		}

		case OPT_WRITE_BYTES_HIGH_WATER_MARK: {
			int writeBytesHighWaterMark = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "writeBytesHighWaterMark");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &writeBytesHighWaterMark) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting writeBytesHighWaterMark element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_write_bytes_high_water_mark (ct->cluster, writeBytesHighWaterMark);
			break;
		}

		case OPT_WRITE_BYTES_LOW_WATER_MARK: {
			int writeBytesLowWaterMark = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "writeBytesLowWaterMark");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &writeBytesLowWaterMark) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting writeBytesLowWaterMark element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_write_bytes_low_water_mark (ct->cluster, writeBytesLowWaterMark);
			break;
		}

		case OPT_PENDING_REQUESTS_HIGH_WATER_MARK: {
			int pendingRequestsHighWaterMark = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "pendingRequestsHighWaterMark");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &pendingRequestsHighWaterMark) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting pendingRequestsHighWaterMark element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_pending_requests_high_water_mark (ct->cluster, pendingRequestsHighWaterMark);
			break;
		}

		case OPT_PENDING_REQUESTS_LOW_WATER_MARK: {
			int pendingRequestsLowWaterMark = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "pendingRequestsLowWaterMark");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &pendingRequestsLowWaterMark) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting pendingRequestsLowWaterMark element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_pending_requests_low_water_mark (ct->cluster, pendingRequestsLowWaterMark);
			break;
		}

		case OPT_CONNECT_TIMEOUT: {
			int timeoutMS = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "ms");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &timeoutMS) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting timeoutMS element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_connect_timeout(ct->cluster, timeoutMS);
			break;
		}

		case OPT_REQUEST_TIMEOUT: {
			int timeoutMS = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "ms");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &timeoutMS) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting timeoutMS element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_request_timeout(ct->cluster, timeoutMS);
			break;
		}

		case OPT_RECONNECT_WAIT_TIME: {
			int waitMS = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "ms");
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[2], &waitMS) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting waitMS element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_reconnect_wait_time (ct->cluster, waitMS);
			break;
		}

		case OPT_TCP_NODELAY: {
			int enable = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "enableFlag");
				return TCL_ERROR;
			}

			if (Tcl_GetBooleanFromObj (interp, objv[2], &enable) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting enable element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_tcp_nodelay(ct->cluster, enable);
			break;
		}

		case OPT_LOAD_BALANCE_ROUND_ROBIN: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_cluster_set_load_balance_round_robin (ct->cluster);
			break;
		}

		case OPT_LOAD_BALANCE_DC_AWARE: {
			char *localDC;
			int usedHostsPerRemoteDC;
			cass_bool_t allowRemoteDCS;
			int allowFlag;

			if (objc != 5) {
				Tcl_WrongNumArgs (interp, 2, objv, "local_dc hosts_per_remote_dc allow_remote_dcs");
				return TCL_ERROR;
			}

			localDC = Tcl_GetString (objv[3]);

			if (Tcl_GetIntFromObj (interp, objv[4], &usedHostsPerRemoteDC) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting usedHostsPerRemoteDC element", NULL);
				return TCL_ERROR;
			}

			if (Tcl_GetBooleanFromObj (interp, objv[5], &allowFlag) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting allowRemoteDCS element", NULL);
				return TCL_ERROR;
			}
			allowRemoteDCS = (allowFlag) ? cass_true : cass_false;

			CassError cassError = cass_cluster_set_load_balance_dc_aware (ct->cluster, localDC, usedHostsPerRemoteDC, allowRemoteDCS);
			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}
			break;
		}


		case OPT_TOKEN_AWARE_ROUTING: {
			int enable = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "enableFlag");
				return TCL_ERROR;
			}

			if (Tcl_GetBooleanFromObj (interp, objv[2], &enable) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting enable element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_token_aware_routing(ct->cluster, enable);
			break;
		}

		case OPT_LATENCY_AWARE_ROUTING: {
			int enable = 0;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "enableFlag");
				return TCL_ERROR;
			}

			if (Tcl_GetBooleanFromObj (interp, objv[2], &enable) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting enable element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_latency_aware_routing(ct->cluster, enable);
			break;
		}

		case OPT_TCP_KEEPALIVE: {
			int enable = 0;
			int delaySecs = 0;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "enableFlag delaySecs");
				return TCL_ERROR;
			}

			if (Tcl_GetBooleanFromObj (interp, objv[2], &enable) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting enable element", NULL);
				return TCL_ERROR;
			}

			if (Tcl_GetIntFromObj (interp, objv[3], &delaySecs) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting delaySecs element", NULL);
				return TCL_ERROR;
			}

			cass_cluster_set_tcp_keepalive (ct->cluster, enable, delaySecs);
			break;
		}


		case OPT_CREDENTIALS: {
			char *username = NULL;
			char *password = NULL;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "username password");
				return TCL_ERROR;
			}

			username = Tcl_GetString (objv[2]);
			password = Tcl_GetString (objv[3]);

			cass_cluster_set_credentials (ct->cluster, username, password);
			break;
		}

		case OPT_ADD_TRUSTED_CERT: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "pemFormattedCertString");
				return TCL_ERROR;
			}

			cass_ssl_add_trusted_cert (ct->ssl, Tcl_GetString (objv[2]));
			break;
		}

		case OPT_SSL_CERT: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "pemFormattedCertString");
				return TCL_ERROR;
			}

			cass_ssl_set_cert (ct->ssl, Tcl_GetString (objv[2]));
			break;
		}

		case OPT_SSL_PRIVATE_KEY: {
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "pemFormattedCertString password");
				return TCL_ERROR;
			}

			cass_ssl_set_private_key (ct->ssl, Tcl_GetString (objv[2]), Tcl_GetString (objv[3]));
			break;
		}

		case OPT_SSL_VERIFY_FLAG: {
			int         subOptIndex;
			int flags = 0;

			static CONST char *subOptions[] = {
				"none",
				"verify_peer_certificate",
				"verify_peer_identity",
				NULL
			};

			enum subOptions {
				SUBOPT_NONE,
				SUBOPT_VERIFY_PEER_CERT,
				SUBOPT_VERIFY_PEER_IDENTITY
			};

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "flag");
				return TCL_ERROR;
			}

			if (Tcl_GetIndexFromObj (interp, objv[2], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
				return TCL_ERROR;
			}

			switch ((enum subOptions) subOptIndex) {
				case SUBOPT_NONE: {
					flags = CASS_SSL_VERIFY_NONE;
					break;
				}

				case SUBOPT_VERIFY_PEER_CERT: {
					flags = CASS_SSL_VERIFY_PEER_CERT;
					break;
				}

				case SUBOPT_VERIFY_PEER_IDENTITY: {
					flags = CASS_SSL_VERIFY_PEER_IDENTITY;
					break;
				}
			}

			cass_ssl_set_verify_flags (ct->ssl, flags);
			break;
		}

		case OPT_SSL_ENABLE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_cluster_set_ssl (ct->cluster, ct->ssl);
			break;
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_session_close (ct->session);
			Tcl_DeleteCommandFromToken (ct->interp, ct->cmdToken);
			break;
		}

		case OPT_CLOSE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_session_close (ct->session);
			break;
		}
	}

    return resultCode;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
