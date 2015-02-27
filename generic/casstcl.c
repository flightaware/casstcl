/*
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include <assert.h>

// possibly unfortunately, the cassandra cpp-driver logging stuff is global
Tcl_Obj *casstcl_loggingCallbackObj = NULL;
Tcl_ThreadId casstcl_loggingCallbackThreadId = NULL;

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
void
casstcl_cassObjectDelete (ClientData clientData)
{
    casstcl_sessionClientData *ct = (casstcl_sessionClientData *)clientData;

    assert (ct->cass_session_magic == CASS_SESSION_MAGIC);

	cass_ssl_free (ct->ssl);
    cass_cluster_free (ct->cluster);
    cass_session_free (ct->session);

    ckfree((char *)clientData);
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
	Tcl_DecrRefCount(fcd->callbackObj);
    ckfree((char *)clientData);
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_batchObjectDelete -- command deletion callback routine.
 *
 * Results:
 *      ...destroys the batch object.
 *      ...frees memory.
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
void
casstcl_batchObjectDelete (ClientData clientData)
{
    casstcl_batchClientData *bcd = (casstcl_batchClientData *)clientData;

    assert (bcd->cass_batch_magic == CASS_BATCH_MAGIC);

	cass_batch_free (bcd->batch);
    ckfree((char *)clientData);
}


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
void
casstcl_preparedObjectDelete (ClientData clientData)
{
    casstcl_preparedClientData *pcd = (casstcl_preparedClientData *)clientData;

    assert (pcd->cass_prepared_magic == CASS_PREPARED_MAGIC);

	cass_prepared_free (pcd->prepared);
    ckfree((char *)clientData);
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_error_to_errorcode_string -- given a CassError
 *   code return a string corresponding to the CassError constant
 *
 * Results:
 *      returns a pointer to a const char *
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *casstcl_cass_error_to_errorcode_string (CassError cassError)
{
	switch (cassError) {
		case CASS_OK:
			return "CASS_OK";

		case CASS_ERROR_LIB_BAD_PARAMS:
			return "CASS_ERROR_LIB_BAD_PARAMS";

		case CASS_ERROR_LIB_NO_STREAMS:
			return "CASS_ERROR_LIB_NO_STREAMS";

		case CASS_ERROR_LIB_UNABLE_TO_INIT:
			return "CASS_ERROR_LIB_UNABLE_TO_INIT";

		case CASS_ERROR_LIB_MESSAGE_ENCODE:
			return "CASS_ERROR_LIB_MESSAGE_ENCODE";

		case CASS_ERROR_LIB_HOST_RESOLUTION:
			return "CASS_ERROR_LIB_HOST_RESOLUTION";

		case CASS_ERROR_LIB_UNEXPECTED_RESPONSE:
			return "CASS_ERROR_LIB_UNEXPECTED_RESPONSE";

		case CASS_ERROR_LIB_REQUEST_QUEUE_FULL:
			return "CASS_ERROR_LIB_REQUEST_QUEUE_FULL";

		case CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD:
			return "CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD";

		case CASS_ERROR_LIB_WRITE_ERROR:
			return "CASS_ERROR_LIB_WRITE_ERROR";

		case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
			return "CASS_ERROR_LIB_NO_HOSTS_AVAILABLE";

		case CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS:
			return "CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS";

		case CASS_ERROR_LIB_INVALID_ITEM_COUNT:
			return "CASS_ERROR_LIB_INVALID_ITEM_COUNT";

		case CASS_ERROR_LIB_INVALID_VALUE_TYPE:
			return "CASS_ERROR_LIB_INVALID_VALUE_TYPE";

		case CASS_ERROR_LIB_REQUEST_TIMED_OUT:
			return "CASS_ERROR_LIB_REQUEST_TIMED_OUT";

		case CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE:
			return "CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE";

		case CASS_ERROR_LIB_CALLBACK_ALREADY_SET:
			return "CASS_ERROR_LIB_CALLBACK_ALREADY_SET";

		case CASS_ERROR_LIB_INVALID_STATEMENT_TYPE:
			return "CASS_ERROR_LIB_INVALID_STATEMENT_TYPE";

		case CASS_ERROR_LIB_NAME_DOES_NOT_EXIST:
			return "CASS_ERROR_LIB_NAME_DOES_NOT_EXIST";

		case CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL:
			return "CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL";

		case CASS_ERROR_LIB_NULL_VALUE:
			return "CASS_ERROR_LIB_NULL_VALUE";

		case CASS_ERROR_LIB_NOT_IMPLEMENTED:
			return "CASS_ERROR_LIB_NOT_IMPLEMENTED";

		case CASS_ERROR_LIB_UNABLE_TO_CONNECT:
			return "CASS_ERROR_LIB_UNABLE_TO_CONNECT";

		case CASS_ERROR_LIB_UNABLE_TO_CLOSE:
			return "CASS_ERROR_LIB_UNABLE_TO_CLOSE";

		case CASS_ERROR_SERVER_SERVER_ERROR:
			return "CASS_ERROR_SERVER_SERVER_ERROR";

		case CASS_ERROR_SERVER_PROTOCOL_ERROR:
			return "CASS_ERROR_SERVER_PROTOCOL_ERROR";

		case CASS_ERROR_SERVER_BAD_CREDENTIALS:
			return "CASS_ERROR_SERVER_BAD_CREDENTIALS";

		case CASS_ERROR_SERVER_UNAVAILABLE:
			return "CASS_ERROR_SERVER_UNAVAILABLE";

		case CASS_ERROR_SERVER_OVERLOADED:
			return "CASS_ERROR_SERVER_OVERLOADED";

		case CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
			return "CASS_ERROR_SERVER_IS_BOOTSTRAPPING";

		case CASS_ERROR_SERVER_TRUNCATE_ERROR:
			return "CASS_ERROR_SERVER_TRUNCATE_ERROR";

		case CASS_ERROR_SERVER_WRITE_TIMEOUT:
			return "CASS_ERROR_SERVER_WRITE_TIMEOUT";

		case CASS_ERROR_SERVER_READ_TIMEOUT:
			return "CASS_ERROR_SERVER_READ_TIMEOUT";

		case CASS_ERROR_SERVER_SYNTAX_ERROR:
			return "CASS_ERROR_SERVER_SYNTAX_ERROR";

		case CASS_ERROR_SERVER_UNAUTHORIZED:
			return "CASS_ERROR_SERVER_UNAUTHORIZED";

		case CASS_ERROR_SERVER_INVALID_QUERY:
			return "CASS_ERROR_SERVER_INVALID_QUERY";

		case CASS_ERROR_SERVER_CONFIG_ERROR:
			return "CASS_ERROR_SERVER_CONFIG_ERROR";

		case CASS_ERROR_SERVER_ALREADY_EXISTS:
			return "CASS_ERROR_SERVER_ALREADY_EXISTS";

		case CASS_ERROR_SERVER_UNPREPARED:
			return "CASS_ERROR_SERVER_UNPREPARED";

		case CASS_ERROR_SSL_INVALID_CERT:;
			return "CASS_ERROR_SSL_INVALID_CERT";

		case CASS_ERROR_SSL_INVALID_PRIVATE_KEY:
			return "CASS_ERROR_SSL_INVALID_PRIVATE_KEY";

		case CASS_ERROR_SSL_NO_PEER_CERT:
			return "CASS_ERROR_SSL_NO_PEER_CERT";

		case CASS_ERROR_SSL_INVALID_PEER_CERT:
			return "CASS_ERROR_SSL_INVALID_PEER_CERT";

		case CASS_ERROR_SSL_IDENTITY_MISMATCH:
			return "CASS_ERROR_SSL_IDENTITY_MISMATCH";

		case CASS_ERROR_LAST_ENTRY:
			return "CASS_ERROR_LAST_ENTRY";

		default:
			return "CASS_ERROR_UNRECOGNIZED_ERROR";
	}
	return NULL;
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_cass_log_level -- lookup a string in a Tcl object
 *   to be one of the log level strings for CassLogLevel and set
 *   a pointer to a passed-in CassLogLevel value to the corresponding
 *   CassLogLevel such as CASS_LOG_CRITICAL, etc
 *
 * Results:
 *      ...cass log level gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_cass_log_level (casstcl_sessionClientData *ct, Tcl_Obj *tclObj, CassLogLevel *cassLogLevel) {
    int                 logIndex;

    static CONST char *logLevels[] = {
        "disabled",
        "critical",
        "error",
        "warn",
        "info",
        "debug",
        "trace",
        NULL
    };

    enum loglevels {
        OPT_DISABLED,
        OPT_CRITICAL,
        OPT_ERROR,
		OPT_WARN,
        OPT_INFO,
        OPT_DEBUG,
        OPT_TRACE
	};

    // argument must be one of the options defined above
    if (Tcl_GetIndexFromObj (ct->interp, tclObj, logLevels, "logLevel",
        TCL_EXACT, &logIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum loglevels) logIndex) {
        case OPT_DISABLED: {
			*cassLogLevel = CASS_LOG_DISABLED;
		}

        case OPT_CRITICAL: {
			*cassLogLevel = CASS_LOG_CRITICAL;
		}

        case OPT_ERROR: {
			*cassLogLevel = CASS_LOG_ERROR;
		}

		case OPT_WARN: {
			*cassLogLevel = CASS_LOG_WARN;
		}

        case OPT_INFO: {
			*cassLogLevel = CASS_LOG_INFO;
		}

        case OPT_DEBUG: {
			*cassLogLevel = CASS_LOG_DEBUG;
		}

        case OPT_TRACE: {
			*cassLogLevel = CASS_LOG_TRACE;
		}

	}

	return TCL_OK;
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_log_level_to_string -- given a CassLogLevel value,
 *   return a const char * to a character string of equivalent
 *   meaning
 *
 * Results:
 *      a string gets returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *
casstcl_cass_log_level_to_string (CassLogLevel severity) {
	switch (severity) {
		case CASS_LOG_DISABLED:
			return "disabled";

		case CASS_LOG_CRITICAL:
			return "critical";

		case CASS_LOG_ERROR:
			return "error";

		case CASS_LOG_WARN:
			return "warn";

		case CASS_LOG_INFO:
			return "info";

		case CASS_LOG_DEBUG:
			return "debug";

		case CASS_LOG_TRACE:
			return "trace";

		default:
			return "unknown";
	}
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_error_to_tcl -- given a CassError code and a field
 *   name, if the error code isn CASS_OK
 *
 * Results:
 *      returns a pointer to a const char *
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int casstcl_cass_error_to_tcl (casstcl_sessionClientData *ct, CassError cassError) {

	if (cassError == CASS_OK) {
		return TCL_OK;
	}

	const char *cassErrorCodeString = casstcl_cass_error_to_errorcode_string (cassError);
	const char *cassErrorDesc = cass_error_desc (cassError);
	Tcl_SetErrorCode (ct->interp, "CASSANDRA", cassErrorCodeString, cassErrorDesc);
    Tcl_SetObjResult (ct->interp, Tcl_NewStringObj (cassErrorDesc, -1));
	return TCL_ERROR;
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_cass_consistency -- lookup a string in a Tcl object
 *   to be one of the consistency strings for CassConsistency and set
 *   a pointer to a passed-in CassConsistency value to the corresponding
 *   CassConsistency such as CASS_CONSISTENCY_ANY, etc
 *
 * Results:
 *      ...cass constinency gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_cass_consistency(casstcl_sessionClientData *ct, Tcl_Obj *tclObj, CassConsistency *cassConsistency) {
    int                 conIndex;

    static CONST char *consistencies[] = {
        "any",
        "one",
        "two",
        "three",
        "quorum",
        "all",
        "local_quorum",
        "each_quorum",
        "serial",
        "local_serial",
        "local_one",
        NULL
    };

    enum consistencies {
        OPT_ANY,
        OPT_ONE,
        OPT_TWO,
		OPT_THREE,
        OPT_QUORUM,
        OPT_ALL,
        OPT_LOCAL_QUORUM,
		OPT_EACH_QUORUM,
		OPT_SERIAL,
		OPT_LOCAL_SERIAL,
		OPT_LOCAL_ONE
	};

    // argument must be one of the subOptions defined above
    if (Tcl_GetIndexFromObj (ct->interp, tclObj, consistencies, "consistency",
        TCL_EXACT, &conIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum consistencies) conIndex) {
        case OPT_ANY: {
			*cassConsistency = CASS_CONSISTENCY_ANY;
		}

        case OPT_ONE: {
			*cassConsistency = CASS_CONSISTENCY_ONE;
		}

        case OPT_TWO: {
			*cassConsistency = CASS_CONSISTENCY_TWO;
		}

		case OPT_THREE: {
			*cassConsistency = CASS_CONSISTENCY_THREE;
		}

        case OPT_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_QUORUM;
		}

        case OPT_ALL: {
			*cassConsistency = CASS_CONSISTENCY_ALL;
		}

        case OPT_LOCAL_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_QUORUM;
		}

		case OPT_EACH_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_EACH_QUORUM;
		}

		case OPT_SERIAL: {
			*cassConsistency = CASS_CONSISTENCY_SERIAL;
		}

		case OPT_LOCAL_SERIAL: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_SERIAL;
		}

		case OPT_LOCAL_ONE: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_ONE;
		}

	}

	return TCL_OK;
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_cass_batch_type -- lookup a string in a Tcl object
 *   to be one of the batch_type strings for CassBatchType and set
 *   a pointer to a passed-in CassBatchType value to the corresponding
 *   CassBatchType such as CASS_BATCH_TYPE_LOGGED, etc
 *
 * Results:
 *      ...cass batch type gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_cass_batch_type (Tcl_Interp *interp, Tcl_Obj *tclObj, CassBatchType *cassBatchType) {
    int                 batchTypeIndex;

    static CONST char *batchTypes[] = {
        "logged",
        "unlogged",
        "counter",
        NULL
    };

    enum batchTypes {
        OPT_LOGGED,
        OPT_UNLOGGED,
        OPT_COUNTER
	};

    // argument must be one of the subOptions defined above
    if (Tcl_GetIndexFromObj (interp, tclObj, batchTypes, "batch_type",
        TCL_EXACT, &batchTypeIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum batchTypes) batchTypeIndex) {
        case OPT_LOGGED: {
			*cassBatchType = CASS_BATCH_TYPE_LOGGED;
		}

        case OPT_UNLOGGED: {
			*cassBatchType = CASS_BATCH_TYPE_UNLOGGED;
		}

        case OPT_COUNTER: {
			*cassBatchType = CASS_BATCH_TYPE_COUNTER;
		}
	}

	return TCL_OK;
}


/*
 *--------------------------------------------------------------
 *
 * casstcl_batch_type_to_batch_type_string -- given a CassBatchType
 *   code return a string corresponding to the CassBatchType constant
 *
 * Results:
 *      returns a pointer to a const char *
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
const char *casstcl_batch_type_to_batch_type_string (CassBatchType cassBatchType)
{
	switch (cassBatchType) {
		case CASS_BATCH_TYPE_LOGGED:
			return "logged";

		case CASS_BATCH_TYPE_UNLOGGED:
			return "unlogged";

		case CASS_BATCH_TYPE_COUNTER:
			return "counter";

		default:
			return "unknown";
	}
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_cass_value_to_tcl_obj --
 *
 *      Given a cassandra CassValue, generate a Tcl_Obj of a corresponding
 *      type
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_cass_value_to_tcl_obj (casstcl_sessionClientData *ct, const CassValue *cassValue, Tcl_Obj **tclObj)
{
	CassValueType valueType = cass_value_type (cassValue);
	switch (valueType) {

		case CASS_VALUE_TYPE_UNKNOWN: {
			*tclObj = NULL;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_CUSTOM: {
			*tclObj = NULL;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_ASCII:
		case CASS_VALUE_TYPE_VARCHAR:
		case CASS_VALUE_TYPE_TEXT: {
			CassString value;

			cass_value_get_string(cassValue, &value);
			*tclObj = Tcl_NewStringObj (value.data, value.length);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_TIMESTAMP:
		case CASS_VALUE_TYPE_COUNTER:
		case CASS_VALUE_TYPE_BIGINT: {
			cass_int64_t cassInt;
			CassError cassError;

			cassError = cass_value_get_int64 (cassValue, &cassInt);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}

			*tclObj = Tcl_NewWideIntObj (cassInt);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_BLOB: {
			CassBytes bytes;

			cass_value_get_bytes(cassValue, &bytes);
			*tclObj = Tcl_NewByteArrayObj (bytes.data, bytes.size);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_BOOLEAN: {
			cass_bool_t cassBool;
			CassError cassError;

			cassError = cass_value_get_bool (cassValue, &cassBool);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}

			*tclObj = Tcl_NewBooleanObj (cassBool);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_DECIMAL: {
			*tclObj = NULL;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_DOUBLE: {
			cass_double_t cassDouble;
			CassError cassError;

			cassError = cass_value_get_double (cassValue, &cassDouble);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}

			*tclObj = Tcl_NewDoubleObj (cassDouble);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_FLOAT: {
			cass_float_t cassFloat;
			CassError cassError;

			cassError = cass_value_get_float (cassValue, &cassFloat);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}

			*tclObj = Tcl_NewDoubleObj (cassFloat);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_INT: {
			cass_int32_t cassInt;
			CassError cassError;

			cassError = cass_value_get_int32 (cassValue, &cassInt);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (ct, cassError);
			}

			*tclObj = Tcl_NewIntObj (cassInt);
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_UUID: {
			CassUuid key;
			char key_str[CASS_UUID_STRING_LENGTH];

			cass_value_get_uuid(cassValue, &key);
			cass_uuid_string(key, key_str);
			*tclObj = Tcl_NewStringObj (key_str, CASS_UUID_STRING_LENGTH);
			return TCL_OK;
		}


		case CASS_VALUE_TYPE_VARINT: {
			*tclObj = NULL;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_TIMEUUID: {
			*tclObj = NULL;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_INET: {
			CassError cassError;
			CassInet cassInet;

			cassError = cass_value_get_inet (cassValue, &cassInet);
		}

		case CASS_VALUE_TYPE_MAP: {
			CassIterator* iterator = cass_iterator_from_map (cassValue);
			Tcl_Obj *listObj = Tcl_NewObj ();
			Tcl_Obj *mapKey;
			Tcl_Obj *mapValue;

			while (cass_iterator_next(iterator)) {
				if (casstcl_cass_value_to_tcl_obj (ct, cass_iterator_get_map_key (iterator), &mapKey) == TCL_ERROR) {
					cass_iterator_free (iterator);
					*tclObj = NULL;
					return TCL_ERROR;

				}

				if (casstcl_cass_value_to_tcl_obj (ct, cass_iterator_get_map_value (iterator), &mapValue)  == TCL_ERROR) {
					cass_iterator_free (iterator);
					*tclObj = NULL;
					return TCL_ERROR;
				}

				// if we successfully converted both values, add to the list
				if (mapKey != NULL && mapValue != NULL) {
					Tcl_ListObjAppendElement (ct->interp, listObj, mapKey);
					Tcl_ListObjAppendElement (ct->interp, listObj, mapValue);
				}
			}

			cass_iterator_free (iterator);
			*tclObj = listObj;
			return TCL_OK;
		}

		case CASS_VALUE_TYPE_LIST:
		case CASS_VALUE_TYPE_SET: {
			CassIterator* iterator = cass_iterator_from_collection (cassValue);
			Tcl_Obj *listObj = Tcl_NewObj ();

			while (cass_iterator_next(iterator)) {
				Tcl_Obj *collectionValue;

				if (casstcl_cass_value_to_tcl_obj (ct, cass_iterator_get_value (iterator), &collectionValue) == TCL_ERROR) {
					cass_iterator_free (iterator);
					*tclObj = NULL;
					return TCL_ERROR;
				}

				if (collectionValue != NULL) {
					Tcl_ListObjAppendElement (ct->interp, listObj, collectionValue);
				}
			}

			cass_iterator_free (iterator);
			*tclObj = listObj;
			return TCL_OK;
		}
	}

	*tclObj = NULL;
	return TCL_ERROR;
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
	result = cass_future_get_result(future);
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

			cassNameString = cass_result_column_name (result, i);
			columnName = cassNameString.data;

			columnValue = cass_row_get_column (row, i);

			if (cass_value_is_null (columnValue)) {
				Tcl_UnsetVar2 (ct->interp, arrayName, columnName, 0);
				continue;
			}

			if (casstcl_cass_value_to_tcl_obj (ct, columnValue, &newObj) == TCL_ERROR) {
				tclReturn = TCL_ERROR;
				break;
			}

			if (newObj == NULL) {
				Tcl_UnsetVar2 (ct->interp, arrayName, columnName, 0);
			} else {
				if (Tcl_SetVar2Ex (ct->interp, arrayName, columnName, newObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
					tclReturn = TCL_ERROR;
					break;
				}
			}
		}

		// now execute the code body
		int evalReturnCode = Tcl_EvalObjEx(ct->interp, codeObj, 0);
		if ((evalReturnCode != TCL_OK) && (evalReturnCode != TCL_CONTINUE)) {
			if (evalReturnCode == TCL_BREAK) {
				tclReturn = TCL_BREAK;
			}

			if (evalReturnCode == TCL_ERROR) {
				char        msg[60];

				tclReturn = TCL_ERROR;

				sprintf(msg, "\n    (\"select\" body line %d)",
						Tcl_GetErrorLine(ct->interp));
				Tcl_AddErrorInfo(ct->interp, msg);
			}

			break;
		}
	}
	cass_iterator_free(iterator);
	return tclReturn;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_select --
 *
 *      Given a cassandra query, array name and Tcl_Obj pointing to some
 *      Tcl code, perform the select, filling the named array with elements
 *      from each row in turn and executing code against it.
 *
 *      break, continue and return are supported
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_select (casstcl_sessionClientData *ct, char *query, char *arrayName, Tcl_Obj *codeObj, int pagingSize) {
	CassStatement* statement = NULL;
	int tclReturn = TCL_OK;

	statement = cass_statement_new(cass_string_init(query), 0);

	cass_bool_t has_more_pages = cass_false;
	const CassResult* result = NULL;
	CassError rc = CASS_OK;
	int columnCount = -1;

	cass_statement_set_paging_size(statement, pagingSize);

	do {
		CassIterator* iterator;
		CassFuture* future = cass_session_execute(ct->session, statement);

		rc = cass_future_error_code(future);
		if (rc != CASS_OK) {
			CassString message = cass_future_error_message(future);

			Tcl_SetStringObj (Tcl_GetObjResult(ct->interp), message.data, message.length);
			tclReturn = TCL_ERROR;
			break;
		}

		result = cass_future_get_result(future);
		iterator = cass_iterator_from_result(result);
		cass_future_free(future);

		if (columnCount == -1) {
			columnCount = cass_result_column_count (result);
		}

		while (cass_iterator_next(iterator)) {
			CassString cassNameString;
			int i;

			const CassRow* row = cass_iterator_get_row(iterator);

			// process all the columns into the tcl array
			for (i = 0; i < columnCount; i++) {
				Tcl_Obj *newObj = NULL;
				const char *columnName;
				const CassValue *columnValue;

				cassNameString = cass_result_column_name (result, i);
				columnName = cassNameString.data;

				columnValue = cass_row_get_column (row, i);

				if (cass_value_is_null (columnValue)) {
					Tcl_UnsetVar2 (ct->interp, arrayName, columnName, 0);
					continue;
				}

				if (casstcl_cass_value_to_tcl_obj (ct, columnValue, &newObj) == TCL_ERROR) {
					tclReturn = TCL_ERROR;
					break;
				}

				if (newObj == NULL) {
					Tcl_UnsetVar2 (ct->interp, arrayName, columnName, 0);
				} else {
					if (Tcl_SetVar2Ex (ct->interp, arrayName, columnName, newObj, (TCL_LEAVE_ERR_MSG)) == NULL) {
						tclReturn = TCL_ERROR;
						break;
					}
				}
			}

			// now execute the code body
			int evalReturnCode = Tcl_EvalObjEx(ct->interp, codeObj, 0);
			if ((evalReturnCode != TCL_OK) && (evalReturnCode != TCL_CONTINUE)) {
				if (evalReturnCode == TCL_BREAK) {
					tclReturn = TCL_BREAK;
				}

				if (evalReturnCode == TCL_ERROR) {
					char        msg[60];

					tclReturn = TCL_ERROR;

					sprintf(msg, "\n    (\"select\" body line %d)",
							Tcl_GetErrorLine(ct->interp));
					Tcl_AddErrorInfo(ct->interp, msg);
				}

				break;
			}
		}

		has_more_pages = cass_result_has_more_pages(result);

		if (has_more_pages) {
			cass_statement_set_paging_state(statement, result);
		}

		cass_iterator_free(iterator);
		cass_result_free(result);
	} while (has_more_pages && tclReturn == TCL_OK);

	cass_statement_free(statement);
	Tcl_UnsetVar (ct->interp, arrayName, 0);

	if (tclReturn == TCL_BREAK) {
		tclReturn = TCL_OK;
	}

	return tclReturn;
}


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
	int tclReturnCode;
	Tcl_Interp *interp = evPtr->interp;

#define CASSTCL_LOG_CALLBACK_ARGCOUNT 2
#define CASSTCL_LOG_CALLBACK_LISTCOUNT 12

	Tcl_Obj *evalObjv[CASSTCL_LOG_CALLBACK_ARGCOUNT];
	Tcl_Obj *listObjv[CASSTCL_LOG_CALLBACK_LISTCOUNT];

	// probably won't happen but if we get a logging callback and have
	// no callback object, return 1 saying we handled it and let the
	// dispatcher delete the message NB this isn't exactly cool
	if (casstcl_loggingCallbackObj == NULL) {
printf("callback obj is null\n");
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

	// construct an objv we'll pass to eval.
	// first is the callback command
	// second is the list we just created
	evalObjv[0] = casstcl_loggingCallbackObj;
	evalObjv[1] = listObj;

	// invoke the logging callback command
	tclReturnCode = Tcl_EvalObjv (interp, CASSTCL_LOG_CALLBACK_ARGCOUNT, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	// if we got a Tcl error, since we initiated the event, it doesn't
	// have anything to traceback further from here to, we must initiate
	// a background error, which will generally cause the bgerror proc
	// to get invoked
	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
	}

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
		"error_code",
		"error_message",
		"delete",
        NULL
    };

    enum options {
        OPT_ISREADY,
        OPT_WAIT,
        OPT_FOREACH,
		OPT_ERRORCODE,
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
				if (Tcl_GetIntFromObj (fcd->ct->interp, objv[2], &microSeconds) == TCL_ERROR) {
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

			if (Tcl_DeleteCommandFromToken (fcd->ct->interp, fcd->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}

		case OPT_ERRORCODE: {
			const char *cassErrorCodeString = casstcl_cass_error_to_errorcode_string (cass_future_error_code (fcd->future));

			Tcl_SetObjResult (fcd->ct->interp, Tcl_NewStringObj (cassErrorCodeString, -1));
			break;
		}

		case OPT_ERRORMESSAGE: {
			CassString cassErrorDesc = cass_future_error_message (fcd->future);
			Tcl_SetStringObj (Tcl_GetObjResult(interp), cassErrorDesc.data, cassErrorDesc.length);
			break;
		}
    }
    return resultCode;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_batchObjectObjCmd --
 *
 *    dispatches the subcommands of a casstcl batch-handling command
 *
 * Results:
 *    stuff
 *
 *----------------------------------------------------------------------
 */
int
casstcl_batchObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	casstcl_batchClientData *bcd = (casstcl_batchClientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "add",
        "consistency",
        "delete",
        NULL
    };

    enum options {
        OPT_ADD,
        OPT_CONSISTENCY,
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
		case OPT_ADD: {
			CassStatement* statement = NULL;
			char *query = NULL;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "statement");
				return TCL_ERROR;
			}

			query = Tcl_GetString (objv[2]);
			statement = cass_statement_new(cass_string_init(query), 0);
			CassError cassError = cass_batch_add_statement (bcd->batch, statement);
			cass_statement_free (statement);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (bcd->ct, cassError);
			}

			break;
		}

		case OPT_CONSISTENCY: {
			CassConsistency cassConsistency;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "consistency");
				return TCL_ERROR;
			}

			if (casstcl_obj_to_cass_consistency(bcd->ct, objv[2], &cassConsistency) == TCL_ERROR) {
				return TCL_ERROR;
			}

			CassError cassError = cass_batch_set_consistency (bcd->batch, cassConsistency);
			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (bcd->ct, cassError);
			}
			break;
		}

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (bcd->ct->interp, bcd->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
    }
    return resultCode;
}


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
int
casstcl_preparedObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int         optIndex;
	casstcl_preparedClientData *pcd = (casstcl_preparedClientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "delete",
        NULL
    };

    enum options {
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
		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			if (Tcl_DeleteCommandFromToken (pcd->ct->interp, pcd->cmdToken) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			break;
		}
    }
    return resultCode;
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
	int tclReturnCode;
	Tcl_Interp *interp = fcd->ct->interp;

	Tcl_Obj *evalObjv[2];

	// construct an objv we'll pass to eval.
	// first is the callback command
	// second is the name of the future object this callback is related to
	evalObjv[0] = fcd->callbackObj;
	evalObjv[1] = Tcl_NewObj();
	Tcl_GetCommandFullName(interp, fcd->cmdToken, evalObjv[1]);

	// eval the command.  it should be the callback we were told as the
	// first argument and the future object we created, like future0, as
	// the second.  go ahead and ask for direct, i.e. don't compile into
	// bytecodes, because our little single two-argument call is ephemeral

	tclReturnCode = Tcl_EvalObjv (interp, 2, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
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
	evPtr = ckalloc (sizeof (casstcl_futureEvent));
	evPtr->event.proc = casstcl_future_eventProc;
	evPtr->fcd = fcd;
	Tcl_ThreadQueueEvent(fcd->ct->threadId, (Tcl_Event *)evPtr, TCL_QUEUE_TAIL);
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
casstcl_createFutureObjectCommand (casstcl_sessionClientData *ct, CassFuture *future, Tcl_Obj *callbackObj)
{
    // allocate one of our cass future objects for Tcl and configure it
	casstcl_futureClientData *fcd = (casstcl_futureClientData *)ckalloc (sizeof (casstcl_futureClientData));

    fcd->cass_future_magic = CASS_FUTURE_MAGIC;
	fcd->ct = ct;
	fcd->future = future;

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
    fcd->cmdToken = Tcl_CreateObjCommand (fcd->ct->interp, commandName, casstcl_futureObjectObjCmd, fcd, casstcl_futureObjectDelete);
    Tcl_SetObjResult (ct->interp, Tcl_NewStringObj (commandName, -1));
	ckfree(commandName);
    return TCL_OK;
}


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
        "set_contact_points",
        "set_port",
        "set_protocol_version",
		"set_num_threads_io",
		"set_queue_size_io",
		"set_queue_size_event",
		"set_queue_size_log",
		"set_core_connections_per_host",
		"set_max_connections_per_host",
		"set_max_concurrent_creation",
		"set_max_concurrent_requests_threshold",
		"set_connect_timeout",
		"set_request_timeout",
		"set_reconnect_wait_time",
		"set_credentials",
		"set_tcp_nodelay",
		"set_token_aware_routing",
		"set_tcp_keepalive",
		"add_trusted_cert",
		"set_ssl_cert",
		"set_ssl_private_key",
		"set_ssl_verify_flag",
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
        OPT_SET_CONTACT_POINTS,
        OPT_SET_PORT,
        OPT_SET_PROTOCOL_VERSION,
		OPT_SET_NUM_THREADS_IO,
		OPT_SET_QUEUE_SIZE_IO,
		OPT_SET_QUEUE_SIZE_EVENT,
		OPT_SET_QUEUE_SIZE_LOG,
		OPT_SET_CORE_CONNECTIONS_PER_HOST,
		OPT_SET_MAX_CONNECTIONS_PER_HOST,
		OPT_SET_MAX_CONCURRENT_CREATION,
		OPT_SET_MAX_CONCURRENT_REQUESTS_THRESHOLD,
		OPT_SET_CONNECT_TIMEOUT,
		OPT_SET_REQUEST_TIMEOUT,
		OPT_SET_RECONNECT_WAIT_TIME,
		OPT_SET_CREDENTIALS,
		OPT_SET_TCP_NODELAY,
		OPT_SET_TOKEN_AWARE_ROUTING,
		OPT_SET_TCP_KEEPALIVE,
		OPT_ADD_TRUSTED_CERT,
		OPT_SET_SSL_CERT,
		OPT_SET_SSL_PRIVATE_KEY,
		OPT_SET_SSL_VERIFY_FLAG,
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
			Tcl_Obj *code;
			int pagingSize = 100;
			int arg = 2;
			int      subOptIndex;

			static CONST char *subOptions[] = {
				"-pagesize",
				NULL
			};

			enum subOptions {
				SUBOPT_PAGESIZE
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
				}
			}

			query = Tcl_GetString (objv[arg++]);
			arrayName = Tcl_GetString (objv[arg++]);
			code = objv[arg++];

			return casstcl_select (ct, query, arrayName, code, pagingSize);
		}

		case OPT_EXEC: {
			CassStatement* statement = NULL;
			CassFuture *future = NULL;
			char *query = NULL;
			CassError rc = CASS_OK;

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "statement");
				return TCL_ERROR;
			}

			query = Tcl_GetString (objv[2]);

			statement = cass_statement_new(cass_string_init(query), 0);

			future = cass_session_execute (ct->session, statement);
			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				CassString message = cass_future_error_message(future);

				Tcl_SetStringObj (Tcl_GetObjResult(interp), message.data, message.length);
				resultCode = TCL_ERROR;
			}

			cass_future_free (future);
			cass_statement_free (statement);

			break;
		}

		case OPT_ASYNC: {
			CassStatement* statement = NULL;
			CassFuture *future = NULL;
			char *query = NULL;
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

			// if we don't have at least three arguments and an odd number
			// of arguments at that, it's an error
			if ((objc < 3) || ((objc & 1) == 0)) {
				Tcl_WrongNumArgs (interp, 2, objv, "?-callback n? statement");
				return TCL_ERROR;
			}

			while (arg + 1 < objc) {
				if (Tcl_GetIndexFromObj (interp, objv[arg++], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
					return TCL_ERROR;
				}

				switch ((enum subOptions) subOptIndex) {
					case SUBOPT_CALLBACK: {
						callbackObj = objv[arg++];
						break;
					}
				}
			}

			query = Tcl_GetString (objv[arg]);
			statement = cass_statement_new(cass_string_init(query), 0);
			future = cass_session_execute (ct->session, statement);

			if (casstcl_createFutureObjectCommand (ct, future, callbackObj) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}
			cass_statement_free (statement);

			break;
		}

		case OPT_CONNECT: {
			CassError rc = CASS_OK;
			CassFuture *future;
			char *keyspace = NULL;

			if ((objc < 2) || (objc > 3)) {
				Tcl_WrongNumArgs (interp, 2, objv, "?keyspace?");
				return TCL_ERROR;
			}

			if (objc == 2) {
				future = cass_session_connect (ct->session, ct->cluster);
			} else {
				keyspace = Tcl_GetString (objv[2]);
				future = cass_session_connect_keyspace (ct->session, ct->cluster, keyspace);
			}

			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				CassString message = cass_future_error_message(future);

				Tcl_SetStringObj (Tcl_GetObjResult(interp), message.data, message.length);
				resultCode = TCL_ERROR;
			}
			break;
		}

		case OPT_PREPARE: {
			char *query = NULL;
			CassError rc = CASS_OK;
			CassFuture *future;

			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "name statement");
				return TCL_ERROR;
			}

			query = Tcl_GetString (objv[3]);

			future = cass_session_prepare (ct->session, cass_string_init(query));

			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				CassString message = cass_future_error_message(future);

				Tcl_SetStringObj (Tcl_GetObjResult(interp), message.data, message.length);
				resultCode = TCL_ERROR;
			}

			const CassPrepared *cassPrepared = cass_future_get_prepared (future);
			cass_future_free (future);

			// allocate one of our cass prepared data objects for Tcl
			// and configure it
			casstcl_preparedClientData *pcd = (casstcl_preparedClientData *)ckalloc (sizeof (casstcl_preparedClientData));

			pcd->cass_prepared_magic = CASS_PREPARED_MAGIC;
			pcd->ct = ct;
			pcd->prepared = cassPrepared;

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
				Tcl_WrongNumArgs (interp, 1, objv, "name ?consistency?");
				return TCL_ERROR;
			}

			if (objc == 4) {
				if (casstcl_obj_to_cass_batch_type (interp, objv[3], &cassBatchType) == TCL_ERROR) {
					Tcl_AppendResult (interp, " while determining batch type", NULL);
					return TCL_ERROR;
				}
			}

			// allocate one of our cass client data objects for Tcl and configure it
			casstcl_batchClientData *bcd = (casstcl_batchClientData *)ckalloc (sizeof (casstcl_batchClientData));

			bcd->cass_batch_magic = CASS_BATCH_MAGIC;
			bcd->ct = ct;
			bcd->batch = cass_batch_new (cassBatchType);

			char *commandName = Tcl_GetString (objv[2]);

#define BATCH_STRING_FORMAT "batch%lu"
			// if commandName is #auto, generate a unique name for the object
			int autoGeneratedName = 0;
			if (strcmp (commandName, "#auto") == 0) {
				static unsigned long nextAutoCounter = 0;
				int baseNameLength = snprintf (NULL, 0, BATCH_STRING_FORMAT, nextAutoCounter) + 1;
				commandName = ckalloc (baseNameLength);
				snprintf (commandName, baseNameLength, BATCH_STRING_FORMAT, nextAutoCounter++);
				autoGeneratedName = 1;
			}

			// create a Tcl command to interface to cass
			bcd->cmdToken = Tcl_CreateObjCommand (interp, commandName, casstcl_batchObjectObjCmd, bcd, casstcl_batchObjectDelete);
			Tcl_SetObjResult (interp, Tcl_NewStringObj (commandName, -1));
			if (autoGeneratedName == 1) {
				ckfree(commandName);
			}
			break;
		}

		case OPT_SET_CONTACT_POINTS: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "address_list");
				return TCL_ERROR;
			}

			cass_cluster_set_contact_points(ct->cluster, Tcl_GetString(objv[2]));
			break;
		}

		case OPT_SET_PORT: {
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

		case OPT_SET_PROTOCOL_VERSION: {
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

		case OPT_SET_NUM_THREADS_IO: {
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

		case OPT_SET_QUEUE_SIZE_IO: {
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

		case OPT_SET_QUEUE_SIZE_EVENT: {
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

		case OPT_SET_QUEUE_SIZE_LOG: {
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

		case OPT_SET_CORE_CONNECTIONS_PER_HOST: {
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

		case OPT_SET_MAX_CONNECTIONS_PER_HOST: {
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

		case OPT_SET_MAX_CONCURRENT_CREATION: {
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

		case OPT_SET_MAX_CONCURRENT_REQUESTS_THRESHOLD: {
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

		case OPT_SET_CONNECT_TIMEOUT: {
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

		case OPT_SET_REQUEST_TIMEOUT: {
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

		case OPT_SET_RECONNECT_WAIT_TIME: {
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

		case OPT_SET_TCP_NODELAY: {
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

		case OPT_SET_TOKEN_AWARE_ROUTING: {
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

		case OPT_SET_TCP_KEEPALIVE: {
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


		case OPT_SET_CREDENTIALS: {
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


			cass_ssl_add_trusted_cert (ct->ssl, cass_string_init (Tcl_GetString (objv[2])));
			break;
		}

		case OPT_SET_SSL_CERT: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "pemFormattedCertString");
				return TCL_ERROR;
			}

			cass_ssl_set_cert (ct->ssl, cass_string_init (Tcl_GetString (objv[2])));
			break;
		}

		case OPT_SET_SSL_PRIVATE_KEY: {
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "pemFormattedCertString password");
				return TCL_ERROR;
			}

			cass_ssl_set_private_key (ct->ssl, cass_string_init (Tcl_GetString (objv[2])), Tcl_GetString (objv[3]));
			break;
		}

		case OPT_SET_SSL_VERIFY_FLAG: {
			int         subOptIndex;
			int flags;

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

		case OPT_DELETE: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_session_close (ct->session);
			Tcl_DeleteCommandFromToken (ct->interp, ct->cmdToken);
			ckfree(ct);
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

    /* ARGSUSED */
int
casstcl_cassObjCmd(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    casstcl_sessionClientData *ct;
    int                 optIndex;
    char               *commandName;
    int                 autoGeneratedName;

    static CONST char *options[] = {
        "create",
        "set_logging_callback",
        NULL
    };

    enum options {
        OPT_CREATE,
		OPT_SET_LOGGING_CALLBACK
    };

    // basic command line processing

    // argument must be one of the subOptions defined above
    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option",
        TCL_EXACT, &optIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    switch ((enum options) optIndex) {
		case OPT_CREATE: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 1, objv, "option arg");
				return TCL_ERROR;
			}

			// allocate one of our cass client data objects for Tcl and configure it
			ct = (casstcl_sessionClientData *)ckalloc (sizeof (casstcl_sessionClientData));

			ct->cass_session_magic = CASS_SESSION_MAGIC;
			ct->interp = interp;
			ct->session = cass_session_new ();
			ct->cluster = cass_cluster_new ();
			ct->ssl = cass_ssl_new ();

			ct->threadId = Tcl_GetCurrentThread();

			Tcl_CreateEventSource (casstcl_EventSetupProc, casstcl_EventCheckProc, NULL);

			commandName = Tcl_GetString (objv[2]);

			// if commandName is #auto, generate a unique name for the object
			autoGeneratedName = 0;
			if (strcmp (commandName, "#auto") == 0) {
				static unsigned long nextAutoCounter = 0;
				char *objName;
				int    baseNameLength;

				objName = Tcl_GetStringFromObj (objv[0], &baseNameLength);
				baseNameLength += snprintf (NULL, 0, "%lu", nextAutoCounter) + 1;
				commandName = ckalloc (baseNameLength);
				snprintf (commandName, baseNameLength, "%s%lu", objName, nextAutoCounter++);
				autoGeneratedName = 1;
			}

			// create a Tcl command to interface to cass
			ct->cmdToken = Tcl_CreateObjCommand (interp, commandName, casstcl_cassObjectObjCmd, ct, casstcl_cassObjectDelete);
			Tcl_SetObjResult (interp, Tcl_NewStringObj (commandName, -1));
			if (autoGeneratedName == 1) {
				ckfree(commandName);
			}
			break;
		}

		case OPT_SET_LOGGING_CALLBACK: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 1, objv, "option arg");
				return TCL_ERROR;
			}

			// if it already isn't null it was set to something, decrement
			// that object's reference count so it will probably be
			// deleted
			if (casstcl_loggingCallbackObj != NULL) {
				Tcl_DecrRefCount (casstcl_loggingCallbackObj);
			}

			casstcl_loggingCallbackObj = objv[2];
			Tcl_IncrRefCount (casstcl_loggingCallbackObj);

			casstcl_loggingCallbackThreadId = Tcl_GetCurrentThread();

			cass_log_set_callback (casstcl_logging_callback, interp);
			break;
		}
	}

    return TCL_OK;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
