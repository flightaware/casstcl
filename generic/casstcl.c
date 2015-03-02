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
			break;
		}

        case OPT_CRITICAL: {
			*cassLogLevel = CASS_LOG_CRITICAL;
			break;
		}

        case OPT_ERROR: {
			*cassLogLevel = CASS_LOG_ERROR;
			break;
		}

		case OPT_WARN: {
			*cassLogLevel = CASS_LOG_WARN;
			break;
		}

        case OPT_INFO: {
			*cassLogLevel = CASS_LOG_INFO;
			break;
		}

        case OPT_DEBUG: {
			*cassLogLevel = CASS_LOG_DEBUG;
			break;
		}

        case OPT_TRACE: {
			*cassLogLevel = CASS_LOG_TRACE;
			break;
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
 * casstcl_string_to_cass_value_type -- lookup a string
 *   to be one of the cass value type strings for CassValueType and set
 *   a pointer to a passed-in CassValueType value to the corresponding
 *   type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Results:
 *      ...cass value type gets returned
 *      CASS_VALUE_TYPE_UNKNOWN is returned if nothing matches
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
CassValueType
casstcl_string_to_cass_value_type (char *string) {
	switch (*string) {
		case 'a': {
			if (strcmp (string, "ascii") == 0) return CASS_VALUE_TYPE_ASCII;
			break;
		}

		case 'b': {
			if (strcmp (string, "bigint") == 0) return CASS_VALUE_TYPE_BIGINT;
			if (strcmp (string, "blob") == 0) return CASS_VALUE_TYPE_BLOB;
			if (strcmp (string, "boolean") == 0) return CASS_VALUE_TYPE_BOOLEAN;
			break;
		}

		case 'c': {
			if (strcmp (string, "counter") == 0) return CASS_VALUE_TYPE_COUNTER;
			if (strcmp (string, "custom") == 0) return CASS_VALUE_TYPE_CUSTOM;
			break;
		}

		case 'd': {
			if (strcmp (string, "decimal") == 0) return CASS_VALUE_TYPE_DECIMAL;
			if (strcmp (string, "double") == 0) return CASS_VALUE_TYPE_DOUBLE;
			break;
		}

		case 'f': {
			if (strcmp (string, "float") == 0) return CASS_VALUE_TYPE_FLOAT;
			break;
		}

		case 'i': {
			if (strcmp (string, "int") == 0) return CASS_VALUE_TYPE_INT;
			if (strcmp (string, "inet") == 0) return CASS_VALUE_TYPE_INET;
			break;
		}

		case 'l': {
			if (strcmp (string, "list") == 0) return CASS_VALUE_TYPE_LIST;
			break;
		}

		case 'm': {
			if (strcmp (string, "map") == 0) return CASS_VALUE_TYPE_MAP;
			break;
		}

		case 's': {
			if (strcmp (string, "set") == 0) return CASS_VALUE_TYPE_SET;
			break;
		}

		case 't': {
			if (strcmp (string, "text") == 0) return CASS_VALUE_TYPE_TEXT;
			if (strcmp (string, "timestamp") == 0) return CASS_VALUE_TYPE_TIMESTAMP;
			if (strcmp (string, "timeuuid") == 0) return CASS_VALUE_TYPE_TIMEUUID;
			break;
		}

		case 'u': {
			if (strcmp (string, "uuid") == 0) return CASS_VALUE_TYPE_UUID;
			if (strcmp (string, "unknown") == 0) return CASS_VALUE_TYPE_UNKNOWN;
			break;
		}

		case 'v': {
			if (strcmp (string, "varchar") == 0) return CASS_VALUE_TYPE_VARCHAR;
			if (strcmp (string, "varint") == 0) return CASS_VALUE_TYPE_VARINT;
			break;
		}
		return CASS_VALUE_TYPE_UNKNOWN;
	}

	return CASS_VALUE_TYPE_UNKNOWN;
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_value_type_to_string -- given a CassConsistency,
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
casstcl_cass_value_type_to_string (CassConsistency consistency) {
	switch (consistency) {
		case CASS_CONSISTENCY_ANY: {
			return "any";
		}

		case CASS_CONSISTENCY_ONE: {
			return "one";
		}

		case CASS_CONSISTENCY_TWO: {
			return "two";
		}

		case CASS_CONSISTENCY_THREE: {
			return "three";
		}

		case CASS_CONSISTENCY_QUORUM: {
			return "quorum";
		}

		case CASS_CONSISTENCY_ALL: {
			return "all";
		}

		case CASS_CONSISTENCY_LOCAL_QUORUM: {
			return "local_quorum";
		}

		case CASS_CONSISTENCY_EACH_QUORUM: {
			return "each_quorum";
		}

		case CASS_CONSISTENCY_SERIAL: {
			return "serial";
		}

		case CASS_CONSISTENCY_LOCAL_SERIAL: {
			return "local_serial";
		}

		case CASS_CONSISTENCY_LOCAL_ONE: {
			return "local_one";
		}

		default:
			return "unknown";
	}
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_compound_cass_value_types
 *
 * Lookup a string from a Tcl object and identify it as one of the cass
 * value type strings for CassValueType (int, text uuid, etc.) and set
 * a pointer to a passed-in CassValueType value to the corresponding
 * type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Also if it is a list of "map type type" or "set type" or "list type"
 * then set the valueSubType1 to type defined by the set or list and
 * in the case of a map set valueSubType1 for the key datatype and
 * valueSubType2 for the value datatype
 *
 * Results:
 *      ...cass value types gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_compound_cass_value_types (Tcl_Interp *interp, Tcl_Obj *tclObj, CassValueType *cassValueType, CassValueType *valueSubType1, CassValueType *valueSubType2) {
	int listObjc;
	Tcl_Obj **listObjv;

	*cassValueType = CASS_VALUE_TYPE_UNKNOWN;
	*valueSubType1 = CASS_VALUE_TYPE_UNKNOWN;
	*valueSubType2 = CASS_VALUE_TYPE_UNKNOWN;

	// try straight lookup.  this should get everything except for
	// maps, sets and lists
	char *string = Tcl_GetString (tclObj);
	CassValueType valueType = casstcl_string_to_cass_value_type (string);

	if (valueType != CASS_VALUE_TYPE_UNKNOWN) {
		*cassValueType = valueType;
		return TCL_OK;
	}

	if (Tcl_ListObjGetElements (interp, tclObj, &listObjc, &listObjv) == TCL_ERROR) {
		Tcl_AppendResult (interp, " while parsing cassandra data type", NULL);
		return TCL_ERROR;
	}

	// the list parsed, now look up the first element, if we don't find it
	// in the type list, we have a bad tyupe
	string = Tcl_GetString (listObjv[0]);
	valueType = casstcl_string_to_cass_value_type (string);

	if ((valueType != CASS_VALUE_TYPE_MAP) && (valueType != CASS_VALUE_TYPE_SET) && (valueType != CASS_VALUE_TYPE_LIST)) {
		Tcl_AppendResult (interp, "cassandra type spec is invalid", NULL);
		return TCL_ERROR;
	}

	if ((valueType == CASS_VALUE_TYPE_MAP && listObjc != 3) || (listObjc != 2)) {
		Tcl_AppendResult (interp, "cassandra type spec wrong # of values", NULL);
		return TCL_ERROR;
	}

	// at this point it's a colleciton and the list count is correct so
	// there is at least one subType that has to be looked up successfully

	*valueSubType1 = casstcl_string_to_cass_value_type (Tcl_GetString(listObjv[1]));
	if (*valueSubType1 == CASS_VALUE_TYPE_UNKNOWN) {
		Tcl_AppendResult (interp, "cassandra type spec unrecognized collection subtype", NULL);
		return TCL_ERROR;
	}

	// only for maps there is a second subType to be checked, converted
	if (valueType == CASS_VALUE_TYPE_MAP) {
		*valueSubType2 = casstcl_string_to_cass_value_type (Tcl_GetString(listObjv[2]));
		if (*valueSubType2 == CASS_VALUE_TYPE_UNKNOWN) {
			Tcl_AppendResult (interp, "cassandra type spec unrecognized map collection value subtype", NULL);
			return TCL_ERROR;
		}
	}

	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_error_to_tcl -- given a CassError code and a field
 *   name, if the error code is CASS_OK return TCL_OK but if it's anything
 *   else, set the interpreter result to the corresponding error string
 *   and set the error code to CASSANDRA and the e-code like
 *   CASS_ERROR_LIB_BAD_PARAMS
 *
 * Results:
 *      A standard Tcl result
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
	Tcl_ResetResult (ct->interp);
	Tcl_SetErrorCode (ct->interp, "CASSANDRA", cassErrorCodeString, cassErrorDesc, NULL);
	Tcl_AppendResult (ct->interp, "cassandra error: ", cassErrorDesc, NULL);
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
 * casstcl_cass_consistency_to_string -- given a CassValueType,
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
casstcl_cass_consistency_to_string (CassValueType valueType) {
	switch (valueType) {
		case CASS_VALUE_TYPE_UNKNOWN: {
			return "unknown";
		}

		case CASS_VALUE_TYPE_CUSTOM: {
			return "unknown";
		}

		case CASS_VALUE_TYPE_ASCII: {
			return "custom";
		}

		case CASS_VALUE_TYPE_BIGINT: {
			return "ascii";
		}

		case CASS_VALUE_TYPE_BLOB: {
			return "blob";
		}

		case CASS_VALUE_TYPE_BOOLEAN: {
			return "boolean";
		}

		case CASS_VALUE_TYPE_COUNTER: {
			return "counter";
		}

		case CASS_VALUE_TYPE_DECIMAL: {
			return "decimal";
		}

		case CASS_VALUE_TYPE_DOUBLE: {
			return "double";
		}

		case CASS_VALUE_TYPE_FLOAT: {
			return "float";
		}

		case CASS_VALUE_TYPE_INT: {
			return "int";
		}

		case CASS_VALUE_TYPE_TEXT: {
			return "text";
		}

		case CASS_VALUE_TYPE_TIMESTAMP: {
			return "timestamp";
		}

		case CASS_VALUE_TYPE_UUID: {
			return "uuid";
		}

		case CASS_VALUE_TYPE_VARCHAR: {
			return "varchar";
		}

		case CASS_VALUE_TYPE_VARINT: {
			return "varint";
		}

		case CASS_VALUE_TYPE_TIMEUUID: {
			return "timeuuid";
		}

		case CASS_VALUE_TYPE_INET: {
			return "inet";
		}

		case CASS_VALUE_TYPE_LIST: {
			return "list";
		}

		case CASS_VALUE_TYPE_MAP: {
			return "map";
		}

		case CASS_VALUE_TYPE_SET: {
			return "set";
		}

		default:
			return "unknown";
	}
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_keyspaces --
 *
 *      Return a list of the extant keyspaces in the cluster by
 *      examining the metadata managed by the driver.
 *
 *      The cpp-driver docs indicate that the driver stays abreast with
 *      changes to the schema so we prefer to ask it rather than
 *      caching our own copy, or something.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_list_keyspaces (casstcl_sessionClientData *ct, Tcl_Obj **objPtr) {
	const CassSchema *schema = cass_session_get_schema(ct->session);
	CassIterator *iterator = cass_iterator_from_schema(schema);
	Tcl_Obj *listObj = Tcl_NewObj();
	int tclReturn = TCL_OK;

	while (cass_iterator_next(iterator)) {
		CassString name;
		const CassSchemaMeta *schemaMeta = cass_iterator_get_schema_meta (iterator);

		const CassSchemaMetaField* field = cass_schema_meta_get_field(schemaMeta, "keyspace_name");
		cass_value_get_string(cass_schema_meta_field_value(field), &name);
		if (Tcl_ListObjAppendElement (ct->interp, listObj, Tcl_NewStringObj (name.data, name.length)) == TCL_ERROR) {
			tclReturn = TCL_ERROR;
			break;
		}
	}
	cass_iterator_free(iterator);
	cass_schema_free(schema);
	*objPtr = listObj;
	return tclReturn;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_tables --
 *
 *      Set the Tcl result to a list of the extant tables in a keyspace by
 *      examining the metadata managed by the driver.
 *
 *      This is cool because the driver will update the metadata if the
 *      schema changes during the session and further examinations of the
 *      metadata by the casstcl metadata-accessing functions will see the
 *      changes
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_list_tables (casstcl_sessionClientData *ct, char *keyspace, Tcl_Obj **objPtr) {
	const CassSchema *schema = cass_session_get_schema(ct->session);
	const CassSchemaMeta *keyspaceMeta = cass_schema_get_keyspace (schema, keyspace);

	if (keyspaceMeta == NULL) {
		Tcl_AppendResult (ct->interp, "keyspace '", keyspace, "' not found", NULL);
		return TCL_ERROR;
	}

	CassIterator *iterator = cass_iterator_from_schema_meta (keyspaceMeta);
	Tcl_Obj *listObj = Tcl_NewObj();
	int tclReturn = TCL_OK;

	while (cass_iterator_next(iterator)) {
		CassString name;
		const CassSchemaMeta *tableMeta = cass_iterator_get_schema_meta (iterator);

		assert (cass_schema_meta_type(tableMeta) == CASS_SCHEMA_META_TYPE_TABLE);

		const CassSchemaMetaField* field = cass_schema_meta_get_field(tableMeta, "columnfamily_name");
		assert (field != NULL);
		cass_value_get_string(cass_schema_meta_field_value(field), &name);
		if (Tcl_ListObjAppendElement (ct->interp, listObj, Tcl_NewStringObj (name.data, name.length)) == TCL_ERROR) {
			tclReturn = TCL_ERROR;
			break;
		}
	}
	cass_iterator_free(iterator);
	cass_schema_free(schema);
	*objPtr = listObj;
	return tclReturn;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_list_columns --
 *
 *      Set a Tcl object pointer to a list of the extant columns in the
 *      specified table in the specified keyspace by examining the
 *      metadata managed by the driver.
 *
 *      If includeTypes is 1 then instead of listing just the columns it
 *      also lists their data types, as a list of key-value pairs.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_list_columns (casstcl_sessionClientData *ct, char *keyspace, char *table, int includeTypes, Tcl_Obj **objPtr) {
	const CassSchema *schema = cass_session_get_schema(ct->session);

	// locate the keyspace
	const CassSchemaMeta *keyspaceMeta = cass_schema_get_keyspace (schema, keyspace);
	Tcl_Interp *interp = ct->interp;

	if (keyspaceMeta == NULL) {
		Tcl_AppendResult (ct->interp, "keyspace '", keyspace, "' not found", NULL);
		return TCL_ERROR;
	}

	// locate the table within the keyspace
	const CassSchemaMeta *tableMeta = cass_schema_meta_get_entry (keyspaceMeta, table);

	if (tableMeta == NULL) {
		Tcl_AppendResult (ct->interp, "table '", table, "' not found in keyspace '", keyspace, "'", NULL);
		return TCL_ERROR;
	}

	// prepare to iterate on the columns within the table
	CassIterator *iterator = cass_iterator_from_schema_meta (tableMeta);
	Tcl_Obj *listObj = Tcl_NewObj();
	int tclReturn = TCL_OK;

	// iterate on the columns within the table
	while (cass_iterator_next(iterator)) {
		CassString name;
		const CassSchemaMeta *columnMeta = cass_iterator_get_schema_meta (iterator);

		assert (cass_schema_meta_type(columnMeta) == CASS_SCHEMA_META_TYPE_COLUMN);

		// get the field name and append it to the list we are creating
		const CassSchemaMetaField* field = cass_schema_meta_get_field(columnMeta, "column_name");
		assert (field != NULL);
		const CassValue *fieldValue = cass_schema_meta_field_value(field);
		CassValueType valueType = cass_value_type (fieldValue);

		// it's a crash if you don't check the data type of valueType
		// there's something fishy in system.IndexInfo, a field that
		// doesn't  have a column name
		if (valueType != CASS_VALUE_TYPE_VARCHAR) {
			continue;
		}
		cass_value_get_string(fieldValue, &name);
		if (Tcl_ListObjAppendElement (ct->interp, listObj, Tcl_NewStringObj (name.data, name.length)) == TCL_ERROR) {
			tclReturn = TCL_ERROR;
			break;
		}
		// if including types then get the data type and append it to the
		// list too
		if (includeTypes) {
			CassString name;
			const CassSchemaMetaField* field = cass_schema_meta_get_field (columnMeta, "validator");
			assert (field != NULL);

			cass_value_get_string(cass_schema_meta_field_value(field), &name);

			// check the cache array directly from C to avoid calling
			// Tcl_Eval if possible
			Tcl_Obj *elementObj = Tcl_GetVar2Ex (interp, "::casstcl::validatorTypeLookupCache", name.data, (TCL_GLOBAL_ONLY));

			// not there, gotta call Tcl to do the heavy lifting
			if (elementObj == NULL) {
				Tcl_Obj *evalObjv[2];
				// construct a call to our casstcl.tcl library function
				// validator_to_type to translate the value to a cassandra
				// data type to text/list
				evalObjv[0] = Tcl_NewStringObj ("::casstcl::validator_to_type", -1);

				evalObjv[1] = Tcl_NewStringObj (name.data, name.length);

				Tcl_IncrRefCount (evalObjv[0]);
				Tcl_IncrRefCount (evalObjv[1]);
				tclReturn = Tcl_EvalObjv (ct->interp, 2, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));
				Tcl_DecrRefCount(evalObjv[0]);
				Tcl_DecrRefCount(evalObjv[1]);

				if (tclReturn == TCL_ERROR) {
					goto error;
				}
				tclReturn = TCL_OK;

				elementObj = Tcl_GetObjResult (ct->interp);
				Tcl_IncrRefCount(elementObj);
				// Tcl_ResetResult (interp);
			}

			// we got here, either we found elementObj by looking it up
			// from the ::casstcl::validatorTypeLookCache array or
			// by invoking eval on ::casstcl::validator_to_type

			if (Tcl_ListObjAppendElement (ct->interp, listObj, elementObj) == TCL_ERROR) {
				tclReturn = TCL_ERROR;
				break;
			}
		}
	}
  error:
	cass_iterator_free(iterator);
	cass_schema_free(schema);
	*objPtr = listObj;

	if (tclReturn == TCL_OK) {
		Tcl_ResetResult (interp);
	}

	return tclReturn;
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
 *--------------------------------------------------------------
 *
 * casstcl_batchClientData -- given a batch command name, find it
 *   in the interpreter and return a pointer to its batch client
 *   data or NULL
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
casstcl_batchClientData *
casstcl_batch_command_to_batchClientData (casstcl_sessionClientData *ct, char *batchCommandName)
{
	Tcl_CmdInfo batchCmdInfo;
	Tcl_Interp *interp = ct->interp;
	
	if (!Tcl_GetCommandInfo (interp, batchCommandName, &batchCmdInfo)) {
//printf("batchCommandName lookup failed for '%s'\n", batchCommandName);
		return NULL;
	}

	casstcl_batchClientData *bcd = (casstcl_batchClientData *)batchCmdInfo.objClientData;
    if (bcd->cass_batch_magic != CASS_BATCH_MAGIC) {
//printf("batch magic check failed\n");
		return NULL;
	}

//printf("batchCommandName lookup succeeded for '%s'\n", batchCommandName);
	return bcd;
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
	Tcl_IncrRefCount (evalObjv[0]);
	Tcl_IncrRefCount (evalObjv[1]);
	tclReturnCode = Tcl_EvalObjv (interp, CASSTCL_LOG_CALLBACK_ARGCOUNT, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	// if we got a Tcl error, since we initiated the event, it doesn't
	// have anything to traceback further from here to, we must initiate
	// a background error, which will generally cause the bgerror proc
	// to get invoked
	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
	}

	Tcl_DecrRefCount(evalObjv[0]);
	Tcl_DecrRefCount(evalObjv[1]);

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
 * casstcl_cass_value_to_tcl_obj --
 *
 *      Given a cassandra CassValue, generate a Tcl_Obj of a corresponding
 *      type
 *
 *      This is a vital routine to the entire edifice.
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
 * casstcl_append_tcl_obj_to_collection
 *
 * Convert a Tcl object to a cassandra value of the specified type and
 * append it to the specified collection
 *
 * This is used for constructing cassandra maps, sets and lists.
 *
 * You create a set or a list by appending elements to it.
 *
 * You create a map by appending successions of key elements and value
 * elements to it.
 *
 * They have a specified datatype for sets and lists; for keys there is
 * one for the key and one for the value so for instance the keys can
 * be integers and the values can be strings or whatever.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */
int casstcl_append_tcl_obj_to_collection (casstcl_sessionClientData *ct, CassCollection *collection, CassValueType valueType, Tcl_Obj *obj) {
	CassError cassError = CASS_OK;
	Tcl_Interp *interp = ct->interp;

	switch (valueType) {
		case CASS_VALUE_TYPE_ASCII:
		case CASS_VALUE_TYPE_TEXT:
		case CASS_VALUE_TYPE_VARCHAR: {
			int length = 0;
			char *value = Tcl_GetStringFromObj (obj, &length);

			cassError = cass_collection_append_string (collection, cass_string_init(value));
			break;
		}

		case CASS_VALUE_TYPE_VARINT:
		case CASS_VALUE_TYPE_BLOB: {
			int length = 0;
			unsigned char *value = Tcl_GetByteArrayFromObj (obj, &length);

			cassError = cass_collection_append_bytes (collection, cass_bytes_init(value, length));
			break;
		}

		case CASS_VALUE_TYPE_BOOLEAN: {
			int value = 0;

			if (Tcl_GetBooleanFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting boolean element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_collection_append_bool (collection, value);
			break;
		}

		case CASS_VALUE_TYPE_TIMESTAMP:
		case CASS_VALUE_TYPE_BIGINT:
		case CASS_VALUE_TYPE_COUNTER: {
			Tcl_WideInt wideValue = 0;

			if (Tcl_GetWideIntFromObj (interp, obj, &wideValue) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting wide int element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_collection_append_int64 (collection, wideValue);
			break;
		}


		case CASS_VALUE_TYPE_DOUBLE: {
			double value = 0;

			if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting double element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_collection_append_double (collection, value);
			break;
		}

		case CASS_VALUE_TYPE_FLOAT: {
			double value = 0;

			if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting float element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_collection_append_float (collection, value);
			break;
		}

		case CASS_VALUE_TYPE_INT: {
			int value = 0;

			if (Tcl_GetIntFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting int element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_collection_append_int32 (collection, value);
			break;
		}

		case CASS_VALUE_TYPE_UUID: {
			break;
		}


		case CASS_VALUE_TYPE_TIMEUUID: {
			break;
		}

		case CASS_VALUE_TYPE_INET: {
			break;
		}

		default: {
			break;
		}
	}

	if (cassError != CASS_OK) {
		return casstcl_cass_error_to_tcl (ct, cassError);
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_tcl_obj --
 *
 * This routine binds Tcl objects to ?-substitution parameters in nascent
 * cassandra statements.
 *
 * It takes a statement, an index (which parameter to substitute left to
 * right from 0 to n-1), the cassandra data type (and subtype(s) if it is
 * a list, set or map), and it will convert the Tcl object to the specified
 * data type and bind it to the statement.
 *
 * This is a really important routine.
 *
 * If type conversion fails, like Cassandra wants floating point and the
 * Tcl object won't convert to floating point then it's a Tcl error.
 *
 * If everything works then TCL_OK is returned.
 *
 * valueSubType1 is only used for maps, sets and lists
 * valueSubType2 is only used for maps
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_bind_tcl_obj (casstcl_sessionClientData *ct, CassStatement *statement, cass_size_t index, CassValueType valueType, CassValueType valueSubType1, CassValueType valueSubType2, Tcl_Obj *obj)
{
	Tcl_Interp *interp = ct->interp;
	CassError cassError = CASS_OK;

	switch (valueType) {
		case CASS_VALUE_TYPE_ASCII:
		case CASS_VALUE_TYPE_TEXT:
		case CASS_VALUE_TYPE_VARCHAR: {
			int length = 0;
			char *value = Tcl_GetStringFromObj (obj, &length);

			cassError = cass_statement_bind_string (statement, index, cass_string_init(value));
			break;
		}

		case CASS_VALUE_TYPE_UNKNOWN: {
			break;
		}

		case CASS_VALUE_TYPE_CUSTOM: {
			int length = 0;
			unsigned char *value = Tcl_GetByteArrayFromObj (obj, &length);
			cass_byte_t *copyBuffer = NULL;

			cassError = cass_statement_bind_custom (statement, index, length, &copyBuffer);

			if (cassError == CASS_OK) {
				memcpy (copyBuffer, value, length);
			}
			break;
		}


		case CASS_VALUE_TYPE_BLOB: {
			int length = 0;
			unsigned char *value = Tcl_GetByteArrayFromObj (obj, &length);

			cassError = cass_statement_bind_bytes (statement, index, cass_bytes_init(value, length));
			break;
		}

		case CASS_VALUE_TYPE_BOOLEAN: {
			int value = 0;

			if (Tcl_GetBooleanFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting boolean element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_statement_bind_bool (statement, index, value);
			break;
		}

		case CASS_VALUE_TYPE_TIMESTAMP:
		case CASS_VALUE_TYPE_BIGINT:
		case CASS_VALUE_TYPE_COUNTER: {
			Tcl_WideInt wideValue = 0;

			if (Tcl_GetWideIntFromObj (interp, obj, &wideValue) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting wide int element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_statement_bind_int64 (statement, index, wideValue);
			break;
		}

		case CASS_VALUE_TYPE_DECIMAL: {
			break;
		}

		case CASS_VALUE_TYPE_DOUBLE: {
			double value = 0;

			if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting double element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_statement_bind_double (statement, index, value);
			break;
		}

		case CASS_VALUE_TYPE_FLOAT: {
			double value = 0;

			if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting float element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_statement_bind_float (statement, index, value);
			break;
		}

		case CASS_VALUE_TYPE_INT: {
			int value = 0;

			if (Tcl_GetIntFromObj (interp, obj, &value) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting int element", NULL);
				return TCL_ERROR;
			}

			cassError = cass_statement_bind_int32 (statement, index, value);
			break;
		}

		case CASS_VALUE_TYPE_UUID: {
			break;
		}

		case CASS_VALUE_TYPE_VARINT: {
			break;
		}

		case CASS_VALUE_TYPE_TIMEUUID: {
			break;
		}

		case CASS_VALUE_TYPE_INET: {
			break;
		}

		case CASS_VALUE_TYPE_SET:
		case CASS_VALUE_TYPE_LIST: {
			int listObjc;
			Tcl_Obj **listObjv;
			int i;

			CassCollectionType collectionType = (valueType == CASS_VALUE_TYPE_SET) ? CASS_COLLECTION_TYPE_SET : CASS_COLLECTION_TYPE_LIST;

			if (Tcl_ListObjGetElements (interp, obj, &listObjc, &listObjv) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting map element", NULL);
				return TCL_ERROR;
			}

			CassCollection *collection = cass_collection_new (collectionType, listObjc);

			for (i = 0; i < listObjc; i++) {
				cassError = casstcl_append_tcl_obj_to_collection (ct, collection, valueSubType1, listObjv[i]);
				if (cassError != CASS_OK) {
					break;
				}
			}

			cassError = cass_statement_bind_collection (statement, index, collection);
			cass_collection_free (collection);

			break;
		}

		case CASS_VALUE_TYPE_MAP: {
			int listObjc;
			Tcl_Obj **listObjv;
			int i;

			if (Tcl_ListObjGetElements (interp, obj, &listObjc, &listObjv) == TCL_ERROR) {
				Tcl_AppendResult (interp, " while converting map element", NULL);
				return TCL_ERROR;
			}

			if (listObjc & 1) {
				Tcl_AppendResult (interp, "list must contain an even number of elements while converting map element", NULL);
				return TCL_ERROR;
			}

			CassCollection *collection = cass_collection_new (CASS_COLLECTION_TYPE_MAP, listObjc);

			for (i = 0; i < listObjc; i += 2) {
				cassError = casstcl_append_tcl_obj_to_collection (ct, collection, valueSubType1, listObjv[i]);
				if (cassError != CASS_OK) {
					break;
				}

				cassError = casstcl_append_tcl_obj_to_collection (ct, collection, valueSubType2, listObjv[i+1]);
				if (cassError != CASS_OK) {
					break;
				}
			}

			cassError = cass_statement_bind_collection (statement, index, collection);
			cass_collection_free (collection);

			break;
		}
	}

	if (cassError != CASS_OK) {
		return casstcl_cass_error_to_tcl (ct, cassError);
	}

	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_values_and_types --
 *
 *   Now this little ditty takes a query and an objv and a pointer to
 *   a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list pairs where the first element
 *   is a value and the second element is a cassandra data type
 *
 *   For each pair it interprets the data type, converts the element to
 *   that type, and binds it to that position in the statement.
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_values_and_types (casstcl_sessionClientData *ct, char *query, int objc, Tcl_Obj *CONST objv[], CassStatement **statementPtr)
{
	int i;
	int masterReturn = TCL_OK;
	int tclReturn = TCL_OK;
	Tcl_Interp *interp = ct->interp;

	CassValueType valueType = CASS_VALUE_TYPE_UNKNOWN;
	CassValueType valueSubType1 = CASS_VALUE_TYPE_UNKNOWN;
	CassValueType valueSubType2 = CASS_VALUE_TYPE_UNKNOWN;

	*statementPtr = NULL;

	if (objc & 1) {
		Tcl_AppendResult (interp, "values_and_types list must contain an even number of elements", NULL);
		return TCL_ERROR;
	}

	CassStatement *statement = cass_statement_new(cass_string_init(query), objc / 2);

	for (i = 0; i < objc; i += 2) {
		tclReturn = casstcl_obj_to_compound_cass_value_types (interp, objv[i+1], &valueType, &valueSubType1, &valueSubType2);

		if (tclReturn == TCL_ERROR) {
			masterReturn = TCL_ERROR;
			break;
		}

		tclReturn = casstcl_bind_tcl_obj (ct, statement, i / 2, valueType, valueSubType1, valueSubType2, objv[i]);

		if (tclReturn == TCL_ERROR) {
			masterReturn = TCL_ERROR;
			break;
		}
	}

	if (masterReturn == TCL_OK) {
		*statementPtr = statement;
	}

	return masterReturn;
}

// Tcl type definition for caching CassValueType

// we don't have to free any internal representation because our representation
// fits into the exist Tcl_Obj definition without a pointer to anything else

// we never invalidate the string representation so we can set the
// UpdateStringOf... function pointer to null

void DupCassTypeTypeInternalRep (Tcl_Obj *srcPtr, Tcl_Obj *copyPtr);
int SetCassTypeTypeFromAny (Tcl_Interp *interp, Tcl_Obj *obj);

// Tcl object type definition for the internal representation of a
// cassTypeTclType.  this allows us to cache the lookup of a type
// like "int" to CASS_VALUE_TYPE_INT or
// "map int text" to
// CASS_VALUE_TYPE_MAP CASS_VALUE_TYPE_INT CASS_VALUE_TYPE_TEXT
//
Tcl_ObjType casstcl_cassTypeTclType = {
	"CassType",
	NULL,
	DupCassTypeTypeInternalRep,
	NULL,
	SetCassTypeTypeFromAny
};

// copy the internal representation of a cassTypeTclType Tcl object
// to a new Tcl object
void
DupCassTypeTypeInternalRep (Tcl_Obj *srcPtr, Tcl_Obj *copyPtr)
{
	// not much to this... since we use the wide int representation,
	// all we have to do is copy the wide into from the source to the copy
	copyPtr->internalRep.wideValue = srcPtr->internalRep.wideValue;
	copyPtr->typePtr = &casstcl_cassTypeTclType;
}

// convert any tcl object to be a cassTypeTclType
int
SetCassTypeTypeFromAny (Tcl_Interp *interp, Tcl_Obj *obj)
{
	casstcl_cassTypeInfo *typeInfo = (casstcl_cassTypeInfo *)&obj->internalRep.wideValue;

	// convert it using our handy routine for doing that
	// if we get TCL_ERROR, it's an error
	// if we get TCL_OK we need to set the object's type pointer to point
	// to our custom type.
	//
	// after this if the object isn't altered future calls to
	// Tcl_ConvertToType will not do anything and access to the
	// internal representation will be quick
	//
	// see casstcl_type_index_name_to_cass_value_types
	//
	if (casstcl_obj_to_compound_cass_value_types (interp, obj, &typeInfo->cassValueType, &typeInfo->valueSubType1, &typeInfo->valueSubType2) == TCL_OK) {
		obj->typePtr = &casstcl_cassTypeTclType;
		return TCL_OK;
	}
	return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_type_index_name_to_cass_value_types --
 *
 *   Look up the validator in the column type map and decode it into
 *   three CassValueType entries, with extremely fast caching
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_type_index_name_to_cass_value_types (Tcl_Interp *interp, char *typeIndexName, CassValueType *valueType, CassValueType *valueSubType1, CassValueType *valueSubType2) {
	Tcl_Obj *typeObj = Tcl_GetVar2Ex (interp, "::casstcl::columnTypeMap", typeIndexName, (TCL_GLOBAL_ONLY|TCL_LEAVE_ERR_MSG));

	if (typeObj == NULL) {
#if 0
		Tcl_AppendResult (interp, " while trying to look up the data type for type index '", typeIndexName, "'", NULL);
		return TCL_ERROR;
#else
		return TCL_CONTINUE;
#endif
	}

	if (Tcl_ConvertToType (interp, typeObj, &casstcl_cassTypeTclType) == TCL_ERROR) {
		return TCL_ERROR;
	}

	casstcl_cassTypeInfo *typeInfo = (casstcl_cassTypeInfo *)&typeObj->internalRep.wideValue;
	*valueType = typeInfo->cassValueType;
	*valueSubType1 = typeInfo->valueSubType1;
	*valueSubType2 = typeInfo->valueSubType2;
	return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_array --
 *
 *   Now this little ditty takes an array name and a query and an objv
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list of column names
 *
 *   It fetches the data type of the column from the column-datatype cache
 *
 *   It fetches the value from the array and converts it and binds it
 *   to the statement
 *
 *   This requires that the table name and keyspace be known
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_array (casstcl_sessionClientData *ct, char *table, char *query, char *tclArray, int objc, Tcl_Obj *CONST objv[], CassStatement **statementPtr)
{
	int i;
	int masterReturn = TCL_OK;
	int tclReturn = TCL_OK;
	Tcl_Interp *interp = ct->interp;

	CassValueType valueType;
	CassValueType valueSubType1;
	CassValueType valueSubType2;

	*statementPtr = NULL;

	CassStatement *statement = cass_statement_new(cass_string_init(query), objc);

	for (i = 0; i < objc; i ++) {
		int varNameSize = 0;
		char *varName = Tcl_GetStringFromObj (objv[i], &varNameSize);
		int typeIndexSize = strlen (table) + 8 + varNameSize;
		char *typeIndexName = ckalloc (typeIndexSize);

		snprintf (typeIndexName, typeIndexSize, "%s.%s", table, varName);

		tclReturn = casstcl_type_index_name_to_cass_value_types (interp, typeIndexName, &valueType, &valueSubType1, &valueSubType2);

		if (tclReturn == TCL_ERROR) {
			masterReturn = TCL_ERROR;
			break;
		}

		// if the index name wasn't found in the row we get TCL_CONTINUE back.
		// NB we can either treat it as an error or not
		// NB eventually we can accumulate unrecognized types into a map
		if (tclReturn == TCL_CONTINUE) {
			tclReturn = TCL_OK;
			continue;
		}

		// get the value out of the array
		Tcl_Obj *valueObj = Tcl_GetVar2Ex (interp, tclArray, Tcl_GetString (objv[i]), (TCL_GLOBAL_ONLY|TCL_LEAVE_ERR_MSG));

		if (valueObj == NULL) {
			Tcl_AppendResult (interp, " while trying to look up the data value for column '", varName, "', ", table, "', ", table, "' from array '", tclArray, "'", NULL);
			masterReturn = TCL_ERROR;
			break;
		}

		tclReturn = casstcl_bind_tcl_obj (ct, statement, i, valueType, valueSubType1, valueSubType2, valueObj);
// printf ("bound arg %d as %d %d %d value '%s'\n", i, valueType, valueSubType1, valueSubType2, Tcl_GetString(valueObj));
		if (tclReturn == TCL_ERROR) {
			masterReturn = TCL_ERROR;
			break;
		}
	}

	if (masterReturn == TCL_OK) {
		*statementPtr = statement;
	}

	return masterReturn;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_list --
 *
 *   fully qualified table name
 *   name of the table
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   Similar to casstcl_bind_names_from_array
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_list (casstcl_sessionClientData *ct, char *table, char *query, int objc, Tcl_Obj *CONST objv[], CassStatement **statementPtr)
{
	int i;
	int masterReturn = TCL_OK;
	int tclReturn = TCL_OK;
	Tcl_Interp *interp = ct->interp;

	CassValueType valueType;
	CassValueType valueSubType1;
	CassValueType valueSubType2;

	*statementPtr = NULL;

	CassStatement *statement = cass_statement_new(cass_string_init(query), objc / 2);

//printf("objc = %d\n", objc);
	for (i = 0; i < objc; i += 2) {
//printf("i = %d\n", i);
		int varNameSize = 0;
		char *varName = Tcl_GetStringFromObj (objv[i], &varNameSize);

		// this can be factored and shared with casstcl_bind_names_from_list
		int typeIndexSize = strlen (table) + 8 + varNameSize;
		char *typeIndexName = ckalloc (typeIndexSize);

		snprintf (typeIndexName, typeIndexSize, "%s.%s", table, varName);
//printf ("typeIndexName '%s', typeIndexSize %d, table '%s', varName '%s'\n", typeIndexName, typeIndexSize, table, varName);

		tclReturn = casstcl_type_index_name_to_cass_value_types (interp, typeIndexName, &valueType, &valueSubType1, &valueSubType2);

		if (tclReturn == TCL_ERROR) {
//printf ("error from casstcl_obj_to_compound_cass_value_types\n");
			masterReturn = TCL_ERROR;
			break;
		}

		// failed to find it?
		if (tclReturn == TCL_CONTINUE) {
			tclReturn = TCL_OK;
			continue;
		}

		// get the value out of the list
		Tcl_Obj *valueObj = objv[i+1];


		tclReturn = casstcl_bind_tcl_obj (ct, statement, i / 2, valueType, valueSubType1, valueSubType2, valueObj);
//printf ("bound arg %d as %d %d %d value '%s'\n", i, valueType, valueSubType1, valueSubType2, Tcl_GetString(valueObj));
		if (tclReturn == TCL_ERROR) {
//printf ("error from casstcl_bind_tcl_obj\n");
			Tcl_AppendResult (interp, " while attempting to bind field '", varName, "' referencing table '", table, "'", NULL);
			masterReturn = TCL_ERROR;
			break;
		}
	}

//printf("finished the loop, i = %d, objc = %d\n", i, objc);
	if (masterReturn == TCL_OK) {
//printf("theoretically got a good statement\n");
		*statementPtr = statement;
	}

//printf("return code is %d\n", masterReturn);
	return masterReturn;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_make_upsert_statement --
 *
 *   given a session client data, fully qualified table name, Tcl list
 *   obj and a pointer to a statement pointer,
 *
 *   this baby...
 *
 *     ...generates an upsert statement (in the form of insert but that's
 *     how cassandra rolls)
 *
 *     ...uses casstcl_bind_names_from_list to bind the data elements
 *     to the statement using the right data types
 *
 *   It creates a cassandra statement and sets your pointer to it
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_make_upsert_statement (casstcl_sessionClientData *ct, char *tableName, Tcl_Obj *listObj, CassStatement **statementPtr) {
	int listObjc;
	Tcl_Obj **listObjv;
	Tcl_Interp *interp = ct->interp;

	if (Tcl_ListObjGetElements (interp, listObj, &listObjc, &listObjv) == TCL_ERROR) {
		Tcl_AppendResult (interp, " while parsing list of key-value pairs", NULL);
		return TCL_ERROR;
	}

	if (listObjc & 1) {
		Tcl_AppendResult (interp, "must contain an even number of elements", NULL);
		return TCL_ERROR;
	}

	Tcl_DString ds;
	Tcl_DStringInit (&ds);
	Tcl_DStringAppend (&ds, "INSERT INTO ", -1);
	Tcl_DStringAppend (&ds, tableName, -1);
	Tcl_DStringAppend (&ds, " (", 2);

	int i;
	int nDone = 0;
	int didOne = 0;

	for (i = 0; i < listObjc; i += 2) {
		int varNameLength;

		char *varName = Tcl_GetStringFromObj (listObjv[i], &varNameLength);

		// ignore Pgtcl meta variables
		if (*varName == '.') {
			continue;
		}

		if (didOne) {
			Tcl_DStringAppend (&ds, ",", 1);
		}
		didOne = 0;

		Tcl_DStringAppend (&ds, varName, varNameLength);
		nDone++;
		didOne = 1;
	}

	Tcl_DStringAppend (&ds, ") values (", -1);

	for (i = 0; i < nDone; i++) {
		if (i > 0) {
			Tcl_DStringAppend (&ds, ",?", 2);
		} else {
			Tcl_DStringAppend (&ds, "?", 1);
		}
	}
	Tcl_DStringAppend (&ds, ")", -1);

	char *query = Tcl_DStringValue (&ds);

//printf("casstcl_make_upsert_statement: query: %s\n", query);
	int tclReturn = casstcl_bind_names_from_list (ct, tableName, query, listObjc, listObjv, statementPtr);
	Tcl_DStringFree (&ds);
//printf("casstcl_make_upsert_statement: freed the dstring, returning %d\n", tclReturn);
	return tclReturn;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_make_statement_from_objv --
 *
 *   This is a beautiful thing because it will work from the like four
 *   places where we generate statements: exec, select, async, and
 *   batch.
 *
 *   This is like what would be in the option-handling case statements.
 *
 *   We will look at the objc and objv we are given with what's in front
 *   of the command that got invoked stripped off, that is for example
 *   if the command was
 *
 *       $batch add -array row $query column column column
 *
 *       $batch add $query $value $type $value $type
 *
 *   ...we expect to get it from "-array" on, that is, they'll pass us
 *   the address of the objv starting from there and the objc properly
 *   discounting whatever preceded the stuff we handle
 *
 *   We then figure it out and invoke the underlying stuff.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_make_statement_from_objv (casstcl_sessionClientData *ct, int objc, Tcl_Obj *CONST objv[], CassStatement **statementPtr) {
	int arrayStyle = 0;
	char *arrayName = NULL;
	char *tableName = NULL;
	Tcl_Interp *interp = ct->interp;

    static CONST char *options[] = {
        "-array",
		"-table",
        NULL
    };

    enum options {
        OPT_ARRAY,
		OPT_TABLE
	};

	int optIndex;
	int arg = 0;

	if (objc < 1) {
	  wrong_numargs:
		Tcl_WrongNumArgs (interp, 2, objv, "?-array arrayName? ?-table tableName? query ?arg...?");
		return TCL_ERROR;
	}

	while (arg + 1 < objc) {
		char *optionString = Tcl_GetString (objv[arg]);

		// if the first character isn't a dash, we're done here.
		// this is going to get called a lot so i don't want
		// Tcl_GetIndexFromObj writing an error message and all
		// that stuff unless there really is an option
		if (*optionString != '-') {
			break;
		}

		// OK so we aren't going to accept anything starting with - that
		// isn't in our option list
		if (Tcl_GetIndexFromObj (ct->interp, objv[arg++], options, "options",
			TCL_EXACT, &optIndex) != TCL_OK) {
			return TCL_ERROR;
		}

		switch ((enum options) optIndex) {
			case OPT_ARRAY: {
				if (arg + 1 >= objc) {
					goto wrong_numargs;
				}

				arrayName = Tcl_GetString (objv[arg++]);
				arrayStyle = 1;
				break;
			}

			case OPT_TABLE: {
				if (arg + 1 >= objc) {
					goto wrong_numargs;
				}

				tableName = Tcl_GetString (objv[arg++]);
				arrayStyle = 1;
				break;
			}
		}
	}

//printf ("looking for query, arg %d, objc %d\n", arg, objc);

	// there must at least be a query string left
	if (arg >= objc) {
		goto wrong_numargs;
	}

	char *query = Tcl_GetString (objv[arg++]);
	// (whatever is left of the objv from arg to the end are column-related)

	if (arrayStyle) {
		if (tableName == NULL) {
			Tcl_AppendResult (interp, "-table must be specified if -array is specified", NULL);
			return TCL_ERROR;
		}

		if (arrayName == NULL) {
			Tcl_AppendResult (interp, "-array must be specified if -table is specified", NULL);
			return TCL_ERROR;
		}

		return casstcl_bind_names_from_array (ct, tableName, query, arrayName, objc - arg, &objv[arg], statementPtr);
	} else {
		return casstcl_bind_values_and_types (ct, query, objc - arg, &objv[arg], statementPtr);
	}
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
 *      break, continue and return are supported (probably)
 *
 *      Issuing commands with async and processing the results with
 *      async foreach allows for greater concurrency.
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
			tclReturn = casstcl_cass_error_to_tcl (ct, rc);
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
		"upsert",
        "consistency",
		"reset",
        "delete",
        NULL
    };

    enum options {
        OPT_ADD,
        OPT_UPSERT,
        OPT_CONSISTENCY,
		OPT_RESET,
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
			if (casstcl_make_statement_from_objv (bcd->ct, objc - 2, &objv[2], &statement) == TCL_ERROR) {
				return TCL_ERROR;
			}

			CassError cassError = cass_batch_add_statement (bcd->batch, statement);
			cass_statement_free (statement);

			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (bcd->ct, cassError);
			}

			break;
		}

		case OPT_UPSERT: {
			if (objc != 4) {
				Tcl_WrongNumArgs (interp, 2, objv, "keyspace.tableName keyValuePairList");
				return TCL_ERROR;
			}

			CassStatement* statement;
			char *tableName = Tcl_GetString (objv[2]);
			Tcl_Obj *listObj = objv[3];

			
			resultCode = casstcl_make_upsert_statement (bcd->ct, tableName, listObj, &statement);
			if (resultCode != TCL_ERROR) {
//printf("calling cass_batch_add_statement\n");
				CassError cassError;
				cassError = cass_batch_add_statement (bcd->batch, statement);
				cass_statement_free (statement);
//printf("returned from cass_batch_add_statement, cassError %d\n", cassError);

				if (cassError != CASS_OK) {
					return casstcl_cass_error_to_tcl (bcd->ct, cassError);
				}
			}

			break;
		}

		case OPT_CONSISTENCY: {
			CassConsistency cassConsistency;

			if ((objc < 2) || (objc > 3)) {
				Tcl_WrongNumArgs (interp, 2, objv, "?consistency?");
				return TCL_ERROR;
			}

			if (objc == 2) {
				Tcl_SetObjResult (interp, Tcl_NewStringObj (casstcl_cass_value_type_to_string (bcd->consistency), -1));
				return TCL_OK;
			}

			if (casstcl_obj_to_cass_consistency(bcd->ct, objv[2], &cassConsistency) == TCL_ERROR) {
				return TCL_ERROR;
			}

			CassError cassError = cass_batch_set_consistency (bcd->batch, cassConsistency);
			bcd->consistency = cassConsistency;
			if (cassError != CASS_OK) {
				return casstcl_cass_error_to_tcl (bcd->ct, cassError);
			}
			break;
		}

		case OPT_RESET: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			cass_batch_free (bcd->batch);
			bcd->batch = cass_batch_new (bcd->batchType);
			CassError cassError = cass_batch_set_consistency (bcd->batch, bcd->consistency);
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
 * casstcl_reimport_column_type_map --
 *    Call out to the Tcl interpreter to invoke
 *    ::casstcl::import_column_type_map from the casstcl library;
 *    the proc resides in source file is casstcl.tcl.
 *
 *    This convenience function gets called from a method of the
 *    casstcl cass object and is invoked upon connection as well
 *
 * Results:
 *    The program compiles.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_reimport_column_type_map (casstcl_sessionClientData *ct)
{
	int tclReturnCode;
	Tcl_Interp *interp = ct->interp;
	Tcl_Obj *evalObjv[2];

	// construct an objv we'll pass to eval.
	// first is the callback command
	// second is the name of the future object this callback is related to
	evalObjv[0] = Tcl_NewStringObj ("::casstcl::import_column_type_map", -1);
	evalObjv[1] = Tcl_NewObj();
	Tcl_GetCommandFullName(interp, ct->cmdToken, evalObjv[1]);

	// eval the command.  it should be the callback we were told as the
	// first argument and the future object we created, like future0, as
	// the second.  go ahead and ask for direct, i.e. don't compile into
	// bytecodes, because our little single two-argument call is ephemeral

	Tcl_IncrRefCount (evalObjv[0]);
	Tcl_IncrRefCount (evalObjv[1]);

	tclReturnCode = Tcl_EvalObjv (interp, 2, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	Tcl_DecrRefCount(evalObjv[0]);
	Tcl_DecrRefCount(evalObjv[1]);

	return tclReturnCode;
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

	Tcl_IncrRefCount (evalObjv[0]);
	Tcl_IncrRefCount (evalObjv[1]);

	tclReturnCode = Tcl_EvalObjv (interp, 2, evalObjv, (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT));

	if (tclReturnCode == TCL_ERROR) {
		Tcl_BackgroundException (interp, TCL_ERROR);
	}

	Tcl_DecrRefCount(evalObjv[0]);
	Tcl_DecrRefCount(evalObjv[1]);

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
		"list_keyspaces",
		"list_tables",
		"list_columns",
		"list_column_types",
		"reimport_column_type_map",
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
		OPT_LIST_KEYSPACES,
		OPT_LIST_TABLES,
		OPT_LIST_COLUMNS,
		OPT_LIST_COLUMN_TYPES,
		OPT_REIMPORT_COLUMN_TYPE_MAP,
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
			CassError rc = CASS_OK;

			if (casstcl_make_statement_from_objv (ct, objc - 2, &objv[2], &statement) == TCL_ERROR) {
				return TCL_ERROR;
			}

			future = cass_session_execute (ct->session, statement);
			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				resultCode = casstcl_cass_error_to_tcl (ct, rc);
			}

			cass_future_free (future);
			cass_statement_free (statement);

			break;
		}

		case OPT_ASYNC: {
			CassStatement* statement = NULL;
			CassFuture *future = NULL;
			int arg = 2;
			int      subOptIndex;
			Tcl_Obj *callbackObj = NULL;
			char *batchObjName = NULL;

			static CONST char *subOptions[] = {
				"-callback",
				"-batch",
				NULL
			};

			enum subOptions {
				SUBOPT_CALLBACK,
				SUBOPT_BATCH
			};

			// if we don't have at least three arguments, it's an error
			if (objc < 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "?-callback n? ?-batch batchObject? ?-array array? ?-table table? statement ?args?");
				return TCL_ERROR;
			}

			while (arg + 1 < objc) {
				if (Tcl_GetIndexFromObj (interp, objv[arg++], subOptions, "subOption", TCL_EXACT, &subOptIndex) != TCL_OK) {
					break;
				}

				switch ((enum subOptions) subOptIndex) {
					case SUBOPT_CALLBACK: {
						callbackObj = objv[arg++];
						break;
					}

					case SUBOPT_BATCH: {
						batchObjName = Tcl_GetString (objv[arg++]);
						break;
					}
				}
			}

			if (batchObjName != NULL) {
				if (arg != objc) {
					Tcl_AppendResult (interp, "batch usage: obj ?-callback? -batch batchName", NULL);
					return TCL_ERROR;
				}

				// get the batch object from the command name we extracted
				casstcl_batchClientData *bcd = casstcl_batch_command_to_batchClientData (ct, batchObjName);
				if (bcd == NULL) {
					Tcl_AppendResult (interp, "batch object '", batchObjName, "' doesn't exist or isn't a batch object", NULL);
					return TCL_ERROR;
				}
				const CassBatch *batch = bcd->batch;

//printf("executing batch %lx\n", batch);
				future = cass_session_execute_batch (ct->session, batch);
//printf("returned from executing batch\n");

			} else {
				// it's a statement, possibly with arguments

				if (casstcl_make_statement_from_objv (ct, objc - arg, &objv[arg], &statement) == TCL_ERROR) {
					return TCL_ERROR;
				}

				future = cass_session_execute (ct->session, statement);
				cass_statement_free (statement);
			}

			if (casstcl_createFutureObjectCommand (ct, future, callbackObj) == TCL_ERROR) {
				resultCode = TCL_ERROR;
			}

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
			if (rc == CASS_OK) {
				// casstcl_reimport_column_type_map (ct);
			} else {
				resultCode = casstcl_cass_error_to_tcl (ct, rc);
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
				resultCode = casstcl_cass_error_to_tcl (ct, rc);
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
			bcd->batchType = cassBatchType;
			bcd->consistency = CASS_CONSISTENCY_ONE;

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
			Tcl_GetCommandFullName(interp, bcd->cmdToken, Tcl_GetObjResult (interp));
			if (autoGeneratedName == 1) {
				ckfree(commandName);
			}
			break;
		}

		case OPT_LIST_KEYSPACES: {
			Tcl_Obj *obj = NULL;
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			resultCode = casstcl_list_keyspaces (ct, &obj);
			Tcl_SetObjResult (ct->interp, obj);
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
			Tcl_SetObjResult (ct->interp, obj);
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
			Tcl_SetObjResult (ct->interp, obj);
			break;
		}

		case OPT_REIMPORT_COLUMN_TYPE_MAP: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			resultCode = casstcl_reimport_column_type_map (ct);
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
