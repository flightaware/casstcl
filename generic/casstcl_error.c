/*
 * casstcl_error - Functions used to interpret errors
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_error.h"

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

		case CASS_ERROR_SSL_INVALID_CERT:
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
 * casstcl_future_error_to_tcl -- given a CassError code and a future,
 *   if the error code is CASS_OK return TCL_OK but if it's anything
 *   else, set the interpreter result to the corresponding error message
 *   from the future object and set the error code to CASSANDRA and the
 *   e-code like CASS_ERROR_LIB_BAD_PARAMS
 *
 * Results:
 *      A standard Tcl result
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int casstcl_future_error_to_tcl (casstcl_sessionClientData *ct, CassError cassError, CassFuture *future) {

	if (cassError == CASS_OK) {
		return TCL_OK;
	}

	const char *cassErrorCodeString = casstcl_cass_error_to_errorcode_string (cassError);
	const char *cassErrorDesc = cass_error_desc (cassError);
	CassString message;
	cass_future_error_message (future, &message.data, &message.length);

	Tcl_ResetResult (ct->interp);
	Tcl_SetErrorCode (ct->interp, "CASSANDRA", cassErrorCodeString, cassErrorDesc, message.data, NULL);
	Tcl_AppendResult (ct->interp, "cassandra error: ", cassErrorDesc, ": ", message.data, NULL);
	return TCL_ERROR;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
