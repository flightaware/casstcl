/*
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include <assert.h>


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
    casstcl_clientData *ct = (casstcl_clientData *)clientData;

    assert (ct->cass_magic == CASS_MAGIC);

	cass_ssl_free (ct->ssl);
    cass_cluster_free (ct->cluster);
    cass_session_free (ct->session);
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
casstcl_obj_to_cass_log_level (casstcl_clientData *ct, Tcl_Obj *tclObj, CassLogLevel *cassLogLevel) {
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
int casstcl_cass_error_to_tcl (casstcl_clientData *ct, CassError cassError) {

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
casstcl_obj_to_cass_consistency(casstcl_clientData *ct, Tcl_Obj *tclObj, CassConsistency *cassConsistency) {
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

int casstcl_cass_value_to_tcl_obj (casstcl_clientData *ct, const CassValue *cassValue, Tcl_Obj **tclObj)
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

int casstcl_select (casstcl_clientData *ct, char *query, char *arrayName, Tcl_Obj *codeObj, int pagingSize) {
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

int
casstcl_list_keyspaces (casstcl_clientData *ct, Tcl_Obj **objPtr) {
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

// meta schema - list the schemas
// meta table - list the tables
// meta columns $table - lists the columns and their types

int
casstcl_meta_walk_table (casstcl_clientData *ct, const CassSchemaMeta *tableMeta) {
	CassIterator *iterator = NULL;
	// = cass_iterator_from_schema(tableMeta);
	const CassSchemaMetaField* field = NULL;
	CassString tableName;

	// should be CASS_SCHEMA_META_TYPE_TABLE
	
	field = cass_schema_meta_get_field (tableMeta, "columnfamily_name");
	cass_value_get_string (cass_schema_meta_field_value (field), &tableName);
	// lappend some list object table and a tablename  string object

	while (cass_iterator_next(iterator)) {
		const CassSchemaMeta *schemaMeta = cass_iterator_get_schema_meta (iterator);
	}
	return TCL_OK;
}

void print_schema_meta(const CassSchemaMeta* meta, int indent);

int
casstcl_meta_walk_schema (casstcl_clientData *ct) {
	const CassSchema *schema = cass_session_get_schema(ct->session);
	CassIterator *iterator = cass_iterator_from_schema(schema);

	while (cass_iterator_next(iterator)) {
		const CassSchemaMeta *schemaMeta = cass_iterator_get_schema_meta (iterator);

		print_schema_meta (schemaMeta, 0);
	}
	return TCL_OK;
}

void print_keyspace(CassSession* session, const char* keyspace) {
  const CassSchema* schema = cass_session_get_schema(session);
  const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

  if (keyspace_meta != NULL) {
    print_schema_meta(keyspace_meta, 0);
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_free(schema);
}

void print_table(CassSession* session, const char* keyspace, const char* table) {
  const CassSchema* schema = cass_session_get_schema(session);
  const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(schema, keyspace);

  if (keyspace_meta != NULL) {
    const CassSchemaMeta* table_meta = cass_schema_meta_get_entry(keyspace_meta, table);
    if (table_meta != NULL) {
      print_schema_meta(table_meta, 0);
    } else {
      fprintf(stderr, "Unable to find \"%s\" table in the schema metadata\n", table);
    }
  } else {
    fprintf(stderr, "Unable to find \"%s\" keyspace in the schema metadata\n", keyspace);
  }

  cass_schema_free(schema);
}

void print_schema_value(const CassValue* value);
void print_schema_list(const CassValue* value);
void print_schema_map(const CassValue* value);
void print_schema_meta_field(const CassSchemaMetaField* field, int indent);
void print_schema_meta_fields(const CassSchemaMeta* meta, int indent);
void print_schema_meta_entries(const CassSchemaMeta* meta, int indent);

void print_indent(int indent) {
  int i;
  for (i = 0; i < indent; ++i) {
    printf("\t");
  }
}

void print_schema_value(const CassValue* value) {
  cass_int32_t i;
  cass_bool_t b;
  cass_double_t d;
  CassString s;
  CassUuid u;
  char us[CASS_UUID_STRING_LENGTH];

  CassValueType type = cass_value_type(value);
  switch (type) {
    case CASS_VALUE_TYPE_INT:
      cass_value_get_int32(value, &i);
      printf("%d", i);
      break;

    case CASS_VALUE_TYPE_BOOLEAN:
      cass_value_get_bool(value, &b);
      printf("%s", b ? "true" : "false");
      break;

    case CASS_VALUE_TYPE_DOUBLE:
      cass_value_get_double(value, &d);
      printf("%f", d);
      break;

    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_VARCHAR:
      cass_value_get_string(value, &s);
      printf("\"%.*s\"", (int)s.length, s.data);
      break;

    case CASS_VALUE_TYPE_UUID:
      cass_value_get_uuid(value, &u);
      cass_uuid_string(u, us);
      printf("%s", us);
      break;

    case CASS_VALUE_TYPE_LIST:
      print_schema_list(value);
      break;

    case CASS_VALUE_TYPE_MAP:
      print_schema_map(value);
      break;

    default:
      if (cass_value_is_null(value)) {
        printf("null");
      } else {
        printf("<unhandled type>");
      }
      break;
  }
}

void print_schema_list(const CassValue* value) {
  CassIterator* iterator = cass_iterator_from_collection(value);
  cass_bool_t is_first = cass_true;

  printf("[ ");
  while (cass_iterator_next(iterator)) {
    if (!is_first) printf(", ");
    print_schema_value(cass_iterator_get_value(iterator));
    is_first = cass_false;
  }
  printf(" ]");
  cass_iterator_free(iterator);
}

void print_schema_map(const CassValue* value) {
  CassIterator* iterator = cass_iterator_from_map(value);
  cass_bool_t is_first = cass_true;

  printf("{ ");
  while (cass_iterator_next(iterator)) {
    if (!is_first) printf(", ");
    print_schema_value(cass_iterator_get_map_key(iterator));
    printf(" : ");
    print_schema_value(cass_iterator_get_map_value(iterator));
    is_first = cass_false;
  }
  printf(" }");
  cass_iterator_free(iterator);
}

void print_schema_meta_field(const CassSchemaMetaField* field, int indent) {
  CassString name = cass_schema_meta_field_name(field);
  const CassValue* value = cass_schema_meta_field_value(field);

  print_indent(indent);
  printf("%.*s: ", (int)name.length, name.data);
  print_schema_value(value);
  printf("\n");
}

void print_schema_meta_fields(const CassSchemaMeta* meta, int indent) {
  CassIterator* fields = cass_iterator_fields_from_schema_meta(meta);

  while (cass_iterator_next(fields)) {
    print_schema_meta_field(cass_iterator_get_schema_meta_field(fields), indent);
  }
  cass_iterator_free(fields);
}

void print_schema_meta_entries(const CassSchemaMeta* meta, int indent) {
  CassIterator* entries = cass_iterator_from_schema_meta(meta);

  while (cass_iterator_next(entries)) {
    print_schema_meta(cass_iterator_get_schema_meta(entries), indent);
  }
  cass_iterator_free(entries);
}

void print_schema_meta(const CassSchemaMeta* meta, int indent) {
  const CassSchemaMetaField* field = NULL;
  CassString name;
  print_indent(indent);

  switch (cass_schema_meta_type(meta)) {
    case CASS_SCHEMA_META_TYPE_KEYSPACE:
      field = cass_schema_meta_get_field(meta, "keyspace_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name);
      printf("Keyspace \"%.*s\":\n", (int)name.length, name.data);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      print_schema_meta_entries(meta, indent + 1);
      break;

    case CASS_SCHEMA_META_TYPE_TABLE:
      field = cass_schema_meta_get_field(meta, "columnfamily_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name);
      printf("Table \"%.*s\":\n", (int)name.length, name.data);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      print_schema_meta_entries(meta, indent + 1);
      break;

    case CASS_SCHEMA_META_TYPE_COLUMN:
      field = cass_schema_meta_get_field(meta, "column_name");
      cass_value_get_string(cass_schema_meta_field_value(field), &name);
      printf("Column \"%.*s\":\n", (int)name.length, name.data);
      print_schema_meta_fields(meta, indent + 1);
      printf("\n");
      break;
  }
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_cassObjectObjCct --
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
	casstcl_clientData *ct = (casstcl_clientData *)cData;
	int resultCode = TCL_OK;

    static CONST char *options[] = {
        "select",
        "exec",
        "connect",
		"prepare",
		"list_keyspaces",
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
		"meta",
        NULL
    };

    enum options {
        OPT_SELECT,
        OPT_EXEC,
        OPT_CONNECT,
		OPT_PREPARE,
		OPT_LIST_KEYSPACES,
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
		OPT_CLOSE,
		OPT_META
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

			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "statement");
				return TCL_ERROR;
			}

			query = Tcl_GetString (objv[2]);

			future = cass_session_prepare (ct->session, cass_string_init(query));

			cass_future_wait (future);

			rc = cass_future_error_code (future);
			if (rc != CASS_OK) {
				CassString message = cass_future_error_message(future);

				Tcl_SetStringObj (Tcl_GetObjResult(interp), message.data, message.length);
				resultCode = TCL_ERROR;
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

#if 0
		case OPT_META: {
			if (objc != 3) {
				Tcl_WrongNumArgs (interp, 2, objv, "keyspace");
				return TCL_ERROR;
			}

			char *keyspace = Tcl_GetString (objv[2]);
			print_keyspace(ct->session, keyspace);
			break;
		}
#else
		case OPT_META: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}

			casstcl_meta_walk_schema (ct);
			break;
		}

    }
#endif
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
    casstcl_clientData *ct;
    int                 optIndex;
    char               *commandName;
    int                 autoGeneratedName;

    static CONST char *options[] = {
        "create",
        NULL
    };

    enum options {
        OPT_CREATE
    };

    // basic command line processing
    if (objc != 3) {
        Tcl_WrongNumArgs (interp, 1, objv, "create name");
        return TCL_ERROR;
    }

    // argument must be one of the subOptions defined above
    if (Tcl_GetIndexFromObj (interp, objv[1], options, "option",
        TCL_EXACT, &optIndex) != TCL_OK) {
        return TCL_ERROR;
    }

    // allocate one of our cass client data objects for Tcl and configure it
    ct = (casstcl_clientData *)ckalloc (sizeof (casstcl_clientData));

    ct->cass_magic = CASS_MAGIC;
    ct->interp = interp;
	ct->session = cass_session_new ();
    ct->cluster = cass_cluster_new ();
	ct->ssl = cass_ssl_new ();

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
    return TCL_OK;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
