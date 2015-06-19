/*
 * casstcl_log - Functions to support custom logging
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_log.h"

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
casstcl_obj_to_cass_log_level (Tcl_Interp *interp, Tcl_Obj *tclObj, CassLogLevel *cassLogLevel) {
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
    if (Tcl_GetIndexFromObj (interp, tclObj, logLevels, "logLevel",
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

/* vim: set ts=4 sw=4 sts=4 noet : */
