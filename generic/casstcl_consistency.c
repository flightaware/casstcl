/*
 * casstcl_consistency - Functions for setting and managing cassandra consistency
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_error.h"
#include "casstcl_consistency.h"

/*
 *--------------------------------------------------------------
 *
 * casstcl_setStatementConsistency -- Setup the consistency
 *   level for the specified statement, if necessary.  Special
 *   handling is automatically used for serial consistency
 *   levels.
 *
 * Results:
 *      A standard Tcl result.
 *
 * Side effects:
 *      The statement will be freed upon error.
 *
 *--------------------------------------------------------------
 */
int
casstcl_setStatementConsistency (casstcl_sessionClientData *ct, CassStatement *statementPtr, CassConsistency *consistencyPtr)
{
	if (consistencyPtr != NULL) {
		CassConsistency consistency = *consistencyPtr;
		CassError consistencyErr;
		if (consistency == CASS_CONSISTENCY_SERIAL ||
		    consistency == CASS_CONSISTENCY_LOCAL_SERIAL) {
			consistencyErr = cass_statement_set_serial_consistency(statementPtr, consistency);
		} else {
			consistencyErr = cass_statement_set_consistency(statementPtr, consistency);
		}
		if (consistencyErr != CASS_OK) {
			cass_statement_free(statementPtr);
			return casstcl_cass_error_to_tcl (ct, consistencyErr);
		}
	}
	return TCL_OK;
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
			break;
		}

        case OPT_ONE: {
			*cassConsistency = CASS_CONSISTENCY_ONE;
			break;
		}

        case OPT_TWO: {
			*cassConsistency = CASS_CONSISTENCY_TWO;
			break;
		}

		case OPT_THREE: {
			*cassConsistency = CASS_CONSISTENCY_THREE;
			break;
		}

        case OPT_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_QUORUM;
			break;
		}

        case OPT_ALL: {
			*cassConsistency = CASS_CONSISTENCY_ALL;
			break;
		}

        case OPT_LOCAL_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_QUORUM;
			break;
		}

		case OPT_EACH_QUORUM: {
			*cassConsistency = CASS_CONSISTENCY_EACH_QUORUM;
			break;
		}

		case OPT_SERIAL: {
			*cassConsistency = CASS_CONSISTENCY_SERIAL;
			break;
		}

		case OPT_LOCAL_SERIAL: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_SERIAL;
			break;
		}

		case OPT_LOCAL_ONE: {
			*cassConsistency = CASS_CONSISTENCY_LOCAL_ONE;
			break;
		}
	}

	return TCL_OK;
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_consistency_to_string -- given a CassConsistency,
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
casstcl_cass_consistency_to_string (CassConsistency consistency) {
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

/* vim: set ts=4 sw=4 sts=4 noet : */
