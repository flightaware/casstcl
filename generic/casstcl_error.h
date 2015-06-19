/*
 *
 * Include file for casstcl_error
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

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
const char *casstcl_cass_error_to_errorcode_string (CassError cassError);


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
int casstcl_cass_error_to_tcl (casstcl_sessionClientData *ct, CassError cassError);

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
int casstcl_future_error_to_tcl (casstcl_sessionClientData *ct, CassError cassError, CassFuture *future);

/* vim: set ts=4 sw=4 sts=4 noet : */
