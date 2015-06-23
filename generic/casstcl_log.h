/*
 *
 * Include file for casstcl_log
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */


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
int casstcl_obj_to_cass_log_level (Tcl_Interp *interp, Tcl_Obj *tclObj, CassLogLevel *cassLogLevel);


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
const char * casstcl_cass_log_level_to_string (CassLogLevel severity);

/* vim: set ts=4 sw=4 sts=4 noet : */

