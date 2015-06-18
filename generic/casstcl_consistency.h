/*
 *
 * Include file for casstcl_consistency
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

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
int casstcl_setStatementConsistency (casstcl_sessionClientData *ct, CassStatement *statementPtr, CassConsistency *consistencyPtr);


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
int casstcl_obj_to_cass_consistency(casstcl_sessionClientData *ct, Tcl_Obj *tclObj, CassConsistency *cassConsistency);

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
const char * casstcl_cass_consistency_to_string (CassConsistency consistency);


/* vim: set ts=4 sw=4 sts=4 noet : */
