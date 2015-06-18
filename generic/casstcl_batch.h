/*
 *
 * Include file for casstcl_batch
 *
 * Copyright (C) 2015 by FlightAware, All Rights Reserved
 *
 * Freely redistributable under the Berkeley copyright, see license.terms
 * for details.
 */

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
int casstcl_obj_to_cass_batch_type (Tcl_Interp *interp, Tcl_Obj *tclObj, CassBatchType *cassBatchType);
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
const char *casstcl_batch_type_to_batch_type_string (CassBatchType cassBatchType);

/*
 *--------------------------------------------------------------
 *
 *   casstcl_batch_command_to_batchClientData -- given a batch command name,
 *   find it in the interpreter and return a pointer to its batch client
 *   data or NULL
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
casstcl_batchClientData * casstcl_batch_command_to_batchClientData (Tcl_Interp *interp, char *batchCommandName);

/*
 *----------------------------------------------------------------------
 *
 * casstcl_createBatchObjectCommand --
 *
 *    given a casstcl_sessionClientData pointer, an object name (or "#auto"),
 *    and a CASS_BATCH_TYPE, create a corresponding batch object command
 *
 * Results:
 *    A standard Tcl result
 *
 *----------------------------------------------------------------------
 */
int casstcl_createBatchObjectCommand (casstcl_sessionClientData *ct, char *commandName, CassBatchType cassBatchType);

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
int casstcl_batchObjectObjCmd(ClientData cData, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[]);

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
void casstcl_batchObjectDelete (ClientData clientData);

/* vim: set ts=4 sw=4 sts=4 noet : */
