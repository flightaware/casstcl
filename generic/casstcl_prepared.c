/*
 * casstcl_prepared - Functions used to handle, make, and delete prepared objects
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_prepared.h"
#include "casstcl_types.h"
#include "casstcl_consistency.h"

#include <assert.h>

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
	Tcl_DecrRefCount (pcd->tableNameObj);
	ckfree (pcd->string);
    ckfree((char *)clientData);
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_prepared_command_to_preparedClientData -- given a "prepared"
 * command name like prepared0, find it
 *   in the interpreter and return a pointer to its prepared client
 *   data or NULL
 *
 * Results:
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
casstcl_preparedClientData *
casstcl_prepared_command_to_preparedClientData (Tcl_Interp *interp, char *preparedCommandName)
{
	Tcl_CmdInfo preparedCmdInfo;

	if (!Tcl_GetCommandInfo (interp, preparedCommandName, &preparedCmdInfo)) {
		return NULL;
	}

	casstcl_preparedClientData *pcd = (casstcl_preparedClientData *)preparedCmdInfo.objClientData;
    if (pcd->cass_prepared_magic != CASS_PREPARED_MAGIC) {
		return NULL;
	}

	return pcd;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_prepared --
 *
 *   binds names into a prepared statement
 *
 *   takes a prepared statement client data, a Tcl list of key-value
 *   pairs and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   Similar to casstcl_bind_names_from_array and casstcl_bind_names_from_list
 *
 *   Works a little differently because it binds the parameters by name
 *   as specified in the key-value pairs.  This capability is only available
 *   to prepared statements, and makes things a little easier on the
 *   developer.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_prepared (casstcl_preparedClientData *pcd, int objc, Tcl_Obj *CONST objv[], CassConsistency *consistencyPtr, CassStatement **statementPtr)
{
	Tcl_Interp *interp = pcd->ct->interp;
	CassStatement *statement = cass_prepared_bind (pcd->prepared);
	casstcl_sessionClientData *ct = pcd->ct;
	int i;
	int masterReturn = TCL_OK;
	int tclReturn = TCL_OK;
	char *table = Tcl_GetString (pcd->tableNameObj);

	casstcl_cassTypeInfo typeInfo;

	*statementPtr = NULL;

	if (casstcl_setStatementConsistency(ct, statement, consistencyPtr) != TCL_OK) {
		return TCL_ERROR;
	}

//printf("objc = %d\n", objc);
	for (i = 0; i < objc; i += 2) {
// printf("i = %d, objv[i] = '%s', objc = %d\n", i, Tcl_GetString(objv[i]), objc);

		tclReturn = casstcl_typename_obj_to_cass_value_types (interp, table, objv[i], &typeInfo);

		if (tclReturn == TCL_ERROR) {
//printf ("error from casstcl_obj_to_compound_cass_value_types\n");
			masterReturn = TCL_ERROR;
			break;
		}

		// failed to find it?  in this case it's an error
		if (tclReturn == TCL_CONTINUE) {
			// Tcl_ResetResult (interp);
			// Tcl_AppendResult (interp, "couldn't look up data type for column '", Tcl_GetString (objv[i]), "' from table '", table, "'", NULL);
			// masterReturn = TCL_ERROR;
			// break;
			continue;
		}

		// get the value out of the list
		Tcl_Obj *valueObj = objv[i+1];
                int name_length = 0;
		char *name = Tcl_GetStringFromObj (objv[i], &name_length);

// printf("requesting bind by name for '%s', valueType %d\n", name, typeInfo.cassValueTYpe);
		tclReturn = casstcl_bind_tcl_obj (ct, statement, name, name_length, 0, &typeInfo, valueObj);
// printf ("tried to bind arg '%s' as type %d %d %d value '%s'\n", name, typeInfo.cassValueType, typeInfo.valueSubType1, typeInfo.valueSubType2, Tcl_GetString(valueObj));
		if (tclReturn == TCL_ERROR) {
//printf ("error from casstcl_bind_tcl_obj\n");
			Tcl_AppendResult (interp, " while attempting to bind field name of '", name, "' of type '", casstcl_cass_value_type_to_string(typeInfo.cassValueType), "' referencing table '", table, "'", NULL);
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
        "statement",
        "delete",
        NULL
    };

    enum options {
		OPT_STATEMENT,
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
		case OPT_STATEMENT: {
			if (objc != 2) {
				Tcl_WrongNumArgs (interp, 2, objv, "");
				return TCL_ERROR;
			}
			Tcl_SetObjResult (interp, Tcl_NewStringObj (pcd->string, -1));

			break;
		}
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

/* vim: set ts=4 sw=4 sts=4 noet : */
