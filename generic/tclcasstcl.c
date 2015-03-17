/*
 * casstcl_Init and casstcl_SafeInit
 *
 * Copyright (C) 2015 FlightAware
 *
 * Freely redistributable under the Berkeley copyright.  See license.terms
 * for details.
 */

#include <tcl.h>
#include <tclTomMath.h>
#include "casstcl.h"

#undef TCL_STORAGE_CLASS
#define TCL_STORAGE_CLASS DLLEXPORT


/*
 *----------------------------------------------------------------------
 *
 * Casstcl_Init --
 *
 *	Initialize the casstcl extension.  The string "casstcl"
 *      in the function name must match the PACKAGE declaration at the top of
 *	configure.in.
 *
 * Results:
 *	A standard Tcl result
 *
 * Side effects:
 *	One new command "casstcl" is added to the Tcl interpreter.
 *
 *----------------------------------------------------------------------
 */

EXTERN int
Casstcl_Init(Tcl_Interp *interp)
{
    Tcl_Namespace *namespace;
    /*
     * This may work with 8.0, but we are using strictly stubs here,
     * which requires 8.1.
     */
    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
		return TCL_ERROR;
    }

    if (Tcl_TomMath_InitStubs(interp, "8.5") == NULL) {
		return TCL_ERROR;
    }

    if (Tcl_PkgRequire(interp, "Tcl", "8.1", 0) == NULL) {
		return TCL_ERROR;
    }

    if (Tcl_PkgProvide(interp, PACKAGE_NAME, PACKAGE_VERSION) != TCL_OK) {
		return TCL_ERROR;
    }

	Tcl_RegisterObjType(&casstcl_cassTypeTclType);

    namespace = Tcl_CreateNamespace (interp, "::casstcl", NULL, NULL);

    /* Create the create command  */
    Tcl_CreateObjCommand(interp, "::casstcl::cass", (Tcl_ObjCmdProc *) casstcl_cassObjCmd, (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

    Tcl_Export (interp, namespace, "*", 0);

    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_SafeInit --
 *
 *	Initialize the casstcl in a safe interpreter.
 *
 * Results:
 *	A standard Tcl result
 *
 * Side effects:
 *	One new command "casstcl" is added to the Tcl interpreter.
 *
 *----------------------------------------------------------------------
 */

EXTERN int
Casstcl_SafeInit(Tcl_Interp *interp)
{
    /*
     * can this work safely?  seems too dangerous...
     */
    return TCL_ERROR;
}

/* vim: set ts=4 sw=4 sts=4 noet : */
