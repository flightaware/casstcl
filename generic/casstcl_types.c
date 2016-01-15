/*
 * casstcl_types - Functions for converting between cassandra and casstcl types
 *
 * casstcl - Tcl interface to CassDB
 *
 * Copyright (C) 2014 FlightAware LLC
 *
 * freely redistributable under the Berkeley license
 */

#include "casstcl.h"
#include "casstcl_types.h"
#include "casstcl_error.h"
#include "casstcl_consistency.h"

#include <assert.h>

// Tcl type definition for caching CassValueType

// we don't have to free any internal representation because our representation
// fits into the exist Tcl_Obj definition without a pointer to anything else

// we never invalidate the string representation so we can set the
// UpdateStringOf... function pointer to null

void DupCassTypeTypeInternalRep (Tcl_Obj *srcPtr, Tcl_Obj *copyPtr);
int SetCassTypeTypeFromAny (Tcl_Interp *interp, Tcl_Obj *obj);
void UpdateCassTypeString (Tcl_Obj *obj);

// copy the internal representation of a cassTypeTclType Tcl object
// to a new Tcl object
void DupCassTypeTypeInternalRep (Tcl_Obj *srcPtr, Tcl_Obj *copyPtr);

// convert any tcl object to be a cassTypeTclType
int SetCassTypeTypeFromAny (Tcl_Interp *interp, Tcl_Obj *obj);

// this converts our internal data type to a string
// it is probably not needed and will not be unless you some day
// write a casstcl_cassTypeInfo into a Tcl object that you didn't
// create with a string or copy to (like for the int data type an object
// that is the target of a calculation will get its int set and its
// string rep invalidated and regenerated later
//
// this also probably means that the routine is buggy because it probably
// hasn't ever been called
//
void UpdateCassTypeString (Tcl_Obj *obj);



// Tcl object type definition for the internal representation of a
// cassTypeTclType.  this allows us to cache the lookup of a type
// like "int" to CASS_VALUE_TYPE_INT or
// "map int text" to
// CASS_VALUE_TYPE_MAP CASS_VALUE_TYPE_INT CASS_VALUE_TYPE_TEXT
//
Tcl_ObjType casstcl_cassTypeTclType = {
  "CassType",
  NULL,
  DupCassTypeTypeInternalRep,
  UpdateCassTypeString,
  SetCassTypeTypeFromAny
};


/*
 *--------------------------------------------------------------
 *
 * casstcl_cass_value_type_to_string -- given a CassValueType,
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
casstcl_cass_value_type_to_string (CassValueType valueType) {
  switch (valueType) {
    case CASS_VALUE_TYPE_UNKNOWN: {
      return "unknown";
    }

    case CASS_VALUE_TYPE_CUSTOM: {
      return "custom";
    }

    case CASS_VALUE_TYPE_ASCII: {
      return "ascii";
    }

    case CASS_VALUE_TYPE_BIGINT: {
      return "bigint";
    }

    case CASS_VALUE_TYPE_BLOB: {
      return "blob";
    }

    case CASS_VALUE_TYPE_BOOLEAN: {
      return "boolean";
    }

    case CASS_VALUE_TYPE_COUNTER: {
      return "counter";
    }

    case CASS_VALUE_TYPE_DECIMAL: {
      return "decimal";
    }

    case CASS_VALUE_TYPE_DOUBLE: {
      return "double";
    }

    case CASS_VALUE_TYPE_FLOAT: {
      return "float";
    }

    case CASS_VALUE_TYPE_INT: {
      return "int";
    }

    case CASS_VALUE_TYPE_TEXT: {
      return "text";
    }

    case CASS_VALUE_TYPE_TIMESTAMP: {
      return "timestamp";
    }

    case CASS_VALUE_TYPE_UUID: {
      return "uuid";
    }

    case CASS_VALUE_TYPE_VARCHAR: {
      return "varchar";
    }

    case CASS_VALUE_TYPE_VARINT: {
      return "varint";
    }

    case CASS_VALUE_TYPE_TIMEUUID: {
      return "timeuuid";
    }

    case CASS_VALUE_TYPE_INET: {
      return "inet";
    }

    case CASS_VALUE_TYPE_LIST: {
      return "list";
    }

    case CASS_VALUE_TYPE_MAP: {
      return "map";
    }

    case CASS_VALUE_TYPE_SET: {
      return "set";
    }

    case CASS_VALUE_TYPE_UDT: {
      return "udt";
    }

    case CASS_VALUE_TYPE_TUPLE: {
      return "tuple";
    }

    default:
      return "unknown";
  }
}



/*
 *--------------------------------------------------------------
 *
 * casstcl_string_to_cass_value_type -- lookup a string
 *   to be one of the cass value type strings for CassValueType and set
 *   a pointer to a passed-in CassValueType value to the corresponding
 *   type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Results:
 *      ...cass value type gets returned
 *      CASS_VALUE_TYPE_UNKNOWN is returned if nothing matches
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
CassValueType
casstcl_string_to_cass_value_type (char *string) {
  switch (*string) {
    case 'a': {
      if (strcmp (string, "ascii") == 0) return CASS_VALUE_TYPE_ASCII;
      break;
    }

    case 'b': {
      if (strcmp (string, "bigint") == 0) return CASS_VALUE_TYPE_BIGINT;
      if (strcmp (string, "blob") == 0) return CASS_VALUE_TYPE_BLOB;
      if (strcmp (string, "boolean") == 0) return CASS_VALUE_TYPE_BOOLEAN;
      break;
    }

    case 'c': {
      if (strcmp (string, "counter") == 0) return CASS_VALUE_TYPE_COUNTER;
      if (strcmp (string, "custom") == 0) return CASS_VALUE_TYPE_CUSTOM;
      break;
    }

    case 'd': {
      if (strcmp (string, "decimal") == 0) return CASS_VALUE_TYPE_DECIMAL;
      if (strcmp (string, "double") == 0) return CASS_VALUE_TYPE_DOUBLE;
      break;
    }

    case 'f': {
      if (strcmp (string, "float") == 0) return CASS_VALUE_TYPE_FLOAT;
      break;
    }

    case 'i': {
      if (strcmp (string, "int") == 0) return CASS_VALUE_TYPE_INT;
      if (strcmp (string, "inet") == 0) return CASS_VALUE_TYPE_INET;
      break;
    }

    case 'l': {
      if (strcmp (string, "list") == 0) return CASS_VALUE_TYPE_LIST;
      break;
    }

    case 'm': {
      if (strcmp (string, "map") == 0) return CASS_VALUE_TYPE_MAP;
      break;
    }

    case 's': {
      if (strcmp (string, "set") == 0) return CASS_VALUE_TYPE_SET;
      break;
    }

    case 't': {
      if (strcmp (string, "text") == 0) return CASS_VALUE_TYPE_TEXT;
      if (strcmp (string, "timestamp") == 0) return CASS_VALUE_TYPE_TIMESTAMP;
      if (strcmp (string, "tuple") == 0) return CASS_VALUE_TYPE_TUPLE;
      if (strcmp (string, "timeuuid") == 0) return CASS_VALUE_TYPE_TIMEUUID;
      break;
    }

    case 'u': {
      if (strcmp (string, "udt") == 0) return CASS_VALUE_TYPE_UDT;
      if (strcmp (string, "uuid") == 0) return CASS_VALUE_TYPE_UUID;
      if (strcmp (string, "unknown") == 0) return CASS_VALUE_TYPE_UNKNOWN;
      break;
    }

    case 'v': {
      if (strcmp (string, "varchar") == 0) return CASS_VALUE_TYPE_VARCHAR;
      if (strcmp (string, "varint") == 0) return CASS_VALUE_TYPE_VARINT;
      break;
    }
  }

  return CASS_VALUE_TYPE_UNKNOWN;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_InitCassBytesFromBignum --
 *
 *  Allocate and initialize a CassBytes from a 'bignum'.
 *
 * Results:
 *  A standard Tcl result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_InitCassBytesFromBignum(
    Tcl_Interp *interp,   /* Used for error reporting if not NULL. */
    CassBytes *v,   /* CassBytes to initialize */
    mp_int *a)      /* Initial value */
{
    unsigned char *data;
    unsigned long outlen;
    int status;

    outlen = TclBN_mp_unsigned_bin_size(a);
    data = (cass_byte_t *) ckalloc(outlen);

    status = TclBN_mp_to_unsigned_bin_n(a, data, &outlen);

    if (status != MP_OKAY) {
  if (interp != NULL) {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "could not init bytes", NULL);
  }
  ckfree((char *)data);
  return TCL_ERROR;
    }

    v->data = data;
    v->size = outlen;
    return TCL_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_InitBignumFromCassBytes --
 *
 *  Allocate and initialize a 'bignum' from a CassBytes.
 *
 * Results:
* A standard Tcl result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_InitBignumFromCassBytes(
    Tcl_Interp *interp,   /* Used for error reporting if not NULL. */
    mp_int *a,      /* Bignum to initialize */
    CassBytes *v)   /* Initial value */
{
    int status = TclBN_mp_init(a);

    if (status != MP_OKAY) {
  if (interp != NULL) {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "could not init bignum", NULL);
  }
  return TCL_ERROR;
    }

    status = mp_read_unsigned_bin(a, v->data, v->size);

    if (status != MP_OKAY) {
  if (interp != NULL) {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "could not read bignum", NULL);
  }
  return TCL_ERROR;
    }

    return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_GetTimestampFromObj --
 *
 *  Accepts a Tcl object value that specifies a whole number of
 *  seconds and optionally a fractional number of seconds, and
 *  converts the value to the whole number of milliseconds.
 *
 * Results:
 *  A standard Tcl result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_GetTimestampFromObj(
    Tcl_Interp *interp,   /* Used for error reporting if not NULL. */
    Tcl_Obj *objPtr,    /* Object from which to get milliseconds. */
    cass_int64_t *milliseconds) /* Place to store whole milliseconds. */
{
  Tcl_WideInt wideVal;
  if (Tcl_GetWideIntFromObj(NULL, objPtr, &wideVal) == TCL_OK) {
    if (wideVal < CASS_TIMESTAMP_LOWER_LIMIT ||
            wideVal > CASS_TIMESTAMP_UPPER_LIMIT) {
      if (interp != NULL) {
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "whole seconds cannot exceed ",
            STRINGIFY(CASS_TIMESTAMP_UPPER_LIMIT), NULL);
      }
      return TCL_ERROR;
    } else {
      *milliseconds = wideVal * 1000; /* SAFE: See 'if' above. */
    }
  } else {
    double doubleVal;
    if (Tcl_GetDoubleFromObj(interp, objPtr, &doubleVal) == TCL_OK) {
      if ((Tcl_WideInt)doubleVal < CASS_TIMESTAMP_LOWER_LIMIT ||
              (Tcl_WideInt)doubleVal > CASS_TIMESTAMP_UPPER_LIMIT) {
        if (interp != NULL) {
          Tcl_ResetResult(interp);
          Tcl_AppendResult(interp, "whole seconds cannot exceed ",
              STRINGIFY(CASS_TIMESTAMP_UPPER_LIMIT), NULL);
        }
        return TCL_ERROR;
      } else {
        wideVal = (Tcl_WideInt)doubleVal; /* SAFE: See 'if' above. */
        doubleVal -= (double)wideVal;
        doubleVal *= 1000.0;
        *milliseconds = (wideVal * 1000) + (Tcl_WideInt)doubleVal;
      }
    } else {
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_NewTimestampObj --
 *
 *  Accepts a Cassandra 'timestamp' value, in milliseconds, and
 *  creates a Tcl object based on it.  If the milliseconds is
 *  evenly divisible by 1000, a Tcl wide integer object will be
 *  returned, containing the exact number of seconds.  Otherwise,
 *  a Tcl double object will be returned with an approximate value,
 *  where the fractional portion of the double will represent the
 *  milliseconds and the whole portion will represent the number
 *  of seconds.
 *
 * Results:
 *  The newly created Tcl object, having a reference count of zero.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

Tcl_Obj *casstcl_NewTimestampObj(
  cass_int64_t milliseconds
){
  if((milliseconds % 1000) == 0) {
    return Tcl_NewWideIntObj(milliseconds / 1000);
  } else {
    return Tcl_NewDoubleObj((double)milliseconds / 1000.0);
  }
}

/*
 *----------------------------------------------------------------------
 *
 * mp_read_unsigned_bin --
 *
 *  Read a binary encoded 'bignum' from the specified buffer.  It
 *  must have been initialized first.  This routine was borrowed
 *  directly from the Tcl 8.6 source code (i.e. because we needed
 *  it and it was not available as an export).
 *
 * Results:
 *  A standard LibTomMath result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int mp_read_unsigned_bin (mp_int * a, const unsigned char *b, int c)
{
  int     res;

  /* make sure there are at least two digits */
  if (a->alloc < 2) {
     if ((res = TclBN_mp_grow(a, 2)) != MP_OKAY) {
        return res;
     }
  }

  /* zero the int */
  TclBN_mp_zero (a);

  /* read the bytes in */
  while (c-- > 0) {
    if ((res = TclBN_mp_mul_2d (a, 8, a)) != MP_OKAY) {
      return res;
    }

#ifndef MP_8BIT
      a->dp[0] |= *b++;
      a->used += 1;
#else
      a->dp[0] = (*b & MP_MASK);
      a->dp[1] |= ((*b++ >> 7U) & 1);
      a->used += 2;
#endif
  }
  TclBN_mp_clamp (a);
  return MP_OKAY;
}



/*
 *----------------------------------------------------------------------
 *
 * casstcl_typename_obj_to_cass_value_types --
 *
 *   Look up the validator in the column type map and decode it into
 *   three CassValueType entries, with extremely fast caching
 *
 * Results:
 *      A standard Tcl result.
 *
 *      TCL_CONTINUE is returned if the type index name isn't found.
 *      Also in that case, the value types are set to CASS_VALUE_TYPE_UNKNOWN.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_typename_obj_to_cass_value_types (Tcl_Interp *interp, char *table, Tcl_Obj *typenameObj, casstcl_cassTypeInfo *typeInfoPtr) {
  int varNameSize = 0;
  char *varName = Tcl_GetStringFromObj (typenameObj, &varNameSize);
  // add two bytes, one for a period and one for a null byte
  int typeIndexSize = strlen (table) + 2 + varNameSize;
  char *typeIndexName = ckalloc (typeIndexSize);

  snprintf (typeIndexName, typeIndexSize, "%s.%s", table, varName);

  Tcl_Obj *typeObj = Tcl_GetVar2Ex (interp, "::casstcl::columnTypeMap", typeIndexName, (TCL_GLOBAL_ONLY|TCL_LEAVE_ERR_MSG));

  ckfree (typeIndexName);

  // if not found, the type didn't exist, but it might not be an error,
  // return TCL_CONTINUE to differentiate it from TCL_OK
  if (typeObj == NULL) {
    typeInfoPtr->cassValueType = CASS_VALUE_TYPE_UNKNOWN;
    typeInfoPtr->valueSubType1 = CASS_VALUE_TYPE_UNKNOWN;
    typeInfoPtr->valueSubType2 = CASS_VALUE_TYPE_UNKNOWN;
    return TCL_CONTINUE;
  }

  if (Tcl_ConvertToType (interp, typeObj, &casstcl_cassTypeTclType) == TCL_ERROR) {
    return TCL_ERROR;
  }

  casstcl_cassTypeInfo *typeInfo = (casstcl_cassTypeInfo *)&typeObj->internalRep.otherValuePtr;
  *typeInfoPtr = *typeInfo; // structure copy

// printf("casstcl_typename_obj_to_cass_value_types took table '%s' type '%s' and produced %x, %x, %x\n", table, Tcl_GetString (typenameObj), *valueType, *valueSubType1, *valueSubType2);
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
 *      This is a vital routine to the entire edifice.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_cass_value_to_tcl_obj (casstcl_sessionClientData *ct, const CassValue *cassValue, Tcl_Obj **tclObj)
{
  char msg[60];
  CassValueType valueType = cass_value_type (cassValue);
  Tcl_Interp *interp = ct->interp;

  switch (valueType) {

    case CASS_VALUE_TYPE_UNKNOWN: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for get operation 'unknown'", NULL);

      *tclObj = NULL;
      return TCL_ERROR;
    }

    case CASS_VALUE_TYPE_CUSTOM: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for get operation 'custom'", NULL);

      *tclObj = NULL;
      return TCL_ERROR;
    }

    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT: {
      CassString value;

      cass_value_get_string(cassValue, &value.data, &value.length);
      *tclObj = Tcl_NewStringObj (value.data, value.length);
      return TCL_OK;
    }

    case CASS_VALUE_TYPE_TIMESTAMP: {
      cass_int64_t cassInt;
      CassError cassError;

      cassError = cass_value_get_int64 (cassValue, &cassInt);

      if (cassError != CASS_OK) {
        return casstcl_cass_error_to_tcl (ct, cassError);
      }

      *tclObj = casstcl_NewTimestampObj (cassInt);
      return TCL_OK;
    }

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

      cass_value_get_bytes(cassValue, &bytes.data, &bytes.size);
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
      mp_int mpVal;
      CassDecimal cassDecimal;
      CassError cassError;
      Tcl_Obj *listObjv[2];

      cassError = cass_value_get_decimal(cassValue, &cassDecimal.varint.data, &cassDecimal.varint.size, &cassDecimal.scale);

      if (cassError != CASS_OK) {
        return casstcl_cass_error_to_tcl (ct, cassError);
      }

      if (casstcl_InitBignumFromCassBytes(interp, &mpVal, &cassDecimal.varint) != TCL_OK) {
        return TCL_ERROR;
      }

      listObjv[0] = Tcl_NewIntObj(cassDecimal.scale);
      listObjv[1] = Tcl_NewBignumObj(&mpVal);

      *tclObj = Tcl_NewListObj(2, listObjv);
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

    case CASS_VALUE_TYPE_TINY_INT:
    case CASS_VALUE_TYPE_SMALL_INT:
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

    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_TIMEUUID: {
      CassUuid key;
      CassError cassError;
      char key_str[CASS_UUID_STRING_LENGTH];

      cassError = cass_value_get_uuid(cassValue, &key);

      if (cassError != CASS_OK) {
        return casstcl_cass_error_to_tcl (ct, cassError);
      }

      cass_uuid_string(key, key_str);
      *tclObj = Tcl_NewStringObj (key_str, CASS_UUID_STRING_LENGTH - 1);
      return TCL_OK;
    }

    case CASS_VALUE_TYPE_VARINT: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for get operation 'varint'", NULL);

      *tclObj = NULL;
      return TCL_ERROR;
    }

	case CASS_VALUE_TYPE_UDT: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "udt value type not yet implemented by casstcl", NULL);

      *tclObj = NULL;
      return TCL_ERROR;
	}

	case CASS_VALUE_TYPE_TUPLE: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "type value type not yet implemented by casstcl", NULL);

      *tclObj = NULL;
      return TCL_ERROR;
	}

	case CASS_VALUE_TYPE_DATE:
	case CASS_VALUE_TYPE_TIME:
	case CASS_VALUE_TYPE_LAST_ENTRY: {
		// this isn't a real value type and shouldn't ever get picked but this instance
		// silences a compiler warning
		assert (0 == 1);
	}

    case CASS_VALUE_TYPE_INET: {
      CassError cassError;
      CassInet cassInet;
      char addrBuf[INET6_ADDRSTRLEN];
      int isIpV6;

      assert(INET6_ADDRSTRLEN >= INET_ADDRSTRLEN);
      cassError = cass_value_get_inet (cassValue, &cassInet);

      if (cassError != CASS_OK) {
        return casstcl_cass_error_to_tcl (ct, cassError);
      }

      isIpV6 = (cassInet.address_length == CASS_INET_V6_LENGTH);
      memset(addrBuf, 0, INET6_ADDRSTRLEN);
      inet_ntop(isIpV6 ? AF_INET6 : AF_INET, cassInet.address, addrBuf, INET6_ADDRSTRLEN);

      *tclObj = Tcl_NewStringObj(addrBuf, -1);
      return TCL_OK;
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
          Tcl_ListObjAppendElement (interp, listObj, mapKey);
          Tcl_ListObjAppendElement (interp, listObj, mapValue);
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
          Tcl_ListObjAppendElement (interp, listObj, collectionValue);
        }
      }

      cass_iterator_free (iterator);
      *tclObj = listObj;
      return TCL_OK;
    }
  }

  sprintf(msg, "%X", valueType);
  Tcl_ResetResult(interp);
  Tcl_AppendResult(interp, "unrecognized value type for get operation 0x", msg, NULL);

  *tclObj = NULL;
  return TCL_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_GetInetFromObj --
 *
 *  Attempt to return an Inet from the Tcl object "objPtr".
 *
 * Results:
 *  A standard Tcl result.
 *
 * Side effects:
 *  None.
 *
 *----------------------------------------------------------------------
 */

int
casstcl_GetInetFromObj(
    Tcl_Interp *interp, /* Used for error reporting if not NULL. */
    Tcl_Obj *objPtr,  /* The object from which to get an Inet. */
    CassInet *inetPtr)  /* Place to store resulting Inet. */
{
  const char *value = Tcl_GetString(objPtr);
  struct addrinfo hints;
  struct addrinfo *result = NULL;
  int rc;

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_flags = AI_NUMERICHOST;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  rc = getaddrinfo(value, NULL, &hints, &result);

  if (rc == 0) {
#if 0
    casstcl_DumpAddrInfo(stdout, result, 0);
#endif
    assert(result != NULL);
    assert(result->ai_addrlen >= 0);
    assert(result->ai_addrlen <= CASS_INET_V6_LENGTH);
    memset(inetPtr, 0, sizeof(CassInet));
    if (result->ai_family == AF_INET) {
      struct sockaddr_in *pSockAddr = (struct sockaddr_in *)result->ai_addr;
      *inetPtr = cass_inet_init_v4((const cass_uint8_t *)&pSockAddr->sin_addr.s_addr);
      freeaddrinfo(result);
      return TCL_OK;
    } else if (result->ai_family == AF_INET6) {
      struct sockaddr_in6 *pSockAddr = (struct sockaddr_in6 *)result->ai_addr;
      *inetPtr = cass_inet_init_v6((const cass_uint8_t *)&pSockAddr->sin6_addr.s6_addr);
      freeaddrinfo(result);
      return TCL_OK;
    } else {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "address \"", value, "\" is not IPv4 or IPv6", NULL);
      freeaddrinfo(result);
      return TCL_ERROR;
    }
  } else {
    Tcl_ResetResult(interp);
    Tcl_AppendResult(interp, gai_strerror(rc), NULL);
    return TCL_ERROR;
  }
}

/*
 *--------------------------------------------------------------
 *
 * casstcl_obj_to_compound_cass_value_types
 *
 * Lookup a string from a Tcl object and identify it as one of the cass
 * value type strings for CassValueType (int, text uuid, etc.) and set
 * a pointer to a passed-in CassValueType value to the corresponding
 * type such as CASS_VALUE_TYPE_DOUBLE, etc
 *
 * Also if it is a list of "map type type" or "set type" or "list type"
 * then set the valueSubType1 to type defined by the set or list and
 * in the case of a map set valueSubType1 for the key datatype and
 * valueSubType2 for the value datatype
 *
 * Results:
 *      ...cass value types gets set
 *      ...a standard Tcl result is returned
 *
 * Side effects:
 *      None.
 *
 *--------------------------------------------------------------
 */
int
casstcl_obj_to_compound_cass_value_types (Tcl_Interp *interp, Tcl_Obj *tclObj, casstcl_cassTypeInfo *typeInfo) {
  int listObjc;
  Tcl_Obj **listObjv;

  typeInfo->cassValueType = CASS_VALUE_TYPE_UNKNOWN;
  typeInfo->valueSubType1 = CASS_VALUE_TYPE_UNKNOWN;
  typeInfo->valueSubType2 = CASS_VALUE_TYPE_UNKNOWN;

  // try straight lookup.  this should get everything except for
  // maps, sets and lists
  char *string = Tcl_GetString (tclObj);
  CassValueType valueType = casstcl_string_to_cass_value_type (string);

  if (valueType != CASS_VALUE_TYPE_UNKNOWN) {
    typeInfo->cassValueType = valueType;
    return TCL_OK;
  }

  if (Tcl_ListObjGetElements (interp, tclObj, &listObjc, &listObjv) == TCL_ERROR) {
    Tcl_AppendResult (interp, " while parsing cassandra data type", NULL);
    return TCL_ERROR;
  }

  // the list parsed, now look up the first element, if we don't find it
  // in the type list, we have a bad tyupe
  string = Tcl_GetString (listObjv[0]);
  valueType = casstcl_string_to_cass_value_type (string);

  if ((valueType != CASS_VALUE_TYPE_MAP) && (valueType != CASS_VALUE_TYPE_SET) && (valueType != CASS_VALUE_TYPE_LIST)) {
    Tcl_ResetResult (interp);
    Tcl_AppendResult (interp, "cassandra type spec '", string, "' is invalid", NULL);
    return TCL_ERROR;
  }

  // it's a collection, so it will have one or two sub values depending
  // on if it's a list or set (1) or a map (2).
  // anyway, set the first type to the map, list or set type that we
  // figured out.
  typeInfo->cassValueType = valueType;

  if (valueType == CASS_VALUE_TYPE_MAP) {
    if (listObjc != 3) {
      Tcl_ResetResult (interp);
      Tcl_AppendResult (interp, "cassandra map type must contain three type values", NULL);
      return TCL_ERROR;
    }
  } else {
    if (listObjc != 2) {
      Tcl_ResetResult (interp);
      Tcl_AppendResult (interp, "cassandra ", string, " type ('", Tcl_GetString (tclObj), "') must contain two values", NULL);
      return TCL_ERROR;
    }
  }

  // at this point it's a colleciton and the list count is correct so
  // there is at least one subType that has to be looked up successfully

  typeInfo->valueSubType1 = casstcl_string_to_cass_value_type (Tcl_GetString(listObjv[1]));
  if (typeInfo->valueSubType1 == CASS_VALUE_TYPE_UNKNOWN) {
    Tcl_ResetResult (interp);
    Tcl_AppendResult (interp, "cassandra ", string, " type spec unrecognized subtype '", Tcl_GetString (listObjv[1]), "'", NULL);
    return TCL_ERROR;
  }

  // only for maps there is a second subType to be checked, converted
  if (valueType == CASS_VALUE_TYPE_MAP) {
    typeInfo->valueSubType2 = casstcl_string_to_cass_value_type (Tcl_GetString(listObjv[2]));
    if (typeInfo->valueSubType2 == CASS_VALUE_TYPE_UNKNOWN) {
      Tcl_ResetResult (interp);
      Tcl_AppendResult (interp, "cassandra map type spec unrecognized second subtype '", Tcl_GetString(listObjv[2]), "'", NULL);
      return TCL_ERROR;
    }
  }
// printf("casstcl_obj_to_compound_cass_value_types took '%s' and made %d, %d, %d\n", Tcl_GetString (tclObj), typeInfo->cassValueType, typeInfo->valueSubType1, typeInfo->valueSubType2);
  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_array --
 *
 *   Now this little ditty takes an array name and a query and an objv
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list of column names
 *
 *   It fetches the data type of the column from the column-datatype cache
 *
 *   It fetches the value from the array and converts it and binds it
 *   to the statement
 *
 *   This requires that the table name and keyspace be known
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_array (casstcl_sessionClientData *ct, char *table, char *query, char *tclArray, int objc, Tcl_Obj *CONST objv[], CassConsistency *consistencyPtr, CassStatement **statementPtr)
{
  int i;
  int masterReturn = TCL_OK;
  int tclReturn = TCL_OK;
  Tcl_Interp *interp = ct->interp;

  casstcl_cassTypeInfo typeInfo;

  *statementPtr = NULL;

  CassStatement *statement = cass_statement_new(query, objc);

  if (casstcl_setStatementConsistency(ct, statement, consistencyPtr) != TCL_OK) {
    return TCL_ERROR;
  }

  for (i = 0; i < objc; i ++) {
    tclReturn = casstcl_typename_obj_to_cass_value_types (interp, table, objv[i], &typeInfo);

    if (tclReturn == TCL_ERROR) {
      masterReturn = TCL_ERROR;
      break;
    }

    // if the index name wasn't found in the row we get TCL_CONTINUE back.
    // NB we can either treat it as an error or not
    // NB eventually we can accumulate unrecognized types into a map
    if (tclReturn == TCL_CONTINUE) {
      tclReturn = TCL_OK;
      continue;
    }

    // get the value out of the array
    char *varName = Tcl_GetString (objv[i]);
    Tcl_Obj *valueObj = Tcl_GetVar2Ex (interp, tclArray, varName, (TCL_GLOBAL_ONLY|TCL_LEAVE_ERR_MSG));

    if (valueObj == NULL) {
      Tcl_AppendResult (interp, " while trying to look up the data value for column '", varName, "', ", table, "', ", table, "' from array '", tclArray, "'", NULL);
      masterReturn = TCL_ERROR;
      break;
    }

    tclReturn = casstcl_bind_tcl_obj (ct, statement, NULL, 0, i, &typeInfo, valueObj);
// printf ("bound arg %d as %d %d %d value '%s'\n", i, valueType, valueSubType1, valueSubType2, Tcl_GetString(valueObj));
    if (tclReturn == TCL_ERROR) {
      masterReturn = TCL_ERROR;
      break;
    }
  }

  if (masterReturn == TCL_OK) {
    *statementPtr = statement;
  }

  return masterReturn;
}


/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_names_from_list --
 *
 *   fully qualified table name
 *   name of the table
 *   and a pointer to a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   Similar to casstcl_bind_names_from_array
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_names_from_list (casstcl_sessionClientData *ct, char *table, char *query, int objc, Tcl_Obj *CONST objv[], CassConsistency *consistencyPtr, CassStatement **statementPtr)
{
  int i;
  int masterReturn = TCL_OK;
  int tclReturn = TCL_OK;
  Tcl_Interp *interp = ct->interp;

  casstcl_cassTypeInfo typeInfo;

  *statementPtr = NULL;

  CassStatement *statement = cass_statement_new(query, objc / 2);

  if (casstcl_setStatementConsistency(ct, statement, consistencyPtr) != TCL_OK) {
    return TCL_ERROR;
  }

  for (i = 0; i < objc; i += 2) {
    tclReturn = casstcl_typename_obj_to_cass_value_types (interp, table, objv[i], &typeInfo);

    if (tclReturn == TCL_ERROR) {
      masterReturn = TCL_ERROR;
      break;
    }

    // failed to find it?
    if (tclReturn == TCL_CONTINUE) {
      tclReturn = TCL_OK;
      continue;
    }

    // get the value out of the list
    Tcl_Obj *valueObj = objv[i+1];


    tclReturn = casstcl_bind_tcl_obj (ct, statement, NULL, 0, i / 2, &typeInfo, valueObj);
    if (tclReturn == TCL_ERROR) {
      Tcl_AppendResult (interp, " while attempting to bind field '", Tcl_GetString (objv[i]), "' of type '", casstcl_cass_value_type_to_string (typeInfo.cassValueType), "', value '", Tcl_GetString (valueObj), "' referencing table '", table, "'", NULL);
      masterReturn = TCL_ERROR;
      break;
    }
  }

  if (masterReturn == TCL_OK) {
    *statementPtr = statement;
  }

  return masterReturn;
}

// copy the internal representation of a cassTypeTclType Tcl object
// to a new Tcl object
void
DupCassTypeTypeInternalRep (Tcl_Obj *srcPtr, Tcl_Obj *copyPtr)
{
  // not much to this... since we use the wide int representation,
  // all we have to do is copy the wide into from the source to the copy
  copyPtr->internalRep.otherValuePtr = srcPtr->internalRep.otherValuePtr;
  copyPtr->typePtr = &casstcl_cassTypeTclType;
}

// convert any tcl object to be a cassTypeTclType
int
SetCassTypeTypeFromAny (Tcl_Interp *interp, Tcl_Obj *obj)
{
  casstcl_cassTypeInfo *typeInfo = (casstcl_cassTypeInfo *)&obj->internalRep.otherValuePtr;
  casstcl_cassTypeInfo localTypeInfo;

  // convert it using our handy routine for doing that
  // if we get TCL_ERROR, it's an error
  // if we get TCL_OK we need to set the object's type pointer to point
  // to our custom type.
  //
  // after this if the object isn't altered future calls to
  // Tcl_ConvertToType will not do anything and access to the
  // internal representation will be quick
  //
  // see casstcl_typename_obj_to_cass_value_types
  //
  if (casstcl_obj_to_compound_cass_value_types (interp, obj, &localTypeInfo) == TCL_OK) {
    *typeInfo = localTypeInfo; // structure copy
    // we manage the data type of this object now
    obj->typePtr = &casstcl_cassTypeTclType;
    return TCL_OK;
  }
  return TCL_ERROR;
}

// this converts our internal data type to a string
// it is probably not needed and will not be unless you some day
// write a casstcl_cassTypeInfo into a Tcl object that you didn't
// create with a string or copy to (like for the int data type an object
// that is the target of a calculation will get its int set and its
// string rep invalidated and regenerated later
//
// this also probably means that the routine is buggy because it probably
// hasn't ever been called
//
void UpdateCassTypeString (Tcl_Obj *obj) {
  casstcl_cassTypeInfo *typeInfo = (casstcl_cassTypeInfo *)&obj->internalRep.otherValuePtr;
  CassValueType cassType = typeInfo->cassValueType;
  const char *string = casstcl_cass_value_type_to_string (cassType);
  int len = strlen(string);

  if (cassType != CASS_VALUE_TYPE_MAP && cassType != CASS_VALUE_TYPE_SET && cassType != CASS_VALUE_TYPE_LIST) {
    char *newString = ckalloc (len + 1);
    strncpy (newString, string, len);
    obj->bytes = newString;
    obj->length = len;
    return;
  }

  // it's set, map or list, decode the second type
  const char *subString1 = casstcl_cass_value_type_to_string (typeInfo->valueSubType1);
  int len1 = strlen(subString1);

  if (cassType != CASS_VALUE_TYPE_MAP) {
    int newStringSize = len + 1 + len1 + 1;
    char *newString = ckalloc (newStringSize);
    strncpy (newString, string, len);
    newString[len] = ' ';
    strncpy (&newString[len+1], subString1, len1);

    obj->bytes = newString;
    obj->length = newStringSize - 1;
    return;
  }

  const char *subString2 = casstcl_cass_value_type_to_string (typeInfo->valueSubType2);
  int len2 = strlen(subString2);
  int newStringSize = len + 1 + len1 + 1 + len2 + 1;
  char *newString = ckalloc (newStringSize);
  strncpy (newString, string, len);
  newString[len] = ' ';
  strncpy (&newString[len+1], subString1, len1);
  newString[len+1+len1] = ' ';
  strncpy (&newString[len+1+len1+1], subString2, len2);

  obj->bytes = newString;
  obj->length = newStringSize - 1;


  // ok it's a map, a little more complicated because three strings
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_append_tcl_obj_to_collection
 *
 * Convert a Tcl object to a cassandra value of the specified type and
 * append it to the specified collection
 *
 * This is used for constructing cassandra maps, sets and lists.
 *
 * You create a set or a list by appending elements to it.
 *
 * You create a map by appending successions of key elements and value
 * elements to it.
 *
 * They have a specified datatype for sets and lists; for keys there is
 * one for the key and one for the value so for instance the keys can
 * be integers and the values can be strings or whatever.
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */
int casstcl_append_tcl_obj_to_collection (casstcl_sessionClientData *ct, CassCollection *collection, CassValueType valueType, Tcl_Obj *obj) {
  CassError cassError = CASS_OK;
  Tcl_Interp *interp = ct->interp;

  switch (valueType) {
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR: {
      int length = 0;
      char *value = Tcl_GetStringFromObj (obj, &length);

      cassError = cass_collection_append_string_n (collection, value, length);
      break;
    }

    case CASS_VALUE_TYPE_VARINT: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for append operation 'varint'", NULL);
      return TCL_ERROR;
    }

    case CASS_VALUE_TYPE_BLOB: {
      int length = 0;
      unsigned char *value = Tcl_GetByteArrayFromObj (obj, &length);

      cassError = cass_collection_append_bytes (collection, value, length);
      break;
    }

    case CASS_VALUE_TYPE_BOOLEAN: {
      int value = 0;

      if (Tcl_GetBooleanFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting boolean element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_bool (collection, value);
      break;
    }

    case CASS_VALUE_TYPE_TIMESTAMP: {
      cass_int64_t wideValue = 0;

      if (casstcl_GetTimestampFromObj (interp, obj, &wideValue) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting 'timestamp' element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_int64 (collection, wideValue);
      break;
    }

    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER: {
      Tcl_WideInt wideValue = 0;

      if (Tcl_GetWideIntFromObj (interp, obj, &wideValue) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting wide int element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_int64 (collection, wideValue);
      break;
    }


    case CASS_VALUE_TYPE_DOUBLE: {
      double value = 0;

      if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting double element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_double (collection, value);
      break;
    }

    case CASS_VALUE_TYPE_FLOAT: {
      double value = 0;

      if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting float element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_float (collection, value);
      break;
    }

    case CASS_VALUE_TYPE_INT: {
      int value = 0;

      if (Tcl_GetIntFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting int element", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_int32 (collection, value);
      break;
    }

    case CASS_VALUE_TYPE_TIMEUUID:
    case CASS_VALUE_TYPE_UUID: {
      CassUuid cassUuid;

      cassError = cass_uuid_from_string(Tcl_GetString(obj), &cassUuid);

      if (cassError == CASS_OK) {
        cassError = cass_collection_append_uuid (collection, cassUuid);
      }
      break;
    }

    case CASS_VALUE_TYPE_INET: {
      CassInet cassInet;
      cass_uint8_t address[CASS_INET_V6_LENGTH];

      if (casstcl_GetInetFromObj(interp, obj, &cassInet)) {
        return TCL_ERROR;
      }

      memcpy(address, cassInet.address, CASS_INET_V6_LENGTH);

      if (cassInet.address_length == CASS_INET_V6_LENGTH) {
        cassInet = cass_inet_init_v6(address);
      } else if (cassInet.address_length == CASS_INET_V4_LENGTH) {
        cassInet = cass_inet_init_v4(address);
      } else {
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "bad 'inet' address length for bind operation", NULL);
        return TCL_ERROR;
      }

      cassError = cass_collection_append_inet (collection, cassInet);
      break;
    }

    default: {
      char msg[60];

      sprintf(msg, "%X", valueType);
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unrecognized value type for append operation 0x", msg, NULL);
      return TCL_ERROR;
    }
  }

  if (cassError != CASS_OK) {
    return casstcl_cass_error_to_tcl (ct, cassError);
  }

  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_tcl_obj --
 *
 * This routine binds Tcl objects to ?-substitution parameters in nascent
 * cassandra statements.
 *
 * It takes a statement, an index (which parameter to substitute left to
 * right from 0 to n-1), the cassandra data type (and subtype(s) if it is
 * a list, set or map), and it will convert the Tcl object to the specified
 * data type and bind it to the statement.
 *
 * This is a really important routine.
 *
 * If type conversion fails, like Cassandra wants floating point and the
 * Tcl object won't convert to floating point then it's a Tcl error.
 *
 * If everything works then TCL_OK is returned.
 *
 * valueSubType1 is only used for maps, sets and lists
 * valueSubType2 is only used for maps
 *
 * Results:
 *      A standard Tcl result.
 *
 *
 *----------------------------------------------------------------------
 */

int casstcl_bind_tcl_obj (casstcl_sessionClientData *ct, CassStatement *statement, char *name, int name_length, cass_size_t index, casstcl_cassTypeInfo *typeInfo, Tcl_Obj *obj)
{
// printf("casstcl_bind_tcl_obj called, index %d, valueType %d\n", index, typeInfo->cassValueType);
  Tcl_Interp *interp = ct->interp;
  CassError cassError = CASS_OK;

  switch (typeInfo->cassValueType) {
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR: {
      int length = 0;
      char *value = Tcl_GetStringFromObj (obj, &length);

      if (name == NULL) {
        cassError = cass_statement_bind_string_n (statement, index, value, length);
      } else {
        cassError = cass_statement_bind_string_by_name_n (statement, name, name_length, value, length);
      }
      break;
    }

    case CASS_VALUE_TYPE_UNKNOWN: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for bind operation 'unknown'", NULL);
      return TCL_ERROR;
    }

    case CASS_VALUE_TYPE_CUSTOM:
    case CASS_VALUE_TYPE_BLOB: {
      int length = 0;
      unsigned char *value = Tcl_GetByteArrayFromObj (obj, &length);

      if (name == NULL) {
        cassError = cass_statement_bind_bytes (statement, index, value, length);
      } else {
        cassError = cass_statement_bind_bytes_by_name (statement, name, value, length);
      }
      break;
    }

    case CASS_VALUE_TYPE_BOOLEAN: {
      int value = 0;

      if (Tcl_GetBooleanFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting boolean element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_bool (statement, index, value);
      } else {
        cassError = cass_statement_bind_bool_by_name (statement, name, value);
      }
      break;
    }

    case CASS_VALUE_TYPE_TIMESTAMP: {
      cass_int64_t wideValue = 0;

      if (casstcl_GetTimestampFromObj (interp, obj, &wideValue) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting 'timestamp' element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_int64 (statement, index, wideValue);
      } else {
        cassError = cass_statement_bind_int64_by_name (statement, name, wideValue);
      }
      break;
    }

    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER: {
      Tcl_WideInt wideValue = 0;

      if (Tcl_GetWideIntFromObj (interp, obj, &wideValue) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting wide int element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_int64 (statement, index, wideValue);
      } else {
        cassError = cass_statement_bind_int64_by_name (statement, name, wideValue);
      }
      break;
    }

    case CASS_VALUE_TYPE_DECIMAL: {
      int listObjc;
      Tcl_Obj **listObjv;
      int scale;
      mp_int mpVal;
      CassBytes cassBytes;
      CassDecimal cassDecimal;

      if (Tcl_ListObjGetElements (interp, obj, &listObjc, &listObjv) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while getting decimal elements", NULL);
        return TCL_ERROR;
      }

      if (listObjc != 2) {
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "decimal requires exactly two elements", NULL);
        return TCL_ERROR;
      }

      if (Tcl_GetIntFromObj(interp, listObjv[0], &scale) != TCL_OK) {
        Tcl_AppendResult (interp, " while converting decimal scale", NULL);
        return TCL_ERROR;
      }

      if (Tcl_GetBignumFromObj(interp, listObjv[1], &mpVal) != TCL_OK) {
        Tcl_AppendResult (interp, " while converting decimal bignum", NULL);
        return TCL_ERROR;
      }

      if (casstcl_InitCassBytesFromBignum(interp, &cassBytes, &mpVal) != TCL_OK) {
        Tcl_AppendResult (interp, " while creating decimal bytes", NULL);
        return TCL_ERROR;
      }

      cassDecimal.scale = scale;
      cassDecimal.varint = cassBytes;

      if (name == NULL) {
        cassError = cass_statement_bind_decimal (statement, index, cassDecimal.varint.data, cassDecimal.varint.size, cassDecimal.scale);
      } else {
        cassError = cass_statement_bind_decimal_by_name (statement, name, cassDecimal.varint.data, cassDecimal.varint.size, cassDecimal.scale);
      }
      ckfree((char *) cassBytes.data);
      break;
    }

    case CASS_VALUE_TYPE_DOUBLE: {
      double value = 0;

      if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting double element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_double (statement, index, value);
      } else {
        cassError = cass_statement_bind_double_by_name (statement, name, value);
      }
      break;
    }

    case CASS_VALUE_TYPE_FLOAT: {
      double value = 0;

      if (Tcl_GetDoubleFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting float element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_float (statement, index, value);
      } else {
        cassError = cass_statement_bind_float_by_name (statement, name, value);
      }
      break;
    }

    case CASS_VALUE_TYPE_INT: {
      int value = 0;

      if (Tcl_GetIntFromObj (interp, obj, &value) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting int element", NULL);
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_int32 (statement, index, value);
      } else {
        cassError = cass_statement_bind_int32_by_name (statement, name, value);
      }
      break;
    }

    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_TIMEUUID: {
      CassUuid cassUuid;

      cassError = cass_uuid_from_string(Tcl_GetString(obj), &cassUuid);

      if (cassError == CASS_OK) {
        if (name == NULL) {
          cassError = cass_statement_bind_uuid (statement, index, cassUuid);
        } else {
          cassError = cass_statement_bind_uuid_by_name (statement, name, cassUuid);
        }
      }
      break;
    }

    case CASS_VALUE_TYPE_VARINT: {
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unsupported value type for bind operation 'varint'", NULL);
      return TCL_ERROR;
    }

    case CASS_VALUE_TYPE_INET: {
      CassInet cassInet;

      if (casstcl_GetInetFromObj(interp, obj, &cassInet)) {
        return TCL_ERROR;
      }

      if (name == NULL) {
        cassError = cass_statement_bind_inet (statement, index, cassInet);
      } else {
        cassError = cass_statement_bind_inet_by_name (statement, name, cassInet);
      }
      break;
    }

    case CASS_VALUE_TYPE_SET:
    case CASS_VALUE_TYPE_LIST: {
      int listObjc;
      Tcl_Obj **listObjv;
      int i;

      CassCollectionType collectionType = (typeInfo->cassValueType == CASS_VALUE_TYPE_SET) ? CASS_COLLECTION_TYPE_SET : CASS_COLLECTION_TYPE_LIST;

      if (Tcl_ListObjGetElements (interp, obj, &listObjc, &listObjv) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting map element", NULL);
        return TCL_ERROR;
      }

      CassCollection *collection = cass_collection_new (collectionType, listObjc);

      for (i = 0; i < listObjc; i++) {
        cassError = casstcl_append_tcl_obj_to_collection (ct, collection, typeInfo->valueSubType1, listObjv[i]);
        if (cassError != CASS_OK) {
          break;
        }
      }

      if (name == NULL) {
        cassError = cass_statement_bind_collection (statement, index, collection);
      } else {
        cassError = cass_statement_bind_collection_by_name (statement, name, collection);
      }
      cass_collection_free (collection);

      break;
    }

    case CASS_VALUE_TYPE_MAP: {
      int listObjc;
      Tcl_Obj **listObjv;
      int i;

      if (Tcl_ListObjGetElements (interp, obj, &listObjc, &listObjv) == TCL_ERROR) {
        Tcl_AppendResult (interp, " while converting map element", NULL);
        return TCL_ERROR;
      }

      if (listObjc & 1) {
        Tcl_ResetResult (interp);
        Tcl_AppendResult (interp, "list must contain an even number of elements while converting map element", NULL);
        return TCL_ERROR;
      }

// printf("map collection accumulation started, %d elements\n", listObjc);
      CassCollection *collection = cass_collection_new (CASS_COLLECTION_TYPE_MAP, listObjc);

      for (i = 0; i < listObjc; i += 2) {
        cassError = casstcl_append_tcl_obj_to_collection (ct, collection, typeInfo->valueSubType1, listObjv[i]);
        if (cassError != CASS_OK) {
          break;
        }

        cassError = casstcl_append_tcl_obj_to_collection (ct, collection, typeInfo->valueSubType2, listObjv[i+1]);
        if (cassError != CASS_OK) {
          break;
        }
      }

      if (name == NULL) {
        cassError = cass_statement_bind_collection (statement, index, collection);
      } else {
        cassError = cass_statement_bind_collection_by_name (statement, name, collection);
      }
      cass_collection_free (collection);

// printf("bound map collection of %d elements as to statement index %d\n", listObjc, index);

      break;
    }

    default: {
      char msg[60];
      sprintf(msg, "%X", typeInfo->cassValueType);
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unrecognized value type for bind operation 0x", msg, NULL);
      return TCL_ERROR;
    }
  }

  if (cassError != CASS_OK) {
    return casstcl_cass_error_to_tcl (ct, cassError);
  }

  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * casstcl_bind_values_and_types --
 *
 *   Now this little ditty takes a query and an objv and a pointer to
 *   a pointer to a cassandra statement
 *
 *   It creates a cassandra statement
 *
 *   It then iterates through the objv as a list pairs where the first element
 *   is a value and the second element is a cassandra data type
 *
 *   For each pair it interprets the data type, converts the element to
 *   that type, and binds it to that position in the statement.
 *
 *   If the data type is a list or set then the corresponding Tcl object
 *   is converted to a list of that type.
 *
 *   If the data type is a map then the corresponding Tcl object is converted
 *   to a map of alternating key-value pairs of the two specified types.
 *
 *   If the objv is empty the statement is created with nothing bound.
 *   It's probably fine if that happens.
 *
 * Results:
 *      A standard Tcl result.
 *
 *----------------------------------------------------------------------
 */
int
casstcl_bind_values_and_types (casstcl_sessionClientData *ct, char *query, int objc, Tcl_Obj *CONST objv[], CassConsistency *consistencyPtr, CassStatement **statementPtr)
{
  int i;
  int masterReturn = TCL_OK;
  int tclReturn = TCL_OK;
  Tcl_Interp *interp = ct->interp;

  casstcl_cassTypeInfo typeInfo = { CASS_VALUE_TYPE_UNKNOWN, CASS_VALUE_TYPE_UNKNOWN, CASS_VALUE_TYPE_UNKNOWN};

  *statementPtr = NULL;

  if (objc & 1) {
    Tcl_SetObjResult (interp, Tcl_NewStringObj ("values_and_types list must contain an even number of elements", -1));
    return TCL_ERROR;
  }

  CassStatement *statement = cass_statement_new(query, objc / 2);

  if (casstcl_setStatementConsistency(ct, statement, consistencyPtr) != TCL_OK) {
    return TCL_ERROR;
  }

  for (i = 0; i < objc; i += 2) {
    tclReturn = casstcl_obj_to_compound_cass_value_types (interp, objv[i+1], &typeInfo);

    if (tclReturn == TCL_ERROR) {
      masterReturn = TCL_ERROR;
      break;
    }

    tclReturn = casstcl_bind_tcl_obj (ct, statement, NULL, 0, i / 2, &typeInfo, objv[i]);

    if (tclReturn == TCL_ERROR) {
      masterReturn = TCL_ERROR;
      break;
    }
  }

  if (masterReturn == TCL_OK) {
    *statementPtr = statement;
  }

  return masterReturn;
}



/* vim: set ts=4 sw=4 sts=4 noet : */
