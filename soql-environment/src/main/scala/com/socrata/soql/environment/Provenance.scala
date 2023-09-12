package com.socrata.soql.environment

// This exists because some types (in the standard library, IDs and
// Versions) need to keep track of what dataset they were originally
// from.  That means that certain operations have to be able to
// convert a MetaTypes#DatabaseTableName to a String, both because
// values (for historical reasons) don't know anything about MetaTypes
// and because these values have to be able to be plumbed through
// extenal systems which basically have "string" as their least common
// denominator for "opaque value".  Certain operations (namely,
// typechecking and rewriting database names on analyses) will have to
// know how to change DatabaseTableNames to and/or from a Provenance.
case class Provenance(value: String)
