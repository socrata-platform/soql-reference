soql-reference provides type definitions and utilities such as analyzers and parsers for Socrata's SoQL query language.

### soql-types

This contains the definitions of all the types in SoQL.

### soql-pack

SoQLPack format - an efficient, MessagePack-based SoQL transport medium.

 *   Much more efficient than CJSON, GeoJSON, etc... especially for geometries
 *   Designed to be very streaming friendly
 *   MessagePack format means easier to implement clients in any language

 ### SoQL Docs
 
 `sbt "soqlStdlib/runMain com.socrata.soql.functions.Docs {~/developer/socrata/dev.socrata.com/docs/functions/}"`

 Running this command will generate SoQL function docs into the dev.socrata.com repository.
