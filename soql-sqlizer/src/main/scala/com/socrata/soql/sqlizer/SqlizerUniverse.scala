package com.socrata.soql.sqlizer

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.prettyprint

import com.socrata.soql.sqlizer

trait SqlizerUniverse[MT <: MetaTypes with MetaTypesExt] extends StatementUniverse[MT] {
  type AugmentedType = sqlizer.AugmentedType[MT]
  type Rep = sqlizer.Rep[MT]
  type ExprSql = sqlizer.ExprSql[MT]
  type OrderBySql = sqlizer.OrderBySql[MT]
  type ExprSqlizer = sqlizer.ExprSqlizer[MT]
  type FuncallSqlizer = sqlizer.FuncallSqlizer[MT]
  type Sqlizer = sqlizer.Sqlizer[MT]
  type SqlizeAnnotation = sqlizer.SqlizeAnnotation[MT]
  type Doc = prettyprint.Doc[SqlizeAnnotation]
  type DocNothing = prettyprint.Doc[Nothing]
  type SqlNamespaces = sqlizer.SqlNamespaces[MT]
  type ResultExtractor = sqlizer.ResultExtractor[MT]
  type RewriteSearch = sqlizer.RewriteSearch[MT]
  type ProvenanceTracker = sqlizer.ProvenanceTracker[MT]
  type ExtraContext = MT#ExtraContext
  type ExtraContextResult = MT#ExtraContextResult
  type ExprSqlFactory = sqlizer.ExprSqlFactory[MT]

  type AugmentedSchema = sqlizer.AugmentedSchema[MT]
  type AvailableSchemas = sqlizer.AvailableSchemas[MT]
}
