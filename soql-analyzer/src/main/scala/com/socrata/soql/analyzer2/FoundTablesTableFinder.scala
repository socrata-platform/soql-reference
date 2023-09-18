package com.socrata.soql.analyzer2

import com.socrata.soql.collection._

class FoundTablesTableFinder[MT <: MetaTypes](foundTables: FoundTables[MT]) extends TableFinder[MT] {
  override protected val parserParameters = foundTables.parserParameters

  override protected def lookup(name: ScopedResourceName): Either[LookupError, FinderTableDescription] = {
    foundTables.tableMap.get(name) match {
      case Some(description) =>
        description match {
          case ds: TableDescription.Dataset[MT] =>
            val converted = Dataset(
              ds.name,
              ds.canonicalName,
              ds.columns.withValuesMapped { dci =>
                DatasetColumnInfo(dci.name, dci.typ, dci.hidden)
              },
              ds.ordering.map { ord =>
                Ordering(ord.column, ord.ascending, ord.nullLast)
              },
              ds.primaryKeys
            )
            foundDataset(name, converted)
          case q: TableDescription.Query[MT] =>
            val converted = Query(
              q.scope,
              q.canonicalName,
              q.basedOn,
              q.unparsed,
              q.parameters,
              q.hiddenColumns
            )
            foundQuery(name, converted)
          case tf: TableDescription.TableFunction[MT] =>
            val converted = TableFunction(
              tf.scope,
              tf.canonicalName,
              tf.unparsed,
              tf.parameters,
              tf.hiddenColumns
            )
            foundTableFunction(name, converted)
        }
      case None =>
        notFound(name)
    }
  }

  def refind: Result[FoundTables[MT]] =
    foundTables.initialQuery match {
      case FoundTables.Saved(resourceName) =>
        findTables(foundTables.initialScope, resourceName)
      case FoundTables.InContext(parent, _soql, text, parameters) =>
        findTables(foundTables.initialScope, parent, text, parameters)
      case FoundTables.InContextImpersonatingSaved(parent, _soql, text, parameters, impersonating) =>
        findTables(foundTables.initialScope, parent, text, parameters, impersonating)
      case FoundTables.Standalone(_soql, text, parameters) =>
        findTables(foundTables.initialScope, text, parameters)
    }

  // These four methods can be overridden to let us edit the results
  // of the found tables; it's kind of pointless to use this class
  // without overriding at least one of the found* methods.

  protected def foundDataset(name: ScopedResourceName, ds: Dataset): Either[LookupError, FinderTableDescription] =
    Right(ds)

  protected def foundQuery(name: ScopedResourceName, q: Query): Either[LookupError, FinderTableDescription] =
    Right(q)

  protected def foundTableFunction(name: ScopedResourceName, tf: TableFunction): Either[LookupError, FinderTableDescription] =
    Right(tf)

  protected def notFound(name: ScopedResourceName): Either[LookupError, FinderTableDescription] =
    Left(LookupError.NotFound)
}
