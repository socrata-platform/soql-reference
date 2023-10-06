package com.socrata.soql.analyzer2.rollup

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName

trait RollupInfo[MT <: MetaTypes, RollupId] extends StatementUniverse[MT] {
  val id: RollupId
  val statement: Statement
  val resourceName: types.ScopedResourceName[MT]
  val databaseName: DatabaseTableName

  def databaseColumnNameOfIndex(idx: Int): DatabaseColumnName

  private lazy val columnMap =
    statement.schema.iterator.zipWithIndex.map { case ((label, _), idx) =>
      label -> databaseColumnNameOfIndex(idx)
    }.toMap

  def from(labelProvider: LabelProvider) = locally {
    new FromTable(
      databaseName,
      resourceName,
      None,
      labelProvider.tableLabel(),
      OrderedMap() ++ statement.schema.iterator.map { case (label, schemaEnt) =>
        columnMap(label) -> NameEntry(schemaEnt.name, schemaEnt.typ)
      },
      statement.unique.map { columnLabels =>
        columnLabels.map(columnMap)
      }.toVector
    )
  }
}
