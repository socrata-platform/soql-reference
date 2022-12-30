package com.socrata.soql.analyzer2.rewrite

import scala.annotation.tailrec
import scala.collection.compat._

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2._

class PreserveSystemColumns[RNS, CT, CV] private (
  labelProvider: LabelProvider,
  // For aggregate queries, given an expression for a system column,
  // produce an expression that aggregates it in some hypothetically
  // useful way, or None if there really isn't such a beast.
  aggregate: Expr[CT, CV] => Option[Expr[CT, CV]]
) {
  type Statement = analyzer2.Statement[RNS, CT, CV]
  type From = analyzer2.From[RNS, CT, CV]
  type Join = analyzer2.Join[RNS, CT, CV]
  type AtomicFrom = analyzer2.AtomicFrom[RNS, CT, CV]
  type FromTable = analyzer2.FromTable[RNS, CT]
  type FromSingleRow = analyzer2.FromSingleRow[RNS]
  type Column = analyzer2.Column[CT]

  private def isSystemColumn(name: ColumnName) = name.name.startsWith(":")

  def rewriteStatement(stmt: Statement): Statement = {
    stmt match {
      case stmt@CombinedTables(_, _, _) =>
        // Adding columns to CombinedTables would change their
        // semantics, so don't.
        stmt

      case stmt@CTE(defLabel, defAlias, defQuery, materializedHint, useQuery) =>
        stmt.copy(useQuery = rewriteStatement(useQuery))

      case stmt@Values(_) =>
        // Valueses don't have system columns
        stmt

      case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        distinctiveness match {
          case Distinctiveness.Indistinct | Distinctiveness.On(_) =>
            val newFrom = rewriteFrom(from)
            val existingColumnNames = selectList.valuesIterator.map(_.name).to(Set)
            val newSelectList = selectList ++ findSystemColumns(newFrom).iterator.flatMap { case (name, column) =>
              if(existingColumnNames(name)) {
                None
              } else if(stmt.isAggregated) {
                aggregate(column).map { merged =>
                  labelProvider.columnLabel() -> NamedExpr(merged, name)
                }
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(column, name))
              }
            }

            stmt.copy(selectList = newSelectList, from = newFrom)
          case Distinctiveness.FullyDistinct =>
            // adding columns would change semantics, so don't
            stmt
        }
    }
  }

  @tailrec
  final def findSystemColumns(from: From): Iterable[(ColumnName, Column)] =
    from match {
      case j: Join => findSystemColumns(j.left)
      case FromTable(_, _, _, tableLabel, columns) =>
        columns.collect { case (colLabel, NameEntry(name, typ)) if isSystemColumn(name) =>
          name -> Column(tableLabel, colLabel, typ)(AtomicPositionInfo.None)
        }
      case FromStatement(stmt, tableLabel, _, _) =>
        stmt.schema.iterator.collect { case (colLabel, NameEntry(name, typ)) if isSystemColumn(name) =>
          name -> Column(tableLabel, colLabel, typ)(AtomicPositionInfo.None)
        }.toSeq
      case FromSingleRow(_, _) =>
        Nil
    }

  def rewriteFrom(from: From): From =
    from.map[RNS, CT, CV](
      rewriteAtomicFrom,
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, right, on) }
    )

  def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom =
    from match {
      case from@FromStatement(stmt, _, _, _) =>
        from.copy(statement = rewriteStatement(stmt))
      case other =>
        other
    }
}

object PreserveSystemColumns {
  def apply[RNS, CT, CV](
    labelProvider: LabelProvider,
    stmt: Statement[RNS, CT, CV],
    aggregate: Expr[CT, CV] => Option[Expr[CT, CV]]
  ): Statement[RNS, CT, CV] = {
    new PreserveSystemColumns[RNS, CT, CV](labelProvider, aggregate).rewriteStatement(stmt)
  }
}
