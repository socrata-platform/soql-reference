package com.socrata.soql.analyzer2

private[analyzer2] class PhysicalTableMap[MT <: MetaTypes] extends StatementUniverse[MT] {
  def go(statement: Statement) = walkStatement(statement, Map.empty)

  private type Acc = Map[AutoTableLabel, DatabaseTableName]

  private def walkStatement(stmt: Statement, acc: Acc): Acc =
    stmt match {
      case CombinedTables(_, left, right) =>
        walkStatement(left, walkStatement(right, acc))
      case CTE(_defLbl, _defAlias, defQ, _matHint, useQ) =>
        walkStatement(defQ, walkStatement(useQ, acc))
      case Values(_, _) =>
        acc
      case s: Select =>
        walkFrom(s.from, acc)
    }

  private def walkFrom(from: From, acc: Acc): Acc =
    from.reduce[Acc](
      walkAtomicFrom(_, acc),
      { (acc, join) => walkAtomicFrom(join.right, acc) }
    )

  private def walkAtomicFrom(from: AtomicFrom, acc: Acc): Acc =
    from match {
      case fs: FromStatement =>
        walkStatement(fs.statement, acc)
      case fsr: FromSingleRow =>
        acc
      case fc: FromCTE =>
        walkStatement(fc.basedOn, acc)
      case ft: FromTable =>
        acc + (ft.label -> ft.tableName)
    }
}
