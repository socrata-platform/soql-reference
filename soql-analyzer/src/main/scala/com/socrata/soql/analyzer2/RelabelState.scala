package com.socrata.soql.analyzer2

private[analyzer2] class RelabelState(provider: LabelProvider) {
  private val tableLabels = new scala.collection.mutable.HashMap[AutoTableLabel, AutoTableLabel]
  private val columnLabels = new scala.collection.mutable.HashMap[AutoColumnLabel, AutoColumnLabel]

  def convert(label: TableLabel): TableLabel =
    label match {
      case c: AutoTableLabel =>
        convert(c)
      case db: DatabaseTableName =>
        db
    }

  def convert(label: AutoTableLabel): AutoTableLabel =
    tableLabels.get(label) match {
      case Some(rename) =>
        rename
      case None =>
        val fresh = provider.tableLabel()
        tableLabels += label -> fresh
        fresh
    }

  def convert(label: ColumnLabel): ColumnLabel =
    label match {
      case c: AutoColumnLabel =>
        convert(c)
      case db: DatabaseColumnName =>
        db
    }

  def convert(label: AutoColumnLabel): AutoColumnLabel =
    columnLabels.get(label) match {
      case Some(rename) =>
        rename
      case None =>
        val fresh = provider.columnLabel()
        columnLabels += label -> fresh
        fresh
    }
}
