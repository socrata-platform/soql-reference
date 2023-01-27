package com.socrata.soql.analyzer2

private[analyzer2] class RelabelState(provider: LabelProvider) {
  private val tableLabels = new scala.collection.mutable.HashMap[AutoTableLabel, AutoTableLabel]
  private val columnLabels = new scala.collection.mutable.HashMap[AutoColumnLabel, AutoColumnLabel]

  def convert[T](label: TableLabel[T]): TableLabel[T] =
    label match {
      case c: AutoTableLabel =>
        convert(c)
      case db: DatabaseTableName[T] =>
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

  def convert[T](label: ColumnLabel[T]): ColumnLabel[T] =
    label match {
      case c: AutoColumnLabel =>
        convert(c)
      case db: DatabaseColumnName[T] =>
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
