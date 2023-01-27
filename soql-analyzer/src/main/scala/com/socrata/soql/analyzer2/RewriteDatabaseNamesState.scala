package com.socrata.soql.analyzer2

private[analyzer2] case class RewriteDatabaseNamesState[MT1 <: MetaTypes, MT2 <: MetaTypes](
  tableName: DatabaseTableName[MT1#DatabaseTableNameImpl] => DatabaseTableName[MT2#DatabaseTableNameImpl],
  columnName: (DatabaseTableName[MT1#DatabaseTableNameImpl], DatabaseColumnName[MT1#DatabaseColumnNameImpl]) => DatabaseColumnName[MT2#DatabaseColumnNameImpl],
  val changesOnlyLabels: ChangesOnlyLabels[MT1, MT2]
) {
  type TableLabel1 = TableLabel[MT1#DatabaseTableNameImpl]
  type TableLabel2 = TableLabel[MT2#DatabaseTableNameImpl]
  type DatabaseTableName1 = DatabaseTableName[MT1#DatabaseTableNameImpl]
  type DatabaseTableName2 = DatabaseTableName[MT2#DatabaseTableNameImpl]

  type ColumnLabel1 = ColumnLabel[MT1#DatabaseColumnNameImpl]
  type ColumnLabel2 = ColumnLabel[MT2#DatabaseColumnNameImpl]
  type DatabaseColumnName1 = DatabaseColumnName[MT1#DatabaseColumnNameImpl]
  type DatabaseColumnName2 = DatabaseColumnName[MT2#DatabaseColumnNameImpl]

  private val tableNames = new scala.collection.mutable.HashMap[DatabaseTableName1, DatabaseTableName2]
  private val columnNames = new scala.collection.mutable.HashMap[(DatabaseTableName1, DatabaseColumnName1), DatabaseColumnName2]

  def convert(name: DatabaseTableName1): DatabaseTableName2 = {
    tableNames.get(name) match {
      case Some(n) => n
      case None =>
        val fresh = tableName(name)
        tableNames += name -> fresh
        fresh
    }
  }

  def convert(tName: DatabaseTableName1, cName: DatabaseColumnName1): DatabaseColumnName2 = {
    columnNames.get((tName, cName)) match {
      case Some(n) => n
      case None =>
        val fresh = columnName(tName, cName)
        columnNames += (tName, cName) -> fresh
        fresh
    }
  }
}
