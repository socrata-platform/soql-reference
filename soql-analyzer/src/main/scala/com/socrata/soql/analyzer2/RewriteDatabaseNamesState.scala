package com.socrata.soql.analyzer2

private[analyzer2] case class RewriteDatabaseNamesState[MT <: MetaTypes](
  realTables: Map[AutoTableLabel, DatabaseTableName[MT#DatabaseTableNameImpl]],
  tableName: DatabaseTableName[MT#DatabaseTableNameImpl] => DatabaseTableName[MT#DatabaseTableNameImpl],
  columnName: (DatabaseTableName[MT#DatabaseTableNameImpl], DatabaseColumnName) => DatabaseColumnName
) extends LabelHelper[MT] {
  private val tableNames = new scala.collection.mutable.HashMap[DatabaseTableName, DatabaseTableName]
  private val columnNames = new scala.collection.mutable.HashMap[(DatabaseTableName, DatabaseColumnName), DatabaseColumnName]

  def convert(name: DatabaseTableName): DatabaseTableName = {
    tableNames.get(name) match {
      case Some(n) => n
      case None =>
        val fresh = tableName(name)
        tableNames += name -> fresh
        fresh
    }
  }

  def convert(label: TableLabel, cName: DatabaseColumnName): DatabaseColumnName = {
    label match {
      case tName: DatabaseTableName =>
        doConvert(tName, cName)
      case auto: AutoTableLabel =>
        realTables.get(auto) match {
          case Some(tName) =>
            doConvert(tName, cName)
          case None =>
            cName
        }
    }
  }

  private def doConvert(tName: DatabaseTableName, cName: DatabaseColumnName): DatabaseColumnName = {
    columnNames.get((tName, cName)) match {
      case Some(n) => n
      case None =>
        val fresh = columnName(tName, cName)
        columnNames += (tName, cName) -> fresh
        fresh
    }
  }
}
