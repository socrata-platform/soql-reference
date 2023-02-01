package com.socrata.soql.analyzer2

private[analyzer2] case class RewriteDatabaseNamesState[MT1 <: MetaTypes, MT2 <: MetaTypes](
  tableName: types.DatabaseTableName[MT1] => types.DatabaseTableName[MT2],
  columnName: (types.DatabaseTableName[MT1], types.DatabaseColumnName[MT1]) => types.DatabaseColumnName[MT2],
  val changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT1, MT2]
) {
  type TableLabel1 = types.TableLabel[MT1]
  type TableLabel2 = types.TableLabel[MT2]
  type DatabaseTableName1 = types.DatabaseTableName[MT1]
  type DatabaseTableName2 = types.DatabaseTableName[MT2]

  type ColumnLabel1 = types.ColumnLabel[MT1]
  type ColumnLabel2 = types.ColumnLabel[MT2]
  type DatabaseColumnName1 = types.DatabaseColumnName[MT1]
  type DatabaseColumnName2 = types.DatabaseColumnName[MT2]

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
