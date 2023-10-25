package com.socrata.soql.analyzer2

import com.socrata.soql.environment.Provenance

private[analyzer2] case class RewriteDatabaseNamesState[MT1 <: MetaTypes, MT2 <: MetaTypes](
  tableName: types.DatabaseTableName[MT1] => types.DatabaseTableName[MT2],
  columnName: (types.DatabaseTableName[MT1], types.DatabaseColumnName[MT1]) => types.DatabaseColumnName[MT2],
  fromMT1Provenance: types.FromProvenance[MT1],
  toMT2Provenance: types.ToProvenance[MT2],
  updateProvenance: types.ColumnValue[MT1] => (Provenance => Provenance) => types.ColumnValue[MT2],
  val realTables: Map[AutoTableLabel, types.DatabaseTableName[MT1]],
  val changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT1, MT2],
) {
  type DatabaseTableName1 = types.DatabaseTableName[MT1]
  type DatabaseTableName2 = types.DatabaseTableName[MT2]

  type ColumnLabel1 = types.ColumnLabel[MT1]
  type ColumnLabel2 = types.ColumnLabel[MT2]
  type DatabaseColumnName1 = types.DatabaseColumnName[MT1]
  type DatabaseColumnName2 = types.DatabaseColumnName[MT2]

  type AtomicPositionInfo1 = AtomicPositionInfo[MT1#ResourceNameScope]
  type AtomicPositionInfo2 = AtomicPositionInfo[MT2#ResourceNameScope]
  type FuncallPositionInfo1 = FuncallPositionInfo[MT1#ResourceNameScope]
  type FuncallPositionInfo2 = FuncallPositionInfo[MT2#ResourceNameScope]

  private val tableNames = new scala.collection.mutable.HashMap[DatabaseTableName1, DatabaseTableName2]
  private val columnNames = new scala.collection.mutable.HashMap[(DatabaseTableName1, DatabaseColumnName1), DatabaseColumnName2]

  def rewriteProvenance(value: types.ColumnValue[MT1]): types.ColumnValue[MT2] = {
    updateProvenance(value) { prov1 =>
      toMT2Provenance.toProvenance(convert(fromMT1Provenance.fromProvenance(prov1)))
    }
  }

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

  def convert(tLabel: AutoTableLabel, cName: DatabaseColumnName1): DatabaseColumnName2 = {
    realTables.get(tLabel) match {
      case Some(tName) => convert(tName, cName)
      case None => throw new Exception("realTables doesn't contain an AutoTableLabel that was found???")
    }
  }

  def convert(pos: AtomicPositionInfo1): AtomicPositionInfo2 =
    pos.asInstanceOf[AtomicPositionInfo2] // SAFETY: a position contains no database names

  def convert(pos: FuncallPositionInfo1): FuncallPositionInfo2 =
    pos.asInstanceOf[FuncallPositionInfo2] // SAFETY: a position contains no database names
}
