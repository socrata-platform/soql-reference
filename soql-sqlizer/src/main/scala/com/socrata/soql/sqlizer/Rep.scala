package com.socrata.soql.sqlizer

import java.sql.ResultSet
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.prettyprint.prelude._

case class SubcolInfo[MT <: MetaTypes with MetaTypesExt](compoundType: types.ColumnType[MT], index: Int, sqlType: String, soqlType: types.ColumnType[MT], compressedExtractor: Doc[SqlizeAnnotation[MT]] => Doc[SqlizeAnnotation[MT]]) {
  def extractor(e: ExprSql[MT]): Doc[SqlizeAnnotation[MT]] = {
    assert(e.typ == compoundType)
    e match {
      case expanded: ExprSql.Expanded[MT] => expanded.sqls(index)
      case cmp: ExprSql.Compressed[MT] => compressedExtractor(cmp.sql)
    }
  }
}

trait Rep[MT <: MetaTypes with MetaTypesExt] extends ExpressionUniverse[MT] {
  def typ: CT
  def physicalColumnRef(col: PhysicalColumn): ExprSql[MT]
  def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean): ExprSql[MT]
  def nullLiteral(e: NullLiteral): ExprSql[MT]
  def literal(value: LiteralValue): ExprSql[MT] // type of literal will be appropriate for this rep

  // "physical" vs "expanded" because of providenced columns; for most
  // column types these will be the same, but provedenced columns add
  // one synthetic column to the "expanded" representation.  MOST USER
  // CODE SHOULD USE "expanded", NOT "physical"; MOST SUBCLASSES
  // SHOULD DEFINE "physical", NOT "expanded"!!!
  def physicalColumnCount: Int
  def expandedColumnCount: Int = physicalColumnCount
  def physicalDatabaseColumns(name: ColumnLabel): Seq[Doc[Nothing]]
  def expandedDatabaseColumns(name: ColumnLabel) = physicalDatabaseColumns(name)
  def physicalDatabaseTypes: Seq[Doc[SqlizeAnnotation[MT]]]
  def expandedDatabaseTypes = physicalDatabaseTypes

  def compressedDatabaseColumn(name: ColumnLabel): Doc[Nothing]
  def isProvenanced: Boolean = false
  def provenanceOf(value: LiteralValue): Set[Option[Provenance]]

  // Postgresql's JDBC driver will read rows in fixed-size blocks; by
  // default, it reads all the results into memory before returning
  // them. You can make it stream by setting the fetch size (in rows),
  // so we'll do that.
  // Specifically, we'll set the fetch size based on the types of
  // columns in the result - in particular geo types routinely use a
  // lot of memory, so we'll consider such types as being "large" and
  // set a small fetch size when one exists in the result.
  def isPotentiallyLarge: Boolean = false

  // throws an exception if "field" is not a subcolumn of this type
  def subcolInfo(field: String): SubcolInfo[MT]

  def compressedSubColumns(table: String, column: ColumnLabel): Seq[Doc[Nothing]]

  // This lets us produce a different representation in the top-level
  // selection if desired (e.g., for geometries we want to convert
  // them to WKB).  For most things this is false and wrapTopLevel is
  // just the identity function.
  def hasTopLevelWrapper: Boolean = false
  def wrapTopLevel(raw: ExprSql[MT]): ExprSql[MT] = {
    assert(raw.typ == typ)
    raw
  }

  def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV)

  def indices(tableName: DatabaseTableName, label: ColumnLabel): Seq[Doc[Nothing]]
}
object Rep {
  trait Provider[MT <: MetaTypes with MetaTypesExt] extends SqlizerUniverse[MT] {
    protected implicit def implicitProvider: Provider[MT] = this
    def apply(typ: CT): Rep

    val namespace: SqlNamespaces
    val exprSqlFactory: ExprSqlFactory

    val toProvenance: types.ToProvenance[MT]
    val isRollup: DatabaseTableName => Boolean

    def mkStringLiteral(name: String): Doc // A text literal without type adornment
    def mkTextLiteral(s: String): Doc // a text literal that is annotated to be type text
    def mkByteaLiteral(bytes: Array[Byte]): Doc // a byte array literal

    protected abstract class SingleColumnRep(val typ: CT, val sqlType: Doc) extends Rep {
      override def physicalColumnRef(col: PhysicalColumn) =
        exprSqlFactory(Seq(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)), col)

      override def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean) =
        if(isExpanded) {
          exprSqlFactory(Seq(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)), col)
        } else {
          exprSqlFactory(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }

      override def physicalColumnCount = 1

      override def physicalDatabaseColumns(name: ColumnLabel) = Seq(compressedDatabaseColumn(name))

      override def physicalDatabaseTypes = Seq(sqlType)

      override def compressedDatabaseColumn(name: ColumnLabel) = namespace.columnBase(name)

      override def compressedSubColumns(table: String, column: ColumnLabel) =
        Seq(Doc(table) ++ d"." ++ compressedDatabaseColumn(column))

      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(d"null ::" +#+ sqlType, e)

      override def subcolInfo(field: String) = throw new Exception(s"$typ has no sub-columns")

      override def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = { (rs, dbCol) =>
        (1, doExtractFrom(rs, dbCol))
      }

      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV

      override def provenanceOf(e: LiteralValue) = Set.empty
    }

    protected abstract class CompoundColumnRep(val typ: CT) extends Rep {
      override def physicalColumnRef(col: PhysicalColumn) =
        genericColumnRef(col, true)

      override def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean) =
        genericColumnRef(col, isExpanded)

      private def genericColumnRef(col: Column, isExpanded: Boolean): ExprSql = {
        val dbTable = namespace.tableLabel(col.table)
        if(isExpanded) {
          exprSqlFactory(expandedDatabaseColumns(col.column).map { cn => dbTable ++ d"." ++ cn }, col)
        } else {
          exprSqlFactory(dbTable ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }
      }

      override def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = {
        if(isExpanded) { (rs, dbCol) =>
          (expandedColumnCount, doExtractExpanded(rs, dbCol))
        } else { (rs, dbCol) =>
          (1, doExtractCompressed(rs, dbCol))
        }
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV

      override def provenanceOf(e: LiteralValue) = Set.empty
    }

    protected abstract class ProvenancedRep(val typ: CT, primarySqlTyp: Doc) extends Rep {
      // We'll be representing provenanced types (SoQLID and
      // SoQLVersion) a little weirdly because we want to track the
      // values' provenance
      // So:
      //   * physical tables contain only a primarySqlTyp (probaby "bigint")
      //   * virtual tables contain both the primarySqlTyp and the canonical name of the table it came from
      // The provenance comes first so that you can't use comparisons
      // with a table under your control to find out information about
      // intervals between IDs in tables you don't control.

      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null ::" +#+ primarySqlTyp), e)

      final override def physicalColumnCount = 1
      final override def expandedColumnCount = 1 + physicalColumnCount

      final override def physicalDatabaseTypes = Seq(primarySqlTyp)
      final override def expandedDatabaseTypes = d"text" +: physicalDatabaseTypes

      final override def physicalDatabaseColumns(name: ColumnLabel) =
        Seq(namespace.columnBase(name))
      final override def expandedDatabaseColumns(name: ColumnLabel) =
        (namespace.columnBase(name) ++ d"_provenance") +: physicalDatabaseColumns(name)

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      override def physicalColumnRef(col: PhysicalColumn) = {
        val dsTable = namespace.tableLabel(col.table)
        if(isRollup(col.tableName)) {
          // This is actually a materialized VirtualColumn
          exprSqlFactory(expandedDatabaseColumns(col.column).map { cn => dsTable ++ d"." ++ cn }, col)
        } else {
          // The "::text" is required so that the provenance is not a
          // literal by SQL's standards.  Otherwise this will have
          // trouble if you order or group by :id
          exprSqlFactory(Seq(mkTextLiteral(toProvenance.toProvenance(col.tableName).value), dsTable ++ d"." ++ compressedDatabaseColumn(col.column)), col)
        }
      }

      override def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean) = {
        val dsTable = namespace.tableLabel(col.table)
        if(isExpanded) {
          exprSqlFactory(expandedDatabaseColumns(col.column).map { cn => dsTable ++ d"." ++ cn }, col)
        } else {
          exprSqlFactory(dsTable ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }
      }

      override def isProvenanced = true

      override def subcolInfo(field: String) = throw new Exception(s"$typ has no sub-columns")

      override def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = {
        if(isExpanded) { (rs, dbCol) =>
          (expandedColumnCount, doExtractExpanded(rs, dbCol))
        } else { (rs, dbCol) =>
          (1, doExtractCompressed(rs, dbCol))
        }
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV
    }
  }
}

